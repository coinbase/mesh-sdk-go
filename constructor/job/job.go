// Copyright 2024 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package job

import (
	"context"
	"errors"
	"fmt"
	"math/big"

	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// New creates a new *Job.
func New(workflow *Workflow) *Job {
	return &Job{
		Workflow: workflow.Name,
		Status:   Ready,

		// We don't need to copy scenarios here because we never
		// alter scenarios during runtime.
		Scenarios: workflow.Scenarios,
	}
}

// CreateBroadcast returns a *Broadcast for a given job or
// nil if none is required.
func (j *Job) CreateBroadcast() (*Broadcast, error) {
	// ProcessNextScenario will increment the index, so we need to subtract
	// by 1 when attempting to create a broadcast payload.
	broadcastIndex := j.Index - 1
	if broadcastIndex < 0 {
		return nil, ErrUnableToCreateBroadcast
	}

	scenario := j.Scenarios[broadcastIndex]

	var operations []*types.Operation
	err := j.unmarshalStruct(scenario.Name, Operations, &operations)
	if errors.Is(err, ErrVariableNotFound) {
		// If <scenario.Name>.operations are not provided, no broadcast
		// is required.

		if j.CheckComplete() {
			j.Status = Completed
		}

		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal operations of scenario %s: %w",
			scenario.Name,
			err,
		)
	}

	confirmationDepth, err := j.unmarshalNumber(scenario.Name, ConfirmationDepth)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to unmarshal confirmation depth of scenario %s: %w",
			scenario.Name,
			err,
		)
	}

	var network types.NetworkIdentifier
	err = j.unmarshalStruct(scenario.Name, Network, &network)
	if err != nil {
		return nil, fmt.Errorf("failed to unmarshal network of scenario %s: %w", scenario.Name, err)
	}

	var metadata map[string]interface{}
	err = j.unmarshalStruct(scenario.Name, PreprocessMetadata, &metadata)
	if err != nil && !errors.Is(err, ErrVariableNotFound) {
		return nil, fmt.Errorf(
			"failed to unmarshal preprocess metadata of scenario %s: %w",
			scenario.Name,
			err,
		)
	}

	dryRun, err := j.unmarshalBoolean(scenario.Name, DryRun)
	if err != nil && !errors.Is(err, ErrVariableNotFound) {
		return nil, fmt.Errorf("failed to unmarshal dry run of scenario %s: %w", scenario.Name, err)
	}

	j.Status = Broadcasting
	return &Broadcast{
		Network:           &network,
		Intent:            operations,
		Metadata:          metadata,
		ConfirmationDepth: confirmationDepth.Int64(),
		DryRun:            dryRun,
	}, nil
}

func (j *Job) unmarshalNumber(
	scenarioName string,
	reservedVariable ReservedVariable,
) (*big.Int, error) {
	variable := fmt.Sprintf("%s.%s", scenarioName, reservedVariable)

	value := gjson.Get(j.State, variable)
	if !value.Exists() {
		return nil, ErrVariableNotFound
	}

	i, ok := new(big.Int).SetString(value.String(), 10) // nolint: gomnd
	if !ok {
		return nil, ErrVariableIncorrectFormat
	}

	return i, nil
}

func (j *Job) unmarshalBoolean(
	scenarioName string,
	reservedVariable ReservedVariable,
) (bool, error) {
	variable := fmt.Sprintf("%s.%s", scenarioName, reservedVariable)

	value := gjson.Get(j.State, variable)
	if !value.Exists() {
		return false, ErrVariableNotFound
	}

	return value.Bool(), nil
}

func (j *Job) unmarshalStruct(
	scenarioName string,
	reservedVariable ReservedVariable,
	output interface{},
) error {
	variable := fmt.Sprintf("%s.%s", scenarioName, reservedVariable)

	value := gjson.Get(j.State, variable)
	if !value.Exists() {
		return ErrVariableNotFound
	}

	return UnmarshalInput([]byte(value.Raw), output)
}

// CheckComplete returns a boolean indicating
// if a job is complete.
func (j *Job) CheckComplete() bool {
	return j.Index > len(j.Scenarios)-1
}

func (j *Job) getBroadcastScenario() (*Scenario, error) {
	if j.Status != Broadcasting {
		return nil, fmt.Errorf(
			"job is in %s state instead of %s: %w",
			j.State,
			Broadcasting,
			ErrJobInWrongState,
		)
	}

	broadcastIndex := j.Index - 1
	if broadcastIndex < 0 {
		return nil, ErrNoBroadcastToConfirm
	}

	return j.Scenarios[broadcastIndex], nil
}

func (j *Job) injectKeyAndMarkReady(
	scenarioName string,
	key ReservedVariable,
	obj string,
) error {
	objKey := fmt.Sprintf("%s.%s", scenarioName, key)
	newState, err := sjson.SetRaw(
		j.State,
		objKey,
		obj,
	)
	if err != nil {
		return fmt.Errorf("failed to set a raw json value: %w", err)
	}
	j.State = newState

	if j.CheckComplete() {
		j.Status = Completed
		return nil
	}

	j.Status = Ready
	return nil
}

// BroadcastComplete is called either after a broadcast
// has been confirmed at the provided confirmation depth or
// if it has failed for some reason.
func (j *Job) BroadcastComplete(
	ctx context.Context,
	transaction *types.Transaction,
) error {
	scenario, err := j.getBroadcastScenario()
	if err != nil {
		return fmt.Errorf("failed to get broadcast scenario: %w", err)
	}

	if transaction == nil {
		j.Status = Failed
		return nil
	}

	if err := j.injectKeyAndMarkReady(
		scenario.Name,
		Transaction,
		types.PrintStruct(transaction),
	); err != nil {
		return fmt.Errorf(
			"unable to store transaction result: %w",
			err,
		)
	}

	return nil
}

// DryRunComplete is invoked after a transaction dry run
// has been performed.
func (j *Job) DryRunComplete(
	ctx context.Context,
	suggestedFee []*types.Amount,
) error {
	scenario, err := j.getBroadcastScenario()
	if err != nil {
		return fmt.Errorf("unable to get broadcast scenario: %w", err)
	}

	if err := j.injectKeyAndMarkReady(
		scenario.Name,
		SuggestedFee,
		types.PrintStruct(suggestedFee),
	); err != nil {
		return fmt.Errorf(
			"unable to store suggested fee result: %w",
			err,
		)
	}

	return nil
}
