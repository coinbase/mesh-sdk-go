// Copyright 2020 Coinbase, Inc.
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

package constructor

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/tidwall/gjson"
	"github.com/tidwall/sjson"
)

// NewJob creates a new *Job.
func NewJob(workflow *Workflow) *Job {
	return &Job{
		Workflow: workflow.Name,
		Status:   Ready,

		// We don't need to copy scenarios here because we never
		// alter scenarios during runtime.
		Scenarios: workflow.Scenarios,
	}
}

func (j *Job) unmarshalReserved(
	scenarioName string,
	reservedVariable ReservedVariable,
	output interface{},
) error {
	variable := fmt.Sprintf("%s.%s", scenarioName, reservedVariable)

	value := gjson.Get(j.State, variable)
	if !value.Exists() {
		return ErrVariableNotFound
	}

	if err := json.Unmarshal([]byte(value.Raw), &output); err != nil {
		return fmt.Errorf("%w: could not unmarshal variable %s", err, variable)
	}

	return nil
}

func (j *Job) checkComplete() bool {
	return j.Index > len(j.Scenarios)-1
}

// Process is called on a Job to execute
// the next available scenario. If no scenarios
// are remaining, this will return an error.
func (j *Job) Process(
	ctx context.Context,
	worker *Worker,
) (*Broadcast, error) {
	if j.checkComplete() {
		return nil, ErrJobComplete
	}

	if err := worker.ProcessNextScenario(ctx, j); err != nil {
		return nil, fmt.Errorf("%w: could not process next scenario", err)
	}

	// ProcessNextScenario will increment the index, so we need to subtract
	// by 1 when attempting to create a broadcast payload.
	broadcastIndex := j.Index - 1
	if broadcastIndex < 0 {
		return nil, ErrUnableToCreateBroadcast
	}

	scenario := j.Scenarios[broadcastIndex]

	var operations []*types.Operation
	err := j.unmarshalReserved(scenario.Name, Operations, &operations)
	if errors.Is(err, ErrVariableNotFound) {
		// If <scenario.Name>.operations are not provided, no broadcast
		// is required.

		if j.checkComplete() {
			j.Status = Completed
		}

		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: could not unmarshal operations", err)
	}

	var network types.NetworkIdentifier
	err = j.unmarshalReserved(scenario.Name, Network, &network)
	if err != nil {
		return nil, fmt.Errorf("%w: could not unmarshal network", err)
	}

	var metadata map[string]interface{}
	err = j.unmarshalReserved(scenario.Name, PreprocessMetadata, &metadata)
	if err != nil && !errors.Is(err, ErrVariableNotFound) {
		return nil, fmt.Errorf("%w: could not unmarshal preprocess metadata", err)
	}

	j.Status = Broadcasting
	return &Broadcast{
		Network:           &network,
		Intent:            operations,
		Metadata:          metadata,
		ConfirmationDepth: scenario.ConfirmationDepth,
	}, nil
}

// BroadcastComplete is called either after a broadcast
// has been confirmed at the provided confirmation depth or
// if it has failed for some reason.
func (j *Job) BroadcastComplete(
	ctx context.Context,
	transaction *types.Transaction,
) error {
	if j.Status != Broadcasting {
		return fmt.Errorf("%w: job is in %s state", ErrUnableToHandleBroadcast, j.State)
	}

	if transaction == nil {
		j.Status = Failed
		return nil
	}

	broadcastIndex := j.Index - 1
	if broadcastIndex < 0 {
		return fmt.Errorf("%w: no broadcast to confirm", ErrUnableToHandleBroadcast)
	}

	scenario := j.Scenarios[broadcastIndex]

	// Store transaction in state
	transactionKey := fmt.Sprintf("%s.%s", scenario.Name, Transaction)
	newState, err := sjson.SetRaw(
		j.State,
		transactionKey,
		types.PrettyPrintStruct(transaction),
	)
	if err != nil {
		return fmt.Errorf(
			"%w: unable to store transaction result in state %s",
			ErrUnableToHandleBroadcast,
			err,
		)
	}
	j.State = newState

	if j.checkComplete() {
		j.Status = Completed
		return nil
	}

	j.Status = Ready
	return nil
}
