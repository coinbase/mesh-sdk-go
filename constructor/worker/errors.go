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

package worker

import (
	"encoding/json"
	"errors"
	"fmt"

	"github.com/fatih/color"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	// ErrInvalidJSON is returned when a populated value is not valid JSON.
	ErrInvalidJSON = errors.New("populated input is not valid JSON")

	// ErrVariableNotFound is returned when a variable is not
	// present in a Job's state.
	ErrVariableNotFound = errors.New("variable not found")

	// ErrJobComplete is returned when there are no more scenarios
	// to process in a Job.
	ErrJobComplete = errors.New("job complete")

	// ErrInvalidInput is returned when the input for an Action
	// cannot be parsed.
	ErrInvalidInput = errors.New("invalid input")

	// ErrInvalidActionType is returned when an Action has an unsupported
	// type.
	ErrInvalidActionType = errors.New("invalid action type")

	// ErrActionFailed is returned when Action exeuction fails with a valid input.
	ErrActionFailed = errors.New("action execution failed")

	// ErrCreateAccount is returned when a new account should
	// be created using the `create_account` workflow.
	ErrCreateAccount = errors.New("create account")

	// ErrUnsatisfiable is returned when there is no available
	// balance that can satisfy a FindBalance request. If there
	// are no pending broadcasts, this usually means that we need
	// to request funds.
	ErrUnsatisfiable = errors.New("unsatisfiable balance")

	// ErrInputOperationIsNotSupported is returned when the input operation
	// is not supported.
	ErrInputOperationIsNotSupported = errors.New("the input operation is not supported")
)

// Error is returned by worker execution.
type Error struct {
	Workflow string `json:"workflow"`
	Job      string `json:"job"`

	Scenario      string `json:"scenario"`
	ScenarioIndex int    `json:"scenario_index"`
	ActionIndex   int    `json:"action_index"`

	Action *job.Action `json:"action,omitempty"`

	ProcessedInput string `json:"processed_input,omitempty"`
	Output         string `json:"output,omitempty"`

	State string `json:"state"`

	Err error `json:"err"`
}

// Log prints the error to the console in a human readable format.
func (e *Error) Log() {
	message := fmt.Sprintf("EXECUTION FAILED!\nMessage: %s\n\n", e.Err.Error())

	if len(e.Job) > 0 { // job identifier is only assigned if persisted once
		message = fmt.Sprintf("%sJob: %s\n", message, e.Job)
	}

	message = fmt.Sprintf(
		"%sWorkflow: %s\nScenario: %s\nScenario Index: %d\n\n",
		message,
		e.Workflow,
		e.Scenario,
		e.ScenarioIndex,
	)

	if e.Action != nil {
		message = fmt.Sprintf(
			"%sAction Index: %d\nAction: %s\n",
			message,
			e.ActionIndex,
			types.PrettyPrintStruct(e.Action),
		)

		message = fmt.Sprintf(
			"%sProcessed Input: %s\nOutput: %s\n\n",
			message,
			e.ProcessedInput,
			e.Output,
		)
	}

	// We must convert state to a map so we can
	// pretty print it!
	var state map[string]interface{}
	if err := json.Unmarshal([]byte(e.State), &state); err == nil {
		message = fmt.Sprintf(
			"%sState: %s\n",
			message,
			types.PrettyPrintStruct(state),
		)
	}

	color.Red(message)
}
