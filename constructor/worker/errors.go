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

package worker

import "errors"

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
)
