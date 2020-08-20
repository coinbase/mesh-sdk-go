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

import "errors"

var (
	// ErrVariableNotFound is returned when a variable is not
	// present in a Job's state.
	ErrVariableNotFound = errors.New("variable not found")

	// ErrVariableIncorrectFormat is returned when a variable
	// is in the incorrect format (i.e. when we find an int
	// instead of a string).
	ErrVariableIncorrectFormat = errors.New("variable in incorrect format")

	// ErrJobComplete is returned when there are no more scenarios
	// to process in a Job.
	ErrJobComplete = errors.New("job complete")

	// ErrInvalidInput is returned when the input for an Action
	// cannot be parsed.
	ErrInvalidInput = errors.New("invalid input")

	// ErrInvalidActionType is returned when an Action has an unsupported
	// type.
	ErrInvalidActionType = errors.New("invalid action type")

	// ErrUnableToCreateBroadcast is returned when it is not possible
	// to create a broadcast or check if a broadcast should be created
	// from a job.
	ErrUnableToCreateBroadcast = errors.New("unable to create broadcast")

	// ErrUnableToHandleBroadcast is returned if a Job cannot handle a
	// broadcast completion (usually because there is no broadcast to confirm).
	ErrUnableToHandleBroadcast = errors.New("unable to handle broadcast")

	// ErrActionFailed is returned when Action exeuction fails with a valid input.
	ErrActionFailed = errors.New("action execution failed")
)
