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
	// ErrInvalidJSON is returned when a populated value is not valid JSON.
	ErrInvalidJSON = errors.New("populated input is not valid JSON")

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

	// ErrOperationFormat is returned when []*types.Operation cannot be unmarshaled
	// from <scenario_name>.operations.
	ErrOperationFormat = errors.New("operation format")

	// ErrConfirmationDepthInvalid is returned when <scenario_name>.operations
	// are populated, but confirmation depth is missing or invalid.
	ErrConfirmationDepthInvalid = errors.New("invalid confirmation depth")

	// ErrNetworkInvalid is returned when <scenario_name>.operations
	// are populated, but network is missing or invalid.
	ErrNetworkInvalid = errors.New("network invalid")

	// ErrMetadataInvalid is returned when <scenario_name>.operations
	// are populated, but construction preprocess metadata is
	// invalid (ok to be missing).
	ErrMetadataInvalid = errors.New("metadata invalid")

	// ErrCreateAccount is returned when a new account should
	// be created using the `create_account` workflow.
	ErrCreateAccount = errors.New("create account")

	// ErrUnsatisfiable is returned when there is no available
	// balance that can satisfy a FindBalance request. If there
	// are no pending broadcasts, this usually means that we need
	// to request funds.
	ErrUnsatisfiable = errors.New("unsatisfiable balance")

	// ErrJobsUnretrievable is returned when an error
	// is returned when querying for jobs.
	ErrJobsUnretrievable = errors.New("unable to retrieve jobs")

	// ErrBroadcastsUnretrievable is returned when an error
	// is returned when querying for broadcasts.
	ErrBroadcastsUnretrievable = errors.New("unable to retrieve broadcasts")

	// ErrNoAvailableJobs is returned when it is not possible
	// to process any jobs. If this is returned, you should wait
	// and retry.
	ErrNoAvailableJobs = errors.New("no jobs available")

	// ErrRequestFundsWorkflowMissing is returned when we want
	// to request funds but the request funds workflow is missing.
	ErrRequestFundsWorkflowMissing = errors.New("request funds workflow missing")

	// ErrJobMissing is returned when the coordinator is invoked with
	// a broadcast complete call but the job that is affected does
	// not exist.
	ErrJobMissing = errors.New("job missing")

	// ErrDuplicateWorkflows is returned when 2 Workflows with the same name
	// are provided as an input to NewCoordinator.
	ErrDuplicateWorkflows = errors.New("duplicate workflows")
)
