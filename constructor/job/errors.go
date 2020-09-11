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

package job

import "errors"

var (
	// ErrNoBroadcastToConfirm is returned when there is no broadcast
	// to confirm in a job.
	ErrNoBroadcastToConfirm = errors.New("no broadcast to confirm")

	// ErrVariableNotFound is returned when a variable is not
	// present in a Job's state.
	ErrVariableNotFound = errors.New("variable not found")

	// ErrVariableIncorrectFormat is returned when a variable
	// is in the incorrect format (i.e. when we find an int
	// instead of a string).
	ErrVariableIncorrectFormat = errors.New("variable in incorrect format")

	// ErrUnableToHandleBroadcast is returned if a Job cannot handle a
	// broadcast completion (usually because there is no broadcast to confirm).
	ErrUnableToHandleBroadcast = errors.New("unable to handle broadcast")

	// ErrUnableToHandleDryRun is returned if a Job cannot handle a
	// dry run completion.
	ErrUnableToHandleDryRun = errors.New("unable to handle dry run")

	// ErrUnableToCreateBroadcast is returned when it is not possible
	// to create a broadcast or check if a broadcast should be created
	// from a job.
	ErrUnableToCreateBroadcast = errors.New("unable to create broadcast")

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
)
