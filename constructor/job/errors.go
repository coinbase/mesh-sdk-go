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

	// ErrUnableToCreateBroadcast is returned when it is not possible
	// to create a broadcast or check if a broadcast should be created
	// from a job.
	ErrUnableToCreateBroadcast = errors.New("unable to create broadcast")

	// ErrJobInWrongState is returned when a job is in wrong state
	ErrJobInWrongState = errors.New("job in wrong state")
)
