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

package coordinator

import (
	"errors"
)

var (
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

	// ErrReturnFundsComplete is returned when it is not possible
	// to process any more ReturnFundsWorkflows or when there is no provided
	// ReturnsFundsWorkflow.
	ErrReturnFundsComplete = errors.New("return funds complete")

	// ErrJobMissing is returned when the coordinator is invoked with
	// a broadcast complete call but the job that is affected does
	// not exist.
	ErrJobMissing = errors.New("job missing")

	// ErrDuplicateWorkflows is returned when 2 Workflows with the same name
	// are provided as an input to NewCoordinator.
	ErrDuplicateWorkflows = errors.New("duplicate workflows")

	// ErrIncorrectConcurrency is returned when CreateAccount or RequestFunds
	// have a concurrency greater than 1.
	ErrIncorrectConcurrency = errors.New("incorrect concurrency")

	// ErrInvalidConcurrency is returned when the concurrency of a Workflow
	// is <= 0.
	ErrInvalidConcurrency = errors.New("invalid concurrency")

	// ErrStalled is returned when the caller does not define
	// a CreateAccount and/or RequestFunds workflow and we run out
	// of available options (i.e. we can't do anything).
	ErrStalled = errors.New("processing stalled")

	// ErrNoWorkflows is returned when no workflows are provided
	// during initialization.
	ErrNoWorkflows = errors.New("no workflows")
)
