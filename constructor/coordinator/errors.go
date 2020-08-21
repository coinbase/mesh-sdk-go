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
