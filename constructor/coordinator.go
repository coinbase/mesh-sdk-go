package constructor

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/coinbase/rosetta-sdk-go/utils"
)

type JobStorage interface {
	// Ready returns the jobs that are ready to be processed.
	Ready(context.Context) ([]*Job, error)

	// Processing returns the number of jobs processing
	// for a particular workflow.
	Processing(context.Context, string) (int, error)

	// Update stores an updated *Job in storage
	// and returns its UUID (which won't exist
	// on first update).
	Update(context.Context, *Job) error
}

type Fetcher interface{}

// TODO: move to types
type Coordinator struct {
	storage JobStorage
	worker  *Worker

	workflows             []*Workflow
	createAccountWorkflow *Workflow
	requestFundsWorkflow  *Workflow
}

func NewCoordinator() *Coordinator {
	// TODO: set worker
	// TODO: set JobStorage (for tracking state of jobs)
	// TODO: set Fetcher (for construction API calls)
}

func (c *Coordinator) findJob(
	ctx context.Context,
	notJobs []string,
	notWorkflows []string,
) (*Job, error) {
	// Look for any jobs ready for processing. If one is found,
	// we return that as the next job to process.
	ready, err := c.storage.Ready(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrJobsUnretrievable,
			err.Error(),
		)
	}
	for _, job := range ready {
		if utils.ContainsString(notJobs, job.Identifier) {
			continue
		}

		return job, nil
	}

	// Attempt all workflows other than required
	// -> if jobs of workflows already has existing == concurrency, skip
	// -> create Job for workflow
	for _, workflow := range c.workflows {
		if utils.ContainsString(notWorkflows, workflow.Name) {
			continue
		}

		processing, err := c.storage.Processing(ctx, workflow.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s",
				ErrJobsUnretrievable,
				err.Error(),
			)
		}

		if processing > workflow.Concurrency {
			continue
		}

		return NewJob(workflow), nil
	}

	// Check if broadcasts, then ErrNoAvailableJobs

	// Check if ErrCreateAccount, then create account if exists

	// Return request funds (if defined, else error)

	return nil, ErrNoAvailableJobs
}

func (c *Coordinator) Process(
	ctx context.Context,
) error {
	// ** Process Job
	// -> if workflow completed, attempt broadcast, restart
	// -> if # of jobs changed, restart before creating account or requesting funds
	// -> if any return ErrCreateAccount, attempt to create account, restart
	// -> -> if create account doesn't exist, move to funds request and log message (dont' exit)
	// -> if all return ErrUnsatisfiable && no pending jobs, request funds
	// -> -> if request funds doesn't exist, error

	// Reset after a success
	attemptedJobs := []string{}
	attemptedWorkflows := []string{}
	receivedCreateAccount := false

	for ctx.Err() == nil {
		if !c.helper.HeadBlockExists(ctx) {
			// We will sleep until at least one block has been synced.
			// Many of the storage-based commands require a synced block
			// to work correctly (i.e. when fetching a balance, a block
			// must be returned).
			time.Sleep(NoHeadBlockWaitTime)
			continue
		}

		// Attempt to find a Job to process.
		job, err := c.findJob(ctx, attemptedJobs, attemptedWorkflows)
		if errors.Is(err, ErrNoAvailableJobs) {
			// TODO: if no broadcasting jobs, we may need to request funds
			// TODO: if create account
			// TODO: if only unsatisfiable, request
			time.Sleep(NoJobsWaitTime)
			attemptedJobs = []string{}
			attemptedWorkflows = []string{}
			receivedCreateAccount = false
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to find job", err)
		}

		broadcast, err := job.Process(ctx, c.worker)
		if errors.Is(err, ErrCreateAccount) {
			receivedCreateAccount = true
			continue
		}
		if errors.Is(err, ErrUnsatisfiable) {
			// We do nothing if unsatisfiable.
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to process job", err)
		}

		// Update job (or store for the first time)
		if err := c.storage.Update(ctx, job); err != nil {
			return fmt.Errorf("%w: unable to update job")
		}

		// Reset all stats
		attemptedJobs = []string{}
		attemptedWorkflows = []string{}
		receivedCreateAccount = false

		if broadcast == nil {
			// Commit db transaction
			continue
		}

		// Construct Transaction
		// TODO: if construction fails, the status of a job in storage
		// will be broadcasting but it will have never made it to broadcast storage.

		// Invoke Broadcast storage (in same TX as update job)

		// Commit db transaction

		// Run Broadcast all (instead of running inside Broadcast)
	}

	return ctx.Err()
}
