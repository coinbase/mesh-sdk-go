package constructor

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// TODO: move to types
type Coordinator struct {
	storage JobStorage
	helper  Helper
	parser  *parser.Parser

	attemptedJobs        []string
	attemptedWorkflows   []string
	seenErrCreateAccount bool

	workflows             []*Workflow
	createAccountWorkflow *Workflow
	requestFundsWorkflow  *Workflow
}

func NewCoordinator() *Coordinator {
	// TODO: set worker
	// TODO: set JobStorage (for tracking state of jobs)
	// TODO: set Fetcher (for construction API calls)
	return &Coordinator{
		attemptedJobs:        []string{},
		attemptedWorkflows:   []string{},
		seenErrCreateAccount: false,
	}
}

func (c *Coordinator) findJob(
	ctx context.Context,
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
		if utils.ContainsString(c.attemptedJobs, job.Identifier) {
			continue
		}

		return job, nil
	}

	// Attempt all workflows other than required
	// -> if jobs of workflows already has existing == concurrency, skip
	// -> create Job for workflow
	for _, workflow := range c.workflows {
		if utils.ContainsString(c.attemptedWorkflows, workflow.Name) {
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
	allBroadcasts, err := c.helper.AllBroadcasts(ctx)
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrBroadcastsUnretrievable,
			err.Error(),
		)
	}

	if len(allBroadcasts) > 0 {
		return nil, ErrNoAvailableJobs
	}

	// Check if ErrCreateAccount, then create account if exists
	if c.seenErrCreateAccount {
		if c.createAccountWorkflow != nil {
			return NewJob(c.createAccountWorkflow), nil
		}

		log.Println("Create account workflow is missing!")
	}

	// Return request funds (if defined, else error)
	if c.requestFundsWorkflow == nil {
		return nil, ErrRequestFundsWorkflowMissing
	}

	return NewJob(c.requestFundsWorkflow), nil
}

// createTransaction constructs and signs a transaction with the provided intent.
func (c *Coordinator) createTransaction(
	ctx context.Context,
	broadcast *Broadcast,
) (*types.TransactionIdentifier, string, error) {
	metadataRequest, err := c.helper.Preprocess(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		broadcast.Metadata,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to preprocess", err)
	}

	requiredMetadata, err := c.helper.Metadata(
		ctx,
		broadcast.Network,
		metadataRequest,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct metadata", err)
	}

	unsignedTransaction, payloads, err := c.helper.Payloads(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		requiredMetadata,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to construct payloads", err)
	}

	parsedOps, signers, _, err := c.helper.Parse(
		ctx,
		broadcast.Network,
		false,
		unsignedTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse unsigned transaction", err)
	}

	if len(signers) != 0 {
		return nil, "", fmt.Errorf(
			"signers should be empty in unsigned transaction but found %d",
			len(signers),
		)
	}

	if err := c.parser.ExpectedOperations(broadcast.Intent, parsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: unsigned parsed ops do not match intent", err)
	}

	signatures, err := c.helper.Sign(ctx, payloads)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to sign payloads", err)
	}

	networkTransaction, err := c.helper.Combine(
		ctx,
		broadcast.Network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to combine signatures", err)
	}

	signedParsedOps, signers, _, err := c.helper.Parse(
		ctx,
		broadcast.Network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to parse signed transaction", err)
	}

	if err := c.parser.ExpectedOperations(broadcast.Intent, signedParsedOps, false, false); err != nil {
		return nil, "", fmt.Errorf("%w: signed parsed ops do not match intent", err)
	}

	if err := parser.ExpectedSigners(payloads, signers); err != nil {
		return nil, "", fmt.Errorf("%w: signed transactions signers do not match intent", err)
	}

	transactionIdentifier, err := c.helper.Hash(
		ctx,
		broadcast.Network,
		networkTransaction,
	)
	if err != nil {
		return nil, "", fmt.Errorf("%w: unable to get transaction hash", err)
	}

	return transactionIdentifier, networkTransaction, nil
}

// BroadcastComplete is called by the broadcast coordinator
// when a transaction broadcast has completed. If the transaction
// is nil, then the transaction did not succeed.
func (c *Coordinator) BroadcastComplete(
	ctx context.Context,
	jobIdentifier string,
	transaction *types.Transaction,
) error {
	dbTx := c.helper.DatabaseTransaction(ctx)
	defer dbTx.Discard(ctx)

	job, err := c.storage.Get(ctx, dbTx, jobIdentifier)
	if err != nil {
		return fmt.Errorf(
			"%w: %s",
			ErrJobMissing,
			err.Error(),
		)
	}

	if err := job.BroadcastComplete(ctx, transaction); err != nil {
		return fmt.Errorf("%w: unable to mark broadcast complete", err)
	}

	if _, err := c.storage.Update(ctx, dbTx, job); err != nil {
		return fmt.Errorf("%w: unable to update job", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: unable to commit job update", err)
	}

	return nil
}

func (c *Coordinator) resetVars() {
	c.attemptedJobs = []string{}
	c.attemptedWorkflows = []string{}
	c.seenErrCreateAccount = false
}

func (c *Coordinator) Process(
	ctx context.Context,
) error {
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
		job, err := c.findJob(ctx)
		if errors.Is(err, ErrNoAvailableJobs) {
			time.Sleep(NoJobsWaitTime)
			c.resetVars()
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to find job", err)
		}

		broadcast, err := job.Process(ctx, NewWorker(c.helper))
		if errors.Is(err, ErrCreateAccount) {
			c.seenErrCreateAccount = true
			continue
		}
		if errors.Is(err, ErrUnsatisfiable) {
			// We do nothing if unsatisfiable.
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to process job", err)
		}

		// Update job and store broadcast in a single DB transaction.
		dbTransaction := c.helper.DatabaseTransaction(ctx)
		defer dbTransaction.Discard(ctx)

		// Update job (or store for the first time)
		jobIdentifier, err := c.storage.Update(ctx, dbTransaction, job)
		if err != nil {
			return fmt.Errorf("%w: unable to update job")
		}

		if broadcast != nil {
			// Construct Transaction
			transactionIdentifier, networkTransaction, err := c.createTransaction(ctx, broadcast)
			if err != nil {
				return fmt.Errorf("%w: unable to create transaction")
			}

			// Invoke Broadcast storage (in same TX as update job)
			if err := c.helper.Broadcast(ctx, dbTransaction, jobIdentifier, broadcast.Network, broadcast.Intent, transactionIdentifier, networkTransaction); err != nil {
				return fmt.Errorf("%w: unable to enque broadcast", err)
			}
		}

		// Commit db transaction
		if err := dbTransaction.Commit(ctx); err != nil {
			return fmt.Errorf("%w: unable to commit job update", err)
		}

		// Run Broadcast all (instead of running inside Broadcast)
		if err := c.helper.BroadcastAll(ctx); err != nil {
			return fmt.Errorf("%w: unable to broadcast all transactions", err)
		}

		// Reset all vars
		c.resetVars()
	}

	return ctx.Err()
}