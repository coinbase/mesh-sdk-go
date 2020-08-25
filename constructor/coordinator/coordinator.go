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
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// New parses a slice of input Workflows
// and creates a new *Coordinator.
func New(
	storage JobStorage,
	helper Helper,
	parser *parser.Parser,
	inputWorkflows []*job.Workflow,
) (*Coordinator, error) {
	workflowNames := make([]string, len(inputWorkflows))
	workflows := []*job.Workflow{}
	var createAccountWorkflow *job.Workflow
	var requestFundsWorkflow *job.Workflow
	for i, workflow := range inputWorkflows {
		if utils.ContainsString(workflowNames, workflow.Name) {
			return nil, ErrDuplicateWorkflows
		}
		workflowNames[i] = workflow.Name

		if workflow.Name == string(job.CreateAccount) {
			if workflow.Concurrency != ReservedWorkflowConcurrency {
				return nil, ErrIncorrectConcurrency
			}

			createAccountWorkflow = workflow
			continue
		}

		if workflow.Name == string(job.RequestFunds) {
			if workflow.Concurrency != ReservedWorkflowConcurrency {
				return nil, ErrIncorrectConcurrency
			}

			requestFundsWorkflow = workflow
			continue
		}

		workflows = append(workflows, workflow)
	}

	return &Coordinator{
		storage:               storage,
		helper:                helper,
		worker:                worker.New(helper),
		parser:                parser,
		attemptedJobs:         []string{},
		attemptedWorkflows:    []string{},
		seenErrCreateAccount:  false,
		workflows:             workflows,
		createAccountWorkflow: createAccountWorkflow,
		requestFundsWorkflow:  requestFundsWorkflow,
	}, nil
}

func (c *Coordinator) findJob(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) (*job.Job, error) {
	// Look for any jobs ready for processing. If one is found,
	// we return that as the next job to process.
	ready, err := c.storage.Ready(ctx, dbTx)
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

	// Attempt non-reserved workflows
	for _, workflow := range c.workflows {
		if utils.ContainsString(c.attemptedWorkflows, workflow.Name) {
			continue
		}

		processing, err := c.storage.Processing(ctx, dbTx, workflow.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s",
				ErrJobsUnretrievable,
				err.Error(),
			)
		}

		if processing >= workflow.Concurrency {
			continue
		}

		return job.New(workflow), nil
	}

	// Check if broadcasts, then ErrNoAvailableJobs
	allBroadcasts, err := c.storage.Broadcasting(ctx, dbTx)
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
		processing, err := c.storage.Processing(ctx, dbTx, string(job.CreateAccount))
		if err != nil {
			return nil, fmt.Errorf(
				"%w: %s",
				ErrJobsUnretrievable,
				err.Error(),
			)
		}

		if processing >= ReservedWorkflowConcurrency {
			return nil, ErrNoAvailableJobs
		}

		if c.createAccountWorkflow != nil {
			return job.New(c.createAccountWorkflow), nil
		}

		log.Println("Create account workflow is missing!")
	}

	// Return request funds (if defined, else error)
	if c.requestFundsWorkflow == nil {
		return nil, ErrRequestFundsWorkflowMissing
	}

	processing, err := c.storage.Processing(ctx, dbTx, string(job.RequestFunds))
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrJobsUnretrievable,
			err.Error(),
		)
	}

	if processing >= ReservedWorkflowConcurrency {
		return nil, ErrNoAvailableJobs
	}

	return job.New(c.requestFundsWorkflow), nil
}

// createTransaction constructs and signs a transaction with the provided intent.
func (c *Coordinator) createTransaction(
	ctx context.Context,
	broadcast *job.Broadcast,
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
	dbTx storage.DatabaseTransaction,
	jobIdentifier string,
	transaction *types.Transaction,
) error {
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

	log.Printf(`broadcast complete for "%s"`, jobIdentifier)

	return nil
}

func (c *Coordinator) resetVars() {
	c.attemptedJobs = []string{}
	c.attemptedWorkflows = []string{}
	c.seenErrCreateAccount = false
}

func (c *Coordinator) addToUnprocessed(job *job.Job) {
	if len(job.Identifier) == 0 {
		c.attemptedWorkflows = append(c.attemptedWorkflows, job.Workflow)
		return
	}
	c.attemptedJobs = append(c.attemptedJobs, job.Identifier)
}

// Process creates and executes jobs
// until failure.
func (c *Coordinator) Process(
	ctx context.Context,
) error {
	for ctx.Err() == nil {
		if !c.helper.HeadBlockExists(ctx) {
			log.Println("waiting for first block synced...")

			// We will sleep until at least one block has been synced.
			// Many of the storage-based commands require a synced block
			// to work correctly (i.e. when fetching a balance, a block
			// must be returned).
			time.Sleep(NoHeadBlockWaitTime)
			continue
		}

		// Update job and store broadcast in a single DB transaction.
		// If job update fails, all associated state changes are rolled
		// back.
		dbTx := c.helper.DatabaseTransaction(ctx)
		defer dbTx.Discard(ctx)

		// Attempt to find a Job to process.
		job, err := c.findJob(ctx, dbTx)
		if errors.Is(err, ErrNoAvailableJobs) {
			log.Println("waiting for available jobs...")

			time.Sleep(NoJobsWaitTime)
			c.resetVars()
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to find job", err)
		}

		statusMessage := fmt.Sprintf(`processing workflow "%s"`, job.Workflow)
		if len(job.Identifier) > 0 {
			statusMessage = fmt.Sprintf(`%s with identifier "%s"`, statusMessage, job.Identifier)
		}
		log.Println(statusMessage)

		broadcast, err := c.worker.Process(ctx, dbTx, job)
		if errors.Is(err, worker.ErrCreateAccount) {
			c.addToUnprocessed(job)
			c.seenErrCreateAccount = true
			continue
		}
		if errors.Is(err, worker.ErrUnsatisfiable) {
			c.addToUnprocessed(job)
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to process job", err)
		}

		// Update job (or store for the first time)
		//
		// Note, we ALWAYS store jobs even if they are complete on
		// their first run so that we can have a full view of everything
		// we've done in JobStorage.
		jobIdentifier, err := c.storage.Update(ctx, dbTx, job)
		if err != nil {
			return fmt.Errorf("%w: unable to update job", err)
		}

		if broadcast != nil {
			// Construct Transaction
			transactionIdentifier, networkTransaction, err := c.createTransaction(ctx, broadcast)
			if err != nil {
				return fmt.Errorf("%w: unable to create transaction", err)
			}

			// Invoke Broadcast storage (in same TX as update job)
			if err := c.helper.Broadcast(
				ctx,
				dbTx,
				jobIdentifier,
				broadcast.Network,
				broadcast.Intent,
				transactionIdentifier,
				networkTransaction,
			); err != nil {
				return fmt.Errorf("%w: unable to enque broadcast", err)
			}

			log.Printf("created transaction for job %s\n", jobIdentifier)
		}

		// Commit db transaction
		if err := dbTx.Commit(ctx); err != nil {
			return fmt.Errorf("%w: unable to commit job update", err)
		}

		// Run Broadcast all after transaction committed.
		if err := c.helper.BroadcastAll(ctx); err != nil {
			return fmt.Errorf("%w: unable to broadcast all transactions", err)
		}

		// Reset all vars
		c.resetVars()

		log.Printf(`processed workflow "%s" with identifier "%s"`, job.Workflow, jobIdentifier)
	}

	return ctx.Err()
}
