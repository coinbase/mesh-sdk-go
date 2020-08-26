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

	"github.com/fatih/color"
)

// New parses a slice of input Workflows
// and creates a new *Coordinator.
func New(
	storage JobStorage,
	helper Helper,
	handler Handler,
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
			if workflow.Concurrency != job.ReservedWorkflowConcurrency {
				return nil, ErrIncorrectConcurrency
			}

			createAccountWorkflow = workflow
			continue
		}

		if workflow.Name == string(job.RequestFunds) {
			if workflow.Concurrency != job.ReservedWorkflowConcurrency {
				return nil, ErrIncorrectConcurrency
			}

			requestFundsWorkflow = workflow
			continue
		}

		workflows = append(workflows, workflow)
	}

	if createAccountWorkflow == nil {
		return nil, ErrCreateAccountWorkflowMissing
	}

	if requestFundsWorkflow == nil {
		return nil, ErrRequestFundsWorkflowMissing
	}

	return &Coordinator{
		storage:               storage,
		helper:                helper,
		handler:               handler,
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
		fmt.Printf("%s\n", types.PrintStruct(job))
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

		if len(processing) >= workflow.Concurrency {
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

		if len(processing) >= job.ReservedWorkflowConcurrency {
			return nil, ErrNoAvailableJobs
		}

		return job.New(c.createAccountWorkflow), nil
	}

	processing, err := c.storage.Processing(ctx, dbTx, string(job.RequestFunds))
	if err != nil {
		return nil, fmt.Errorf(
			"%w: %s",
			ErrJobsUnretrievable,
			err.Error(),
		)
	}

	if len(processing) >= job.ReservedWorkflowConcurrency {
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
		log.Printf("expected %s, observed %s\n", types.PrintStruct(broadcast.Intent), types.PrintStruct(parsedOps))
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
		log.Printf("expected %s, observed %s\n", types.PrintStruct(broadcast.Intent), types.PrintStruct(signedParsedOps))
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
	j, err := c.storage.Get(ctx, dbTx, jobIdentifier)
	if err != nil {
		return fmt.Errorf(
			"%w: %s",
			ErrJobMissing,
			err.Error(),
		)
	}

	if err := j.BroadcastComplete(ctx, transaction); err != nil {
		return fmt.Errorf("%w: unable to mark broadcast complete", err)
	}

	if _, err := c.storage.Update(ctx, dbTx, j); err != nil {
		return fmt.Errorf("%w: unable to update job", err)
	}

	// We are optimisticall reseting all vars here
	// although the update could get rolled back.
	c.resetVars()
	statusString := fmt.Sprintf(
		"broadcast complete for job \"%s (%s)\" with transaction hash \"%s\"\n",
		j.Workflow,
		jobIdentifier,
		transaction.TransactionIdentifier.Hash,
	)

	// To calculate balance changes, we must create a fake block that
	// only contains the transaction we are completing.
	//
	// TODO: modify parser to calculate balance changes for a single
	// transaction.
	balanceChanges, err := c.parser.BalanceChanges(ctx, &types.Block{
		Transactions: []*types.Transaction{
			transaction,
		},
	}, false)
	if err != nil {
		return fmt.Errorf("%w: unable to calculate balance changes", err)
	}

	for _, balanceChange := range balanceChanges {
		parsedDiff, err := types.BigInt(balanceChange.Difference)
		if err != nil {
			return fmt.Errorf("%w: unable to parse Difference", err)
		}

		statusString = fmt.Sprintf(
			"%s%s -> %s\n",
			statusString,
			types.PrintStruct(balanceChange.Account),
			utils.PrettyAmount(parsedDiff, balanceChange.Currency),
		)
	}
	color.Magenta(statusString)

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

func (c *Coordinator) invokeHandlersAndBroadcast(
	ctx context.Context,
	jobIdentifier string,
	transactionCreated *types.TransactionIdentifier,
) error {
	if transactionCreated != nil {
		if err := c.handler.TransactionCreated(ctx, jobIdentifier, transactionCreated); err != nil {
			return fmt.Errorf("%w: unable to handle transaction created", err)
		}
	}

	// Run Broadcast all after transaction committed.
	if err := c.helper.BroadcastAll(ctx); err != nil {
		return fmt.Errorf("%w: unable to broadcast all transactions", err)
	}

	return nil
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

		// Attempt to find a Job to process.
		j, err := c.findJob(ctx, dbTx)
		if errors.Is(err, ErrNoAvailableJobs) {
			log.Println("waiting for available jobs...")

			c.resetVars()
			dbTx.Discard(ctx)
			time.Sleep(NoJobsWaitTime)
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: unable to find job", err)
		}

		statusMessage := fmt.Sprintf(`processing workflow "%s"`, j.Workflow)
		if len(j.Identifier) > 0 {
			statusMessage = fmt.Sprintf(`%s with identifier "%s"`, statusMessage, j.Identifier)
		}
		log.Println(statusMessage)

		broadcast, err := c.worker.Process(ctx, dbTx, j)
		if errors.Is(err, worker.ErrCreateAccount) {
			log.Println("err create account")
			c.addToUnprocessed(j)
			c.seenErrCreateAccount = true
			dbTx.Discard(ctx)
			continue
		}
		if errors.Is(err, worker.ErrUnsatisfiable) {
			log.Println("err unsatisfiable")
			c.addToUnprocessed(j)
			dbTx.Discard(ctx)
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
		jobIdentifier, err := c.storage.Update(ctx, dbTx, j)
		if err != nil {
			return fmt.Errorf("%w: unable to update job", err)
		}

		var transactionCreated *types.TransactionIdentifier
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
				broadcast.ConfirmationDepth,
			); err != nil {
				return fmt.Errorf("%w: unable to enque broadcast", err)
			}

			transactionCreated = transactionIdentifier
			log.Printf(`created transaction "%s" for job "%s"\n`, transactionIdentifier.Hash, jobIdentifier)
		}

		// Reset all vars
		c.resetVars()
		log.Printf(`processed workflow "%s" with identifier "%s"`, j.Workflow, jobIdentifier)

		// Commit db transaction
		if err := dbTx.Commit(ctx); err != nil {
			return fmt.Errorf("%w: unable to commit job update", err)
		}

		// Invoke handlers and broadcast
		if err := c.invokeHandlersAndBroadcast(ctx, jobIdentifier, transactionCreated); err != nil {
			return fmt.Errorf("%w: unable to handle job success", err)
		}
	}

	return ctx.Err()
}
