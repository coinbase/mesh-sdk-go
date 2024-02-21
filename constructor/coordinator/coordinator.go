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

package coordinator

import (
	"context"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/fatih/color"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
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
	if len(inputWorkflows) == 0 {
		return nil, ErrNoWorkflows
	}

	workflowNames := make([]string, len(inputWorkflows))
	workflows := []*job.Workflow{}
	var createAccountWorkflow *job.Workflow
	var requestFundsWorkflow *job.Workflow
	var returnFundsWorkflow *job.Workflow
	for i, workflow := range inputWorkflows {
		if utils.ContainsString(workflowNames, workflow.Name) {
			return nil, ErrDuplicateWorkflows
		}

		if workflow.Concurrency <= 0 {
			return nil, ErrInvalidConcurrency
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

		if workflow.Name == string(job.ReturnFunds) {
			// We allow for unlimited concurrency here unlike
			// other reserved workflows.
			returnFundsWorkflow = workflow
			continue
		}

		workflows = append(workflows, workflow)
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
		returnFundsWorkflow:   returnFundsWorkflow,
	}, nil
}

func (c *Coordinator) findJob(
	ctx context.Context,
	dbTx database.Transaction,
	returnFunds bool,
) (*job.Job, error) {
	// Look for any jobs ready for processing. If one is found,
	// we return that as the next job to process.
	ready, err := c.storage.Ready(ctx, dbTx)
	if err != nil {
		return nil, fmt.Errorf(
			"failed to return ready jobs: %w",
			err,
		)
	}
	for _, job := range ready {
		if utils.ContainsString(c.attemptedJobs, job.Identifier) {
			continue
		}

		return job, nil
	}

	// We should only attempt the ReturnFunds workflow
	// if returnFunds is true. If it is true, we know that
	// c.returnFundsWorkflow must be defined.
	availableWorkflows := c.workflows
	if returnFunds {
		availableWorkflows = []*job.Workflow{c.returnFundsWorkflow}
	}

	// Attempt non-reserved workflows
	for _, workflow := range availableWorkflows {
		if utils.ContainsString(c.attemptedWorkflows, workflow.Name) {
			continue
		}

		processing, err := c.storage.Processing(ctx, dbTx, workflow.Name)
		if err != nil {
			return nil, fmt.Errorf(
				"failed to return processing jobs for workflow %s: %w",
				workflow.Name,
				err,
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
			"failed to return broadcasting jobs: %w",
			err,
		)
	}

	// Check if ErrCreateAccount, then create account if less
	// processing CreateAccount jobs than ReservedWorkflowConcurrency.
	if c.seenErrCreateAccount && c.createAccountWorkflow != nil {
		processing, err := c.storage.Processing(ctx, dbTx, string(job.CreateAccount))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to return processing jobs for workflow %s: %w",
				string(job.CreateAccount),
				err,
			)
		}

		if len(processing) >= job.ReservedWorkflowConcurrency {
			return nil, ErrNoAvailableJobs
		}

		return job.New(c.createAccountWorkflow), nil
	}

	if len(allBroadcasts) > 0 {
		return nil, ErrNoAvailableJobs
	}

	// If we are returning funds, we should exit here
	// because we don't want to create any new accounts
	// or request funds while returning funds.
	if returnFunds {
		return nil, ErrReturnFundsComplete
	}

	if c.requestFundsWorkflow != nil {
		processing, err := c.storage.Processing(ctx, dbTx, string(job.RequestFunds))
		if err != nil {
			return nil, fmt.Errorf(
				"failed to return processing jobs for workflow %s: %w",
				string(job.RequestFunds),
				err,
			)
		}

		if len(processing) >= job.ReservedWorkflowConcurrency {
			return nil, ErrNoAvailableJobs
		}

		return job.New(c.requestFundsWorkflow), nil
	}

	return nil, ErrStalled
}

// createTransaction constructs and signs a transaction with the provided intent.
func (c *Coordinator) createTransaction(
	ctx context.Context,
	dbTx database.Transaction,
	broadcast *job.Broadcast,
) (*types.TransactionIdentifier, string, []*types.Amount, error) {
	metadataRequest, requiredPublicKeys, err := c.helper.Preprocess(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		broadcast.Metadata,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to preprocess: %w", err)
	}

	publicKeys := make([]*types.PublicKey, len(requiredPublicKeys))
	for i, accountIdentifier := range requiredPublicKeys {
		keyPair, err := c.helper.GetKey(ctx, dbTx, accountIdentifier)
		if err != nil {
			return nil, "", nil, fmt.Errorf(
				"unable to get key for address %s: %w",
				accountIdentifier.Address,
				err,
			)
		}

		publicKeys[i] = keyPair.PublicKey
	}

	requiredMetadata, suggestedFees, err := c.helper.Metadata(
		ctx,
		broadcast.Network,
		metadataRequest,
		publicKeys,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to construct metadata: %w", err)
	}

	if broadcast.DryRun {
		return nil, "", suggestedFees, nil
	}

	unsignedTransaction, payloads, err := c.helper.Payloads(
		ctx,
		broadcast.Network,
		broadcast.Intent,
		requiredMetadata,
		publicKeys,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to construct payloads: %w", err)
	}

	parsedOps, signers, _, err := c.helper.Parse(
		ctx,
		broadcast.Network,
		false,
		unsignedTransaction,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to parse unsigned transaction: %w", err)
	}

	if len(signers) != 0 {
		return nil, "", nil, ErrSignersNotEmpty
	}

	if err := c.parser.ExpectedOperations(broadcast.Intent, parsedOps, false, false); err != nil {
		log.Printf(
			"expected %s, observed %s\n",
			types.PrintStruct(broadcast.Intent),
			types.PrintStruct(parsedOps),
		)
		return nil, "", nil, fmt.Errorf("unsigned parsed ops do not match intent: %w", err)
	}

	signatures, err := c.helper.Sign(ctx, payloads)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to sign payloads: %w", err)
	}

	networkTransaction, err := c.helper.Combine(
		ctx,
		broadcast.Network,
		unsignedTransaction,
		signatures,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to combine signatures: %w", err)
	}

	signedParsedOps, signers, _, err := c.helper.Parse(
		ctx,
		broadcast.Network,
		true,
		networkTransaction,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to parse signed transaction: %w", err)
	}

	if err := c.parser.ExpectedOperations(broadcast.Intent, signedParsedOps, false, false); err != nil {
		log.Printf(
			"expected %s, observed %s\n",
			types.PrintStruct(broadcast.Intent),
			types.PrintStruct(signedParsedOps),
		)
		return nil, "", nil, fmt.Errorf("signed parsed ops do not match intent: %w", err)
	}

	if err := parser.ExpectedSigners(payloads, signers); err != nil {
		return nil, "", nil, fmt.Errorf("signed transactions signers do not match intent: %w", err)
	}

	transactionIdentifier, err := c.helper.Hash(
		ctx,
		broadcast.Network,
		networkTransaction,
	)
	if err != nil {
		return nil, "", nil, fmt.Errorf("unable to get transaction hash: %w", err)
	}

	return transactionIdentifier, networkTransaction, nil, nil
}

// BroadcastComplete is called by the broadcast coordinator
// when a transaction broadcast has completed. If the transaction
// is nil, then the transaction did not succeed.
func (c *Coordinator) BroadcastComplete(
	ctx context.Context,
	dbTx database.Transaction,
	jobIdentifier string,
	transaction *types.Transaction,
) error {
	j, err := c.storage.Get(ctx, dbTx, jobIdentifier)
	if err != nil {
		return fmt.Errorf(
			"failed to get job %s: %w",
			types.PrintStruct(jobIdentifier),
			err,
		)
	}

	if err := j.BroadcastComplete(ctx, transaction); err != nil {
		return fmt.Errorf("unable to mark broadcast complete: %w", err)
	}

	if _, err := c.storage.Update(ctx, dbTx, j); err != nil {
		return fmt.Errorf("unable to update job %s: %w", types.PrintStruct(j), err)
	}

	// We are optimistically resetting all vars here
	// although the update could get rolled back. In the worst
	// case, we will attempt to process a few extra jobs
	// that are unsatisfiable.
	c.resetVars()

	// If the transaction is nil, the broadcast failed.
	if transaction == nil {
		color.Red(
			"broadcast failed for job \"%s (%s)\"\n",
			j.Workflow,
			jobIdentifier,
		)

		return nil
	}

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
		return fmt.Errorf("unable to calculate balance changes: %w", err)
	}

	for _, balanceChange := range balanceChanges {
		parsedDiff, err := types.BigInt(balanceChange.Difference)
		if err != nil {
			return fmt.Errorf("unable to parse balance change difference: %w", err)
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
			return fmt.Errorf("unable to handle transaction created: %w", err)
		}
	}

	// Run Broadcast all after transaction committed.
	if err := c.helper.BroadcastAll(ctx); err != nil {
		return fmt.Errorf("unable to broadcast all transactions: %w", err)
	}

	return nil
}

// process orchestrates the execution of workflows
// and the broadcast of transactions. It returns the amount
// of time to sleep before calling again.
func (c *Coordinator) process( // nolint:gocognit
	ctx context.Context,
	returnFunds bool,
) (time.Duration, error) {
	if !c.helper.HeadBlockExists(ctx) {
		// We will sleep until at least one block has been synced.
		// Many of the storage-based commands require a synced block
		// to work correctly (i.e. when fetching a balance, a block
		// must be returned).
		return NoHeadBlockWaitTime, nil
	}

	// Update job and store broadcast in a single DB transaction.
	// If job update fails, all associated state changes are rolled
	// back.
	dbTx := c.helper.DatabaseTransaction(ctx)
	defer dbTx.Discard(ctx)

	// Attempt to find a Job to process.
	j, err := c.findJob(ctx, dbTx, returnFunds)
	if errors.Is(err, ErrNoAvailableJobs) {
		log.Println("waiting for available jobs...")

		c.resetVars()
		return NoJobsWaitTime, nil
	}
	if errors.Is(err, ErrStalled) {
		color.Yellow(
			"processing stalled, the request_funds and/or create_account workflow(s) are/is not defined properly",
		)

		return -1, ErrStalled
	}
	if errors.Is(err, ErrReturnFundsComplete) {
		color.Cyan("fund return complete!")

		return -1, nil
	}
	if err != nil {
		return -1, fmt.Errorf("unable to find job: %w", err)
	}

	statusMessage := fmt.Sprintf(`processing workflow "%s"`, j.Workflow)
	if len(j.Identifier) > 0 {
		statusMessage = fmt.Sprintf(`%s for job "%s"`, statusMessage, j.Identifier)
	}
	log.Println(statusMessage)

	broadcast, executionErr := c.worker.Process(ctx, dbTx, j)
	if executionErr != nil {
		if errors.Is(executionErr.Err, worker.ErrCreateAccount) {
			c.addToUnprocessed(j)
			c.seenErrCreateAccount = true
			return 0, nil
		}
		if errors.Is(executionErr.Err, worker.ErrUnsatisfiable) {
			c.addToUnprocessed(j)
			return 0, nil
		}

		// Log the exeuction error to the terminal so
		// the caller can debug their scripts.
		executionErr.Log()

		return -1, fmt.Errorf(
			"unable to process job %s: %w",
			types.PrintStruct(j),
			executionErr.Err,
		)
	}

	// Update job (or store for the first time)
	//
	// Note, we ALWAYS store jobs even if they are complete on
	// their first run so that we can have a full view of everything
	// we've done in JobStorage.
	jobIdentifier, err := c.storage.Update(ctx, dbTx, j)
	if err != nil {
		return -1, fmt.Errorf("unable to update job %s: %w", types.PrintStruct(j), err)
	}
	j.Identifier = jobIdentifier

	var transactionCreated *types.TransactionIdentifier
	if broadcast != nil {
		// Construct Transaction (or dry run)
		transactionIdentifier, networkTransaction, suggestedFees, err := c.createTransaction(
			ctx,
			dbTx,
			broadcast,
		)
		if err != nil {
			return -1, fmt.Errorf("unable to create transaction: %w", err)
		}

		if broadcast.DryRun {
			// Update the job with the result of the dry run. This will
			// mark it as ready!
			if err := j.DryRunComplete(ctx, suggestedFees); err != nil {
				return -1, fmt.Errorf("unable to mark dry run complete: %w", err)
			}

			if _, err := c.storage.Update(ctx, dbTx, j); err != nil {
				return -1, fmt.Errorf("unable to update job after dry run: %w", err)
			}
		} else {
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
				broadcast.Metadata,
			); err != nil {
				return -1, fmt.Errorf("unable to enqueue broadcast: %w", err)
			}

			transactionCreated = transactionIdentifier
			log.Printf(
				`created transaction "%s" for job "%s"`,
				transactionIdentifier.Hash,
				jobIdentifier,
			)
		}
	}

	// Reset all vars
	c.resetVars()
	log.Printf(`processed workflow "%s" for job "%s"`, j.Workflow, jobIdentifier)

	// Commit db transaction
	if err := dbTx.Commit(ctx); err != nil {
		return -1, fmt.Errorf("unable to commit job update: %w", err)
	}

	// Invoke handlers and broadcast
	if err := c.invokeHandlersAndBroadcast(ctx, jobIdentifier, transactionCreated); err != nil {
		return -1, fmt.Errorf("unable to handle job %s success: %w", jobIdentifier, err)
	}

	return 0, nil
}

// processLoop calls process until we should
// not continue or an error is returned.
func (c *Coordinator) processLoop(
	ctx context.Context,
	returnFunds bool,
) error {
	// Make sure to cleanup the state from the
	// last execution.
	c.resetVars()

	// We don't include this loop inside process
	// so that we can defer dbTx.Discard(ctx). Defer
	// is only invoked when a function returns.
	for ctx.Err() == nil {
		sleepTime, err := c.process(ctx, returnFunds)
		if err != nil {
			return err
		}

		switch sleepTime {
		case 0:
			continue
		case -1:
			return nil
		default:
			time.Sleep(sleepTime)
		}
	}

	return ctx.Err()
}

// Process creates and executes jobs
// until failure.
func (c *Coordinator) Process(
	ctx context.Context,
) error {
	return c.processLoop(ctx, false)
}

// ReturnFunds attempts to execute
// the ReturnFunds workflow until
// it is no longer satisfiable. This
// is typically called on shutdown
// to return funds to a faucet.
func (c *Coordinator) ReturnFunds(
	ctx context.Context,
) error {
	// We return immediately if there is no return
	// funds workflow defined.
	if c.returnFundsWorkflow == nil {
		return nil
	}

	color.Cyan("attemping fund return...")
	return c.processLoop(ctx, true)
}
