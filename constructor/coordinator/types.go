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
	"time"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/constructor/worker"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// NoHeadBlockWaitTime is the amount of
	// time we wait when no blocks have been
	// synced.
	NoHeadBlockWaitTime = 1 * time.Second

	// NoJobsWaitTime is the amount of time
	// we wait when no jobs are available
	// to process.
	NoJobsWaitTime = 10 * time.Second
)

// Helper is used by the coordinator to process Jobs.
// It is a superset of functions required by the constructor/worker.Helper.
type Helper interface {
	// HeadBlockExists returns a boolean indicating if a block
	// has been synced by BlockStorage.
	HeadBlockExists(context.Context) bool

	// DatabaseTransaction returns a new database.Transaction.
	// This is used to update jobs and enque them for broadcast atomically.
	DatabaseTransaction(context.Context) database.Transaction

	// StoreKey is called to persist a
	// *types.AccountIdentifier + KeyPair.
	StoreKey(
		context.Context,
		database.Transaction,
		*types.AccountIdentifier,
		*keys.KeyPair,
	) error

	// GetKey is called to get the *types.KeyPair
	// associated with an address.
	GetKey(
		context.Context,
		database.Transaction,
		*types.AccountIdentifier,
	) (*keys.KeyPair, error)

	// AllAccounts returns a slice of all known *types.AccountIdentifier.
	AllAccounts(
		context.Context,
		database.Transaction,
	) ([]*types.AccountIdentifier, error)

	// LockedAccounts is a slice of all *types.AccountIdentifier currently sending or receiving
	// funds.
	LockedAccounts(
		context.Context,
		database.Transaction,
	) ([]*types.AccountIdentifier, error)

	// Balance returns the balance
	// for a provided address.
	Balance(
		context.Context,
		database.Transaction,
		*types.AccountIdentifier,
		*types.Currency,
	) (*types.Amount, error)

	// Coins returns all *types.Coin owned by an address.
	Coins(
		context.Context,
		database.Transaction,
		*types.AccountIdentifier,
		*types.Currency,
	) ([]*types.Coin, error)

	// BroadcastAll broadcasts all transactions considered ready for
	// broadcast (unbroadcasted or stale).
	BroadcastAll(context.Context) error

	// Broadcast enqueues a particular intent for broadcast.
	Broadcast(
		context.Context,
		database.Transaction,
		string, // Job.Identifier
		*types.NetworkIdentifier,
		[]*types.Operation,
		*types.TransactionIdentifier,
		string, // network transaction
		int64, // confirmation depth
		map[string]interface{}, // transaction metadata
	) error

	// Derive returns a new *types.AccountIdentifier for a provided publicKey.
	Derive(
		context.Context,
		*types.NetworkIdentifier,
		*types.PublicKey,
		map[string]interface{},
	) (*types.AccountIdentifier, map[string]interface{}, error)

	// Preprocess calls the /construction/preprocess endpoint
	// on an offline node.
	Preprocess(
		context.Context,
		*types.NetworkIdentifier,
		[]*types.Operation,
		map[string]interface{},
	) (map[string]interface{}, []*types.AccountIdentifier, error)

	// Metadata calls the /construction/metadata endpoint
	// using the online node.
	Metadata(
		context.Context,
		*types.NetworkIdentifier,
		map[string]interface{},
		[]*types.PublicKey,
	) (map[string]interface{}, []*types.Amount, error)

	// Payloads calls the /construction/payloads endpoint
	// using the offline node.
	Payloads(
		context.Context,
		*types.NetworkIdentifier,
		[]*types.Operation,
		map[string]interface{},
		[]*types.PublicKey,
	) (string, []*types.SigningPayload, error)

	// Parse calls the /construction/parse endpoint
	// using the offline node.
	Parse(
		context.Context,
		*types.NetworkIdentifier,
		bool, // signed
		string, // transaction
	) ([]*types.Operation, []*types.AccountIdentifier, map[string]interface{}, error)

	// Combine calls the /construction/combine endpoint
	// using the offline node.
	Combine(
		context.Context,
		*types.NetworkIdentifier,
		string, // unsigned transaction
		[]*types.Signature,
	) (string, error)

	// Hash calls the /construction/hash endpoint
	// using the offline node.
	Hash(
		context.Context,
		*types.NetworkIdentifier,
		string, // network transaction
	) (*types.TransactionIdentifier, error)

	// Sign returns signatures for the provided
	// payloads.
	Sign(
		context.Context,
		[]*types.SigningPayload,
	) ([]*types.Signature, error)

	// SetBlob transactionally persists
	// a key and value.
	SetBlob(
		ctx context.Context,
		dbTx database.Transaction,
		key string,
		value []byte,
	) error

	// GetBlob transactionally retrieves
	// a key and value.
	GetBlob(
		ctx context.Context,
		dbTx database.Transaction,
		key string,
	) (bool, []byte, error)
}

// Handler is an interface called by the coordinator whenever
// an address is created or a transaction is created.
type Handler interface {
	TransactionCreated(
		context.Context,
		string, // job identifier
		*types.TransactionIdentifier,
	) error
}

// Coordinator faciliates the creation and processing
// of jobs.
type Coordinator struct {
	storage JobStorage
	handler Handler
	helper  Helper
	parser  *parser.Parser
	worker  *worker.Worker

	attemptedJobs        []string
	attemptedWorkflows   []string
	seenErrCreateAccount bool

	workflows             []*job.Workflow
	createAccountWorkflow *job.Workflow
	requestFundsWorkflow  *job.Workflow
	returnFundsWorkflow   *job.Workflow
}

// JobStorage allows for the persistent and transactional
// storage of Jobs.
type JobStorage interface {
	// Ready returns the jobs that are ready to be processed.
	Ready(
		context.Context,
		database.Transaction,
	) ([]*job.Job, error)

	// Broadcasting returns all jobs that are broadcasting.
	Broadcasting(
		context.Context,
		database.Transaction,
	) ([]*job.Job, error)

	// Processing returns the number of jobs processing
	// for a particular workflow.
	Processing(
		context.Context,
		database.Transaction,
		string,
	) ([]*job.Job, error)

	// Update stores an updated *Job in storage
	// and returns its UUID (which won't exist
	// on first update).
	Update(context.Context, database.Transaction, *job.Job) (string, error)

	// Get fetches a *Job by Identifier. It returns an error
	// if the identifier doesn't exist.
	Get(context.Context, database.Transaction, string) (*job.Job, error)
}
