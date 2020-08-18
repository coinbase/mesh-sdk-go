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

package constructor

import (
	"context"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ReservedVariable is a reserved variable
// field in a Job's state.
type ReservedVariable string

const (
	// Network is the *types.NetworkIdentifier to use
	// for broadcast.
	Network ReservedVariable = "network"

	// Operations are the []*types.Operation to use
	// as intent.
	Operations ReservedVariable = "operations"

	// PreprocessMetadata is the metadata to provide to /construction/preprocess.
	PreprocessMetadata ReservedVariable = "preprocess_metadata"

	// Transaction is the *types.Transaction confirmed
	// on-chain.
	Transaction ReservedVariable = "transaction"
)

// ActionType is a type of Action that can be processed.
type ActionType string

const (
	// GenerateKey creates a new *keys.KeyPair.
	GenerateKey ActionType = "generate_key"

	// SaveAddress saves a generated *keys.KeyPair
	// and address to key storage.
	SaveAddress ActionType = "save_address"

	// Derive calls `/construction/derive` with a *keys.PublicKey.
	Derive ActionType = "derive"

	// SetVariable allows for setting the value of any
	// variable (as opposed to calculating it using
	// other actions).
	SetVariable ActionType = "set_variable"

	// FindAccount finds some unlocked account that optionally
	// has a minimum balance of funds.
	FindAccount ActionType = "find_account"

	// FindCoin finds some coin above a certain amount. It is
	// possible to specify coins which should not be considered (by identifier).
	FindCoin ActionType = "find_coin"

	// PrintMessage prints some message to the terminal. This is
	// typically used for funding an account.
	PrintMessage ActionType = "print_message"

	// WaitForFunds halts execution until a specified
	// address has a minimum balance.
	WaitForFunds ActionType = "wait_for_funds"

	// Math is used to perform addition or subtraction of variables. It is
	// most commonly used to determine how much to send to a change output
	// on UTXO blockchains.
	Math ActionType = "math"

	// RandomString generates a string according to some provided regex.
	// It is used to generate account names for blockchains that require
	// on-chain origination.
	RandomString ActionType = "random_string"
)

// Action is a step of computation that
// where the result is saved to OutputPath.
type Action struct {
	Input      string
	Type       ActionType
	OutputPath string
}

// GenerateKeyInput is the input for GenerateKey.
type GenerateKeyInput struct {
	CurveType types.CurveType `json:"curve_type"`
}

// Scenario is a collection of Actions with a specific
// confirmation depth.
//
// There is a special variable you can set at the end
// of a scenario called "<scenario_name>.operations" to
// indicate that a transaction should be broadcast. It is
// also possible to specify the network where the transaction
// should be broadcast and the metadata to provide in a
// call to /construction/preprocess.
//
// Once a scenario is broadcasted and confirmed,
// the transaction details are placed in a special
// variable called "transaction". This can be used
// in scenarios following the execution of this one.
type Scenario struct {
	Name    string
	Actions []*Action

	// We set ConfirmationDepth on a per scenario basis because
	// certain transactions may only be considered complete
	// after some time (ex: staking transaction).
	ConfirmationDepth int64
}

// ReservedWorkflow is a Workflow reserved for special circumstances.
// All ReservedWorkflows must exist when running the constructor.
type ReservedWorkflow string

const (
	// Note: required workflows will be executed with a concurrency of 1.

	// CreateAccount is where another account (already with funds)
	// creates a new account. It is possible to configure how many
	// accounts should be created.
	CreateAccount ReservedWorkflow = "create_account"

	// RequestFunds is where the user funds an account. This flow
	// is invoked when there are no pending broadcasts and it is not possible
	// to make progress on any Flows or start new ones.
	RequestFunds ReservedWorkflow = "request_funds"
)

// Workflow is a collection of scenarios to run (i.e.
// transactions to broadcast) with some shared state.
type Workflow struct {
	Name string

	// Concurrency is the number of workflows of a particular
	// kind to execute at once. For example, you may not want
	// to process concurrent workflows of some staking operations
	// that take days to play out.
	Concurrency int
	Scenarios   []*Scenario
}

// JobStatus is status of a Job.
type JobStatus string

const (
	// Ready means that a Job is ready to process.
	Ready JobStatus = "ready"

	// Broadcasting means that the intent of the last
	// scenario is broadcasting.
	Broadcasting JobStatus = "broadcasting"

	// Failed means that Broadcasting failed.
	Failed JobStatus = "failed"

	// Completed means that all scenarios were
	// completed successfully.
	Completed JobStatus = "completed"
)

// Job is an instantion of a Workflow.
type Job struct {
	// Identifier is a UUID that is generated
	// when a Job is stored in JobStorage for the
	// first time. When executing the first scenario
	// in a Job, this will be empty.
	Identifier string
	State      string
	Index      int
	Status     JobStatus

	// Workflow is the name of the workflow being executed.
	Workflow string

	// Scenarios are copied into each context in case
	// a configuration file changes that could corrupt
	// in-process flows.
	Scenarios []*Scenario
}

// Broadcast contains information needed to create
// and broadcast a transaction. Broadcast is returned
// from Job processing only IF a broadcast is required.
type Broadcast struct {
	Network           *types.NetworkIdentifier
	Intent            []*types.Operation
	Metadata          map[string]interface{}
	ConfirmationDepth int64
}

// WorkerHelper is used by the worker to process Jobs.
type WorkerHelper interface {
	Derive(
		context.Context,
		*types.NetworkIdentifier,
		*types.PublicKey,
		map[string]interface{},
	) (string, map[string]interface{}, error)
}

// Worker processes jobs.
type Worker struct {
	helper WorkerHelper
}
