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

import (
	"encoding/json"
	"net/http"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// ReservedWorkflowConcurrency is the expected concurrency
	// of the create account and request funds scenario.
	ReservedWorkflowConcurrency = 1
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

	// ConfirmationDepth is the amount of blocks we wait to confirm
	// a transaction. We allow setting this on a per scenario basis because
	// certain transactions may only be considered complete
	// after some time (ex: staking transaction).
	ConfirmationDepth ReservedVariable = "confirmation_depth"

	// DryRun is a boolean that indicates whether we should perform the
	// entire transaction construction process or just /construction/preprocess
	// and /construction/metadata to determine the suggested transaction
	// fee. If this variable is not populated, we assume that it is NOT
	// a dry run.
	DryRun ReservedVariable = "dry_run"

	// SuggestedFee is the []*types.Amount returned from
	// an implementation's /construction/metadata endpoint (if implemented).
	SuggestedFee ReservedVariable = "suggested_fee"
)

// ActionType is a type of Action that can be processed.
type ActionType string

const (
	// GenerateKey creates a new *keys.KeyPair.
	GenerateKey ActionType = "generate_key"

	// SaveAccount saves a generated *keys.KeyPair
	// and *types.AccountIdentifier to key storage.
	SaveAccount ActionType = "save_account"

	// Derive calls `/construction/derive` with a *keys.PublicKey.
	Derive ActionType = "derive"

	// SetVariable allows for setting the value of any
	// variable (as opposed to calculating it using
	// other actions).
	SetVariable ActionType = "set_variable"

	// FindBalance finds some unlocked account that optionally
	// has a minimum balance of funds.
	// FindCoin finds some coin above a certain amount. It is
	// possible to specify coins which should not be considered (by identifier).
	FindBalance ActionType = "find_balance"

	// PrintMessage can be used to print any message
	// to the terminal. This is usually used to indicate
	// to the caller that they should deposit money to an
	// address. It is left generic to allow the user to
	// include whatever content they would like in the
	// message (especially useful in on-chain origination).
	// This can also be used to log information during
	// execution.
	PrintMessage ActionType = "print_message"

	// Math is used to perform addition or subtraction of variables. It is
	// most commonly used to determine how much to send to a change output
	// on UTXO blockchains.
	Math ActionType = "math"

	// RandomString generates a string according to some provided regex.
	// It is used to generate account names for blockchains that require
	// on-chain origination.
	RandomString ActionType = "random_string"

	// RandomNumber generates a random number in some range [min, max).
	// It is used to generate random transaction amounts.
	RandomNumber ActionType = "random_number"

	// FindCurrencyAmount finds a *types.Amount for a certain currency
	// in an array of []*types.Amount. This is typically used when parsing
	// the suggested fee response from /construction/metadata.
	FindCurrencyAmount ActionType = "find_currency_amount"

	// Assert ensures that a provided number is >= 0 and causes
	// execution to exit if this is not true. This is useful when
	// ensuring that an account has sufficient balance to pay the
	// suggested fee to broadcast a transaction.
	Assert ActionType = "assert"

	// LoadEnv loads some value from an environment variable. This
	// is very useful injecting an API token for algorithmic fauceting
	// when running CI.
	LoadEnv ActionType = "load_env"

	// HTTPRequest makes an HTTP request at some URL. This is useful
	// for making a request to a faucet to automate Construction API
	// testing.
	HTTPRequest ActionType = "http_request"

	// SetBlob stores an arbitrary blob at some key (any valid JSON is
	// accepted as a key). If a value at a key already exists,
	// it will be overwritten.
	//
	// SetBlob is often used when there is some metadata created
	// during a workflow execution that needs to be accessed
	// in another workflow (i.e. a mapping between different generated
	// addresses).
	SetBlob ActionType = "set_blob"

	// GetBlob attempts to retrieve some previously saved blob.
	// If the blob is not accessible, it will return an error.
	GetBlob ActionType = "get_blob"
)

// Action is a step of computation that
// where the result is saved to OutputPath.
type Action struct {
	Input      string     `json:"input"`
	Type       ActionType `json:"type"`
	OutputPath string     `json:"output_path,omitempty"`
}

// GenerateKeyInput is the input for GenerateKey.
type GenerateKeyInput struct {
	CurveType types.CurveType `json:"curve_type"`
}

// SaveAccountInput is the input for SaveAccount.
type SaveAccountInput struct {
	AccountIdentifier *types.AccountIdentifier `json:"account_identifier"`
	KeyPair           *keys.KeyPair            `json:"keypair"`
}

// RandomStringInput is the input to RandomString.
type RandomStringInput struct {
	Regex string `json:"regex"`

	// Limit is the maximum number of times each star, range, or
	// plus character could be repeated.
	Limit int `json:"limit"`
}

// MathOperation is some mathematical operation that
// can be performed on 2 numbers.
type MathOperation string

const (
	// Addition is adding LeftValue + RightValue.
	Addition MathOperation = "addition"

	// Subtraction is LeftValue - RightValue.
	Subtraction MathOperation = "subtraction"

	Multiplication MathOperation = "multiplication"

	Division MathOperation = "division"
)

// MathInput is the input to Math.
type MathInput struct {
	Operation  MathOperation `json:"operation"`
	LeftValue  string        `json:"left_value"`
	RightValue string        `json:"right_value"`
}

// FindBalanceInput is the input to FindBalance.
type FindBalanceInput struct {
	// AccountIdentifier can be optionally provided to ensure the balance returned
	// is for a particular address (this is used when fetching the balance
	// of the same account in multiple currencies, when requesting funds,
	// or when constructing a multi-input UTXO transfer with the
	// same address).
	AccountIdentifier *types.AccountIdentifier `json:"account_identifier,omitempty"`

	// SubAccountIdentifier can be used to find addresses with particular
	// SubAccount balances. This is particularly useful for
	// orchestrating staking transactions.
	SubAccountIdentifier *types.SubAccountIdentifier `json:"sub_account_identifier,omitempty"`

	// NotAddress can be populated to ensure a different
	// address is found. This is useful when avoiding a
	// self-transfer.
	NotAddress []string `json:"not_address,omitempty"`

	// NotAccountIdentifier can be used to avoid entire
	// *types.AccountIdentifiers and is used when only
	// certain SubAccountIdentifiers of an Address are
	// desired.
	NotAccountIdentifier []*types.AccountIdentifier `json:"not_account_identifier,omitempty"`

	// MinimumBalance is the minimum required balance that must be found.
	MinimumBalance *types.Amount `json:"minimum_balance,omitempty"`

	// RequireCoin indicates if a coin must be found with the minimum balance.
	// This is useful for orchestrating transfers on UTXO-based blockchains.
	RequireCoin bool `json:"require_coin,omitempty"`

	// NotCoins indicates that certain coins should not be considered. This is useful
	// for avoiding using the same Coin twice.
	NotCoins []*types.CoinIdentifier `json:"not_coins,omitempty"`

	// CreateLimit is used to determine if we should create a new address using
	// the CreateAccount Workflow. This will only occur if the
	// total number of addresses is under some pre-defined limit.
	// If the value is <= 0, we will not attempt to create.
	CreateLimit int `json:"create_limit,omitempty"`

	// CreateProbability is used to determine if a new account should be
	// created with some probability [0, 100). This will override the search
	// for any valid accounts and instead return ErrCreateAccount.
	CreateProbability int `json:"create_probability,omitempty"`
}

// FindBalanceOutput is returned by FindBalance.
type FindBalanceOutput struct {
	// AccountIdentifier is the account associated with the balance
	// (and coin).
	AccountIdentifier *types.AccountIdentifier `json:"account_identifier"`

	// Balance found at a particular currency.
	Balance *types.Amount `json:"balance"`

	// Coin is populated if RequireCoin is true.
	Coin *types.CoinIdentifier `json:"coin,omitempty"`
}

// RandomNumberInput is used to generate a random
// number in the range [minimum, maximum).
type RandomNumberInput struct {
	Minimum string `json:"minimum"`
	Maximum string `json:"maximum"`
}

// FindCurrencyAmountInput is the input
// to FindCurrencyAmount.
type FindCurrencyAmountInput struct {
	Currency *types.Currency `json:"currency"`
	Amounts  []*types.Amount `json:"amounts"`
}

// HTTPMethod is a type representing
// allowed HTTP methods.
type HTTPMethod string

// Supported HTTP Methods
const (
	MethodGet  = http.MethodGet
	MethodPost = http.MethodPost
)

// HTTPRequestInput is the input to
// HTTP Request.
type HTTPRequestInput struct {
	Method  string `json:"method"`
	URL     string `json:"url"`
	Timeout int    `json:"timeout"`

	// If the Method is POST, the Body
	// can be populated with JSON.
	Body string `json:"body"`
}

// SetBlobInput is the input to
// SetBlob.
type SetBlobInput struct {
	Key   interface{}     `json:"key"`
	Value json.RawMessage `json:"value"`
}

// GetBlobInput is the input to
// GetBlob.
type GetBlobInput struct {
	Key interface{} `json:"key"`
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
	Name    string    `json:"name"`
	Actions []*Action `json:"actions"`
}

// ReservedWorkflow is a Workflow reserved for special circumstances.
// All ReservedWorkflows must exist when running the constructor.
type ReservedWorkflow string

const (
	// CreateAccount is where another account (already with funds)
	// creates a new account. It is possible to configure how many
	// accounts should be created. CreateAccount must be executed
	// with a concurrency of 1.
	CreateAccount ReservedWorkflow = "create_account"

	// RequestFunds is where the user funds an account. This flow
	// is invoked when there are no pending broadcasts and it is not possible
	// to make progress on any Flows or start new ones. RequestFunds
	// must be executed with a concurrency of 1.
	RequestFunds ReservedWorkflow = "request_funds"

	// ReturnFunds is invoked on shutdown so funds can be
	// returned to a single address (like a faucet). This
	// is useful for CI testing.
	ReturnFunds ReservedWorkflow = "return_funds"
)

// Workflow is a collection of scenarios to run (i.e.
// transactions to broadcast) with some shared state.
type Workflow struct {
	Name string `json:"name"`

	// Concurrency is the number of workflows of a particular
	// kind to execute at once. For example, you may not want
	// to process concurrent workflows of some staking operations
	// that take days to play out.
	Concurrency int         `json:"concurrency"`
	Scenarios   []*Scenario `json:"scenarios"`
}

// Status is status of a Job.
type Status string

const (
	// Ready means that a Job is ready to process.
	Ready Status = "ready"

	// Broadcasting means that the intent of the last
	// scenario is broadcasting.
	Broadcasting Status = "broadcasting"

	// Failed means that Broadcasting failed.
	Failed Status = "failed"

	// Completed means that all scenarios were
	// completed successfully.
	Completed Status = "completed"
)

// Job is an instantion of a Workflow.
type Job struct {
	// Identifier is a UUID that is generated
	// when a Job is stored in JobStorage for the
	// first time. When executing the first scenario
	// in a Job, this will be empty.
	Identifier string `json:"identifier"`
	State      string `json:"state"`
	Index      int    `json:"index"`
	Status     Status `json:"status"`

	// Workflow is the name of the workflow being executed.
	Workflow string `json:"workflow"`

	// Scenarios are copied into each context in case
	// a configuration file changes that could corrupt
	// in-process flows.
	Scenarios []*Scenario `json:"scenarios"`
}

// Broadcast contains information needed to create
// and broadcast a transaction. Broadcast is returned
// from Job processing only IF a broadcast is required.
type Broadcast struct {
	Network           *types.NetworkIdentifier
	Intent            []*types.Operation
	Metadata          map[string]interface{}
	ConfirmationDepth int64
	DryRun            bool
}
