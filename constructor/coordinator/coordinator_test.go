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
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/keys"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

const (
	unsignedTx    = "unsigned transaction"
	networkTx     = "network transaction"
	jobIdentifier = "job"
)

var (
	broadcastMetadata = map[string]interface{}{
		"test": "works",
	}
)

func simpleAsserterConfiguration() (*asserter.Asserter, error) {
	return asserter.NewClientWithOptions(
		&types.NetworkIdentifier{
			Blockchain: "bitcoin",
			Network:    "mainnet",
		},
		&types.BlockIdentifier{
			Hash:  "block 0",
			Index: 0,
		},
		[]string{"Vin", "Vout"},
		[]*types.OperationStatus{
			{
				Status:     "success",
				Successful: true,
			},
			{
				Status:     "failure",
				Successful: false,
			},
		},
		[]*types.Error{},
		nil,
		&asserter.Validations{
			Enabled: false,
		},
	)
}

func defaultParser(t *testing.T) *parser.Parser {
	asserter, err := simpleAsserterConfiguration()
	assert.NoError(t, err)

	return parser.New(asserter, nil, nil)
}

func TestProcess(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "find_address",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit":1}`, // nolint
							OutputPath: "random_address",
						},
					},
				},
				{
					Name: "request",
					Actions: []*job.Action{
						{
							Type:       job.FindBalance,
							Input:      `{"account_identifier": {{random_address.account_identifier}}, "minimum_balance":{"value": "100", "currency": {{currency}}}}`, // nolint
							OutputPath: "loaded_address",
						},
					},
				},
			},
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "create_account",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "network",
						},
						{
							Type:       job.GenerateKey,
							Input:      `{"curve_type": "secp256k1"}`,
							OutputPath: "key",
						},
						{
							Type:       job.Derive,
							Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
							OutputPath: "account",
						},
						{
							Type:  job.SaveAccount,
							Input: `{"account_identifier": {{account.account_identifier}}, "keypair": {{key.public_key}}}`,
						},
					},
				},
			},
		},
		{
			Name:        "transfer",
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "100", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "sender",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value": "0", "right_value":{{sender.balance.value}}}`,
							OutputPath: "sender_amount",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"not_account_identifier":[{{sender.account_identifier}}], "minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "recipient",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value":{{sender.balance.value}}, "right_value":"10"}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"test": "works"}`,
							OutputPath: "transfer.preprocess_metadata",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{{sender.account_identifier}},"amount":{"value":{{sender_amount}},"currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{{recipient.account_identifier}},"amount":{"value":{{recipient_amount}},"currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer.operations",
						},
					},
				},
				{
					Name: "print_transaction",
					Actions: []*job.Action{
						{
							Type:  job.PrintMessage,
							Input: `{{transfer.transaction}}`,
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NotNil(t, c.requestFundsWorkflow)
	assert.NotNil(t, c.createAccountWorkflow)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// HeadBlockExists is false first
	helper.On("HeadBlockExists", ctx).Return(false).Once()
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Attempt to transfer
	// We use a "read" database transaction in this test because we mock
	// all responses from the database and "write" transactions require a
	// lock. While it would be possible to orchestrate these locks in this
	// test, it is simpler to just use a "read" transaction.
	dbTxFail := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail).Once()
	jobStorage.On("Ready", ctx, dbTxFail).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTxFail, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{}, nil).Once()

	// Start processor
	go func() {
		err := c.Process(ctx)
		fmt.Println(err)
		assert.True(t, errors.Is(err, context.Canceled))
		close(processCanceled)
	}()

	// Determine account must be created
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	dbTx := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx).Once()
	jobStorage.On("Ready", ctx, dbTx).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx, "create_account").Return([]*job.Job{}, nil).Once()
	helper.On(
		"Derive",
		ctx,
		&types.NetworkIdentifier{
			Blockchain: "Bitcoin",
			Network:    "Testnet3",
		},
		mock.Anything,
		(map[string]interface{})(nil),
	).Return(
		&types.AccountIdentifier{Address: "address1"},
		nil,
		nil,
	).Once()
	helper.On(
		"StoreKey",
		ctx,
		dbTx,
		&types.AccountIdentifier{Address: "address1"},
		mock.Anything,
	).Return(nil).Once()
	jobStorage.On("Update", ctx, dbTx, mock.Anything).Return("job1", nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Attempt to run transfer again (but determine funds are needed)
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTxFail2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail2).Once()
	jobStorage.On("Ready", ctx, dbTxFail2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTxFail2, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTxFail2).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxFail2).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxFail2,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()

	// Attempt funds request
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	dbTx2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx2).Once()
	jobStorage.On("Ready", ctx, dbTx2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx2, "request_funds").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTx2).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx2).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx2,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	var jobExtra job.Job
	jobStorage.On(
		"Update",
		ctx,
		dbTx2,
		mock.Anything,
	).Return(
		"jobExtra",
		nil,
	).Run(
		func(args mock.Arguments) {
			jobExtra = *args.Get(2).(*job.Job)
			jobExtra.Identifier = "jobExtra"
		},
	).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Load funds
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTxExtra := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxExtra).Once()
	jobStorage.On("Ready", ctx, dbTxExtra).Return([]*job.Job{&jobExtra}, nil).Once()
	helper.On("AllAccounts", ctx, dbTxExtra).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxExtra).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxExtra,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()

	// Wait until we get here to continue setting up mocks
	jobStorage.On("Update", ctx, dbTxExtra, mock.Anything).Return("jobExtra", nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Attempt to transfer again
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTxFail3 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail3).Once()
	jobStorage.On("Ready", ctx, dbTxFail3).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTxFail3, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTxFail3).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxFail3).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxFail3,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTxFail3).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxFail3).Return([]*types.AccountIdentifier{}, nil).Once()

	// Attempt to create recipient
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx3 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx3).Once()
	jobStorage.On("Ready", ctx, dbTx3).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx3).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx3, "create_account").Return([]*job.Job{}, nil).Once()
	helper.On(
		"Derive",
		ctx,
		&types.NetworkIdentifier{
			Blockchain: "Bitcoin",
			Network:    "Testnet3",
		},
		mock.Anything,
		(map[string]interface{})(nil),
	).Return(
		&types.AccountIdentifier{Address: "address2"},
		nil,
		nil,
	).Once()
	helper.On(
		"StoreKey",
		ctx,
		dbTx3,
		&types.AccountIdentifier{Address: "address2"},
		mock.Anything,
	).Return(nil).Once()
	jobStorage.On("Update", ctx, dbTx3, mock.Anything).Return("job3", nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Attempt to create transfer
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx4 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx4).Once()
	jobStorage.On("Ready", ctx, dbTx4).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx4, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTx4).Return([]*types.AccountIdentifier{
		{Address: "address1"},
		{Address: "address2"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx4).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx4,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTx4).Return([]*types.AccountIdentifier{
		{Address: "address1"},
		{Address: "address2"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx4).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx4,
		&types.AccountIdentifier{Address: "address2"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	var job4 job.Job
	jobStorage.On(
		"Update",
		ctx,
		dbTx4,
		mock.Anything,
	).Return(
		"job4",
		nil,
	).Run(
		func(args mock.Arguments) {
			job4 = *args.Get(2).(*job.Job)
			job4.Identifier = "job4"
		},
	).Once()

	// Construct Transaction
	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	currency := &types.Currency{
		Symbol:   "tBTC",
		Decimals: 8,
	}
	ops := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "address1",
			},
			Amount: &types.Amount{
				Value:    "-100",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "address2",
			},
			Amount: &types.Amount{
				Value:    "90",
				Currency: currency,
			},
		},
	}
	metadataOptions := map[string]interface{}{
		"metadata": "test",
	}
	helper.On(
		"Preprocess",
		ctx,
		network,
		ops,
		map[string]interface{}{
			"test": "works",
		},
	).Return(metadataOptions, nil, nil).Once()
	fetchedMetadata := map[string]interface{}{
		"tx_meta": "help",
	}
	helper.On(
		"Metadata",
		ctx,
		network,
		metadataOptions,
		[]*types.PublicKey{},
	).Return(fetchedMetadata, nil, nil).Once()

	signingPayloads := []*types.SigningPayload{
		{
			AccountIdentifier: &types.AccountIdentifier{Address: "address1"},
			Bytes:             []byte("blah"),
			SignatureType:     types.Ecdsa,
		},
	}
	helper.On(
		"Payloads",
		ctx,
		network,
		ops,
		fetchedMetadata,
		[]*types.PublicKey{},
	).Return(unsignedTx, signingPayloads, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		false,
		unsignedTx,
	).Return(ops, []*types.AccountIdentifier{}, nil, nil).Once()
	signatures := []*types.Signature{
		{
			SigningPayload: signingPayloads[0],
			PublicKey: &types.PublicKey{
				Bytes:     []byte("pubkey"),
				CurveType: types.Secp256k1,
			},
			SignatureType: types.Ecdsa,
			Bytes:         []byte("signature"),
		},
	}
	helper.On(
		"Sign",
		ctx,
		signingPayloads,
	).Return(signatures, nil).Once()
	helper.On(
		"Combine",
		ctx,
		network,
		unsignedTx,
		signatures,
	).Return(networkTx, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		true,
		networkTx,
	).Return(
		ops,
		[]*types.AccountIdentifier{{Address: "address1"}},
		nil,
		nil,
	).Once()
	txIdentifier := &types.TransactionIdentifier{Hash: "transaction hash"}
	helper.On(
		"Hash",
		ctx,
		network,
		networkTx,
	).Return(txIdentifier, nil).Once()
	helper.On(
		"Broadcast",
		ctx,
		dbTx4,
		"job4",
		network,
		ops,
		txIdentifier,
		networkTx,
		int64(1),
		broadcastMetadata,
	).Return(nil).Once()
	handler.On("TransactionCreated", ctx, "job4", txIdentifier).Return(nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Wait for transfer to complete
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx5 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx5).Once()
	jobStorage.On("Ready", ctx, dbTx5).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx5, "transfer").Return([]*job.Job{&job4}, nil).Once()

	markConfirmed := make(chan struct{})
	jobStorage.On("Broadcasting", ctx, dbTx5).Return([]*job.Job{
		&job4,
	}, nil).Run(func(args mock.Arguments) {
		close(markConfirmed)
	}).Once()

	onchainOps := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Status: types.String("success"),
			Type:   "Vin",
			Account: &types.AccountIdentifier{
				Address: "address1",
			},
			Amount: &types.Amount{
				Value:    "-100",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Status: types.String("success"),
			Type:   "Vout",
			Account: &types.AccountIdentifier{
				Address: "address2",
			},
			Amount: &types.Amount{
				Value:    "90",
				Currency: currency,
			},
		},
	}
	go func() {
		<-markConfirmed
		dbTx6 := db.ReadTransaction(ctx)
		jobStorage.On("Get", ctx, dbTx6, "job4").Return(&job4, nil).Once()
		jobStorage.On(
			"Update",
			ctx,
			dbTx6,
			mock.Anything,
		).Run(func(args mock.Arguments) {
			job4 = *args.Get(2).(*job.Job)
			job4.Identifier = "job4"
		}).Return(
			"job4",
			nil,
		)
		tx := &types.Transaction{
			TransactionIdentifier: txIdentifier,
			Operations:            onchainOps,
		}

		// Process second step of job4
		err = c.BroadcastComplete(ctx, dbTx6, "job4", tx)
		assert.NoError(t, err)
	}()

	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx7 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx7).Once()
	jobStorage.On("Ready", ctx, dbTx7).Return([]*job.Job{&job4}, nil).Once()
	jobStorage.On(
		"Update",
		ctx,
		dbTx7,
		mock.Anything,
	).Return(
		"job4",
		nil,
	)
	helper.On("BroadcastAll", ctx).Return(nil).Run(func(args mock.Arguments) {
		fmt.Printf("canceling %+v\n", args)
		cancel()
	}).Once()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestProcess_Failed(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
		},
		{
			Name:        "transfer",
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "100", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "sender",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value": "0", "right_value":{{sender.balance.value}}}`,
							OutputPath: "sender_amount",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"not_account_identifier":[{{sender.account_identifier}}], "minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "recipient",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value":{{sender.balance.value}}, "right_value":"10"}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"test": "works"}`,
							OutputPath: "transfer.preprocess_metadata",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{{sender.account_identifier}},"amount":{"value":{{sender_amount}},"currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{{recipient.account_identifier}},"amount":{"value":{{recipient_amount}},"currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer.operations",
						},
					},
				},
				{
					Name: "print_transaction",
					Actions: []*job.Action{
						{
							Type:  job.PrintMessage,
							Input: `{{transfer.transaction}}`,
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// HeadBlockExists is false first
	helper.On("HeadBlockExists", ctx).Return(false).Once()

	// Attempt to create transfer
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx).Once()
	jobStorage.On("Ready", ctx, dbTx).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTx).Return([]*types.AccountIdentifier{
		{Address: "address1"},
		{Address: "address2"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTx).Return([]*types.AccountIdentifier{
		{Address: "address1"},
		{Address: "address2"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx,
		&types.AccountIdentifier{Address: "address2"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	var j job.Job
	jobStorage.On(
		"Update",
		ctx,
		dbTx,
		mock.Anything,
	).Return(
		jobIdentifier,
		nil,
	).Run(
		func(args mock.Arguments) {
			j = *args.Get(2).(*job.Job)
			j.Identifier = jobIdentifier
		},
	).Once()

	// Construct Transaction
	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	currency := &types.Currency{
		Symbol:   "tBTC",
		Decimals: 8,
	}
	ops := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "address1",
			},
			Amount: &types.Amount{
				Value:    "-100",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "address2",
			},
			Amount: &types.Amount{
				Value:    "90",
				Currency: currency,
			},
		},
	}
	metadataOptions := map[string]interface{}{
		"metadata": "test",
	}
	helper.On(
		"Preprocess",
		ctx,
		network,
		ops,
		map[string]interface{}{
			"test": "works",
		},
	).Return(metadataOptions, []*types.AccountIdentifier{
		{
			Address: "hello",
		},
		{
			Address: "hello2",
		},
	}, nil, nil).Once()
	helper.On(
		"GetKey",
		ctx,
		dbTx,
		&types.AccountIdentifier{Address: "hello"},
	).Return(
		&keys.KeyPair{
			PublicKey: &types.PublicKey{
				Bytes:     []byte("hello"),
				CurveType: types.Secp256k1,
			},
		},
		nil,
	).Once()
	helper.On(
		"GetKey",
		ctx,
		dbTx,
		&types.AccountIdentifier{Address: "hello2"},
	).Return(
		&keys.KeyPair{
			PublicKey: &types.PublicKey{
				Bytes:     []byte("hello2"),
				CurveType: types.Edwards25519,
			},
		},
		nil,
	).Once()

	fetchedMetadata := map[string]interface{}{
		"tx_meta": "help",
	}
	helper.On(
		"Metadata",
		ctx,
		network,
		metadataOptions,
		[]*types.PublicKey{
			{
				Bytes:     []byte("hello"),
				CurveType: types.Secp256k1,
			},
			{
				Bytes:     []byte("hello2"),
				CurveType: types.Edwards25519,
			},
		},
	).Return(fetchedMetadata, nil, nil).Once()

	signingPayloads := []*types.SigningPayload{
		{
			AccountIdentifier: &types.AccountIdentifier{Address: "address1"},
			Bytes:             []byte("blah"),
			SignatureType:     types.Ecdsa,
		},
	}
	helper.On(
		"Payloads",
		ctx,
		network,
		ops,
		fetchedMetadata,
		[]*types.PublicKey{
			{
				Bytes:     []byte("hello"),
				CurveType: types.Secp256k1,
			},
			{
				Bytes:     []byte("hello2"),
				CurveType: types.Edwards25519,
			},
		},
	).Return(unsignedTx, signingPayloads, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		false,
		unsignedTx,
	).Return(ops, []*types.AccountIdentifier{}, nil, nil).Once()
	signatures := []*types.Signature{
		{
			SigningPayload: signingPayloads[0],
			PublicKey: &types.PublicKey{
				Bytes:     []byte("pubkey"),
				CurveType: types.Secp256k1,
			},
			SignatureType: types.Ecdsa,
			Bytes:         []byte("signature"),
		},
	}
	helper.On(
		"Sign",
		ctx,
		signingPayloads,
	).Return(signatures, nil).Once()
	helper.On(
		"Combine",
		ctx,
		network,
		unsignedTx,
		signatures,
	).Return(networkTx, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		true,
		networkTx,
	).Return(ops, []*types.AccountIdentifier{
		{Address: "address1"},
	}, nil, nil).Once()
	txIdentifier := &types.TransactionIdentifier{Hash: "transaction hash"}
	helper.On(
		"Hash",
		ctx,
		network,
		networkTx,
	).Return(txIdentifier, nil).Once()
	helper.On(
		"Broadcast",
		ctx,
		dbTx,
		jobIdentifier,
		network,
		ops,
		txIdentifier,
		networkTx,
		int64(1),
		broadcastMetadata,
	).Return(nil).Once()
	handler.On("TransactionCreated", ctx, jobIdentifier, txIdentifier).Return(nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Start processor
	go func() {
		err := c.Process(ctx)
		fmt.Println(err)
		assert.True(t, errors.Is(err, context.Canceled))
		close(processCanceled)
	}()

	// Wait for transfer to complete
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx2).Once()
	jobStorage.On("Ready", ctx, dbTx2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx2, "transfer").Return([]*job.Job{&j}, nil).Once()

	markConfirmed := make(chan struct{})
	jobStorage.On("Broadcasting", ctx, dbTx2).Return([]*job.Job{
		&j,
	}, nil).Run(func(args mock.Arguments) {
		close(markConfirmed)
	}).Once()

	go func() {
		<-markConfirmed
		dbTx3 := db.ReadTransaction(ctx)
		jobStorage.On("Get", ctx, dbTx3, jobIdentifier).Return(&j, nil).Once()
		jobStorage.On(
			"Update",
			ctx,
			dbTx3,
			mock.Anything,
		).Run(func(args mock.Arguments) {
			j = *args.Get(2).(*job.Job)
			j.Identifier = jobIdentifier
			cancel()
		}).Return(
			jobIdentifier,
			nil,
		)

		// Process second step of job
		err = c.BroadcastComplete(ctx, dbTx3, jobIdentifier, nil)
		assert.NoError(t, err)
	}()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestInitialization_NoWorkflows(t *testing.T) {
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.Nil(t, c)
	assert.True(t, errors.Is(err, ErrNoWorkflows))

	helper.AssertExpectations(t)
	handler.AssertExpectations(t)
}

func TestInitialization_OnlyCreateAccountWorkflows(t *testing.T) {
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)
	helper.AssertExpectations(t)
	handler.AssertExpectations(t)
}

func TestInitialization_InvalidConcurrency(t *testing.T) {
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
		},
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
		},
		{
			Name:        "transfer",
			Concurrency: 0,
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.Nil(t, c)
	assert.True(t, errors.Is(err, ErrInvalidConcurrency))
	helper.AssertExpectations(t)
	handler.AssertExpectations(t)
}

func TestInitialization_OnlyRequestFundsWorkflows(t *testing.T) {
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)

	assert.NotNil(t, c)
	assert.NoError(t, err)
	helper.AssertExpectations(t)
	handler.AssertExpectations(t)
}

func TestProcess_DryRun(t *testing.T) {
	ctx := context.Background()

	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
		},
		{
			Name:        "transfer",
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer_1",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer_1.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer_1.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{"address":"sender"},"amount":{"value":"-10","currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{"address":"recipient"},"amount":{"value":"5","currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer_1.operations",
						},
						{
							Type:       job.SetVariable,
							Input:      `"true"`,
							OutputPath: "transfer_1.dry_run",
						},
					},
				},
				{
					Name: "transfer_2",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer_2.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer_2.confirmation_depth",
						},
						{
							Type:       job.FindCurrencyAmount,
							Input:      `{"currency":{{currency}},"amounts":{{transfer_1.suggested_fee}}}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{"address":"sender"},"amount":{"value":"-10","currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{"address":"recipient"},"amount":{{recipient_amount}}}]`, // nolint
							OutputPath: "transfer_2.operations",
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Attempt to transfer
	dbTx := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx).Once()
	jobStorage.On("Ready", ctx, dbTx).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTx, "transfer").Return([]*job.Job{}, nil).Once()
	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	currency := &types.Currency{
		Symbol:   "tBTC",
		Decimals: 8,
	}
	dryRunOps := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "sender",
			},
			Amount: &types.Amount{
				Value:    "-10",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "recipient",
			},
			Amount: &types.Amount{
				Value:    "5",
				Currency: currency,
			},
		},
	}
	broadcastOps := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "sender",
			},
			Amount: &types.Amount{
				Value:    "-10",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "recipient",
			},
			Amount: &types.Amount{
				Value:    "7",
				Currency: currency,
			},
		},
	}
	metadataOptions := map[string]interface{}{
		"metadata": "test",
	}
	helper.On(
		"Preprocess",
		ctx,
		network,
		dryRunOps,
		(map[string]interface{})(nil),
	).Return(metadataOptions, nil, nil).Once()
	fetchedMetadata := map[string]interface{}{
		"tx_meta": "help",
	}
	helper.On(
		"Metadata",
		ctx,
		network,
		metadataOptions,
		[]*types.PublicKey{},
	).Return(fetchedMetadata, []*types.Amount{
		{
			Value:    "7",
			Currency: currency,
		},
	}, nil).Once()
	var j job.Job
	jobStorage.On("Update", ctx, dbTx, mock.Anything).Run(func(args mock.Arguments) {
		j = *args.Get(2).(*job.Job)
	}).Return("job1", nil).Once()
	jobStorage.On("Update", ctx, dbTx, mock.Anything).Run(func(args mock.Arguments) {
		j = *args.Get(2).(*job.Job)
	}).Return("job1", nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Start processor
	go func() {
		err := c.Process(ctx)
		assert.Contains(t, err.Error(), "fake failure")
		close(processCanceled)
	}()

	// Process second scenario
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx2).Once()
	jobStorage.On("Ready", ctx, dbTx2).Return([]*job.Job{&j}, nil).Once()
	jobStorage.On("Update", ctx, dbTx2, mock.Anything).Run(func(args mock.Arguments) {
		j = *args.Get(2).(*job.Job)
	}).Return("job1", nil).Once()
	helper.On(
		"Preprocess",
		ctx,
		network,
		broadcastOps,
		(map[string]interface{})(nil),
	).Return(nil, nil, errors.New("fake failure")).Once()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestReturnFunds_NoBalance(t *testing.T) {
	ctx := context.Background()
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "find_address",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit":1}`, // nolint
							OutputPath: "random_address",
						},
					},
				},
				{
					Name: "request",
					Actions: []*job.Action{
						{
							Type:       job.FindBalance,
							Input:      `{"account_identifier": {{random_address.account_identifier}}, "minimum_balance":{"value": "100", "currency": {{currency}}}}`, // nolint
							OutputPath: "loaded_address",
						},
					},
				},
			},
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "create_account",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "network",
						},
						{
							Type:       job.GenerateKey,
							Input:      `{"curve_type": "secp256k1"}`,
							OutputPath: "key",
						},
						{
							Type:       job.Derive,
							Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
							OutputPath: "account",
						},
						{
							Type:  job.SaveAccount,
							Input: `{"account_identifier": {{account.account_identifier}}, "keypair": {{key.public_key}}}`,
						},
					},
				},
			},
		},
		{
			Name:        string(job.ReturnFunds),
			Concurrency: 2,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "100", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "sender",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value": "0", "right_value":{{sender.balance.value}}}`,
							OutputPath: "sender_amount",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"not_account_identifier":[{{sender.account_identifier}}], "minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "recipient",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value":{{sender.balance.value}}, "right_value":"10"}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"test": "works"}`,
							OutputPath: "transfer.preprocess_metadata",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{{sender.account_identifier}},"amount":{"value":{{sender_amount}},"currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{{recipient.account_identifier}},"amount":{"value":{{recipient_amount}},"currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer.operations",
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// HeadBlockExists is false first
	helper.On("HeadBlockExists", ctx).Return(false).Once()
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Attempt to transfer
	dbTxFail := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail).Once()
	jobStorage.On("Ready", ctx, dbTxFail).Return([]*job.Job{}, nil).Once()
	jobStorage.On(
		"Processing",
		ctx,
		dbTxFail,
		string(job.ReturnFunds),
	).Return(
		[]*job.Job{},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{
		{Address: "address1"},
		{Address: "address2"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxFail,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxFail,
		&types.AccountIdentifier{Address: "address2"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "50",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()

	// Start processor
	go func() {
		err := c.ReturnFunds(ctx)
		assert.NoError(t, err)
		close(processCanceled)
	}()

	// Will exit this round because we've tried all workflows.
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx2).Once()
	jobStorage.On("Ready", ctx, dbTx2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx2).Return([]*job.Job{}, nil).Once()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestReturnFunds_NoWorkflow(t *testing.T) {
	ctx := context.Background()
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "find_address",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit":1}`, // nolint
							OutputPath: "random_address",
						},
					},
				},
				{
					Name: "request",
					Actions: []*job.Action{
						{
							Type:       job.FindBalance,
							Input:      `{"account_identifier": {{random_address.account_identifier}}, "minimum_balance":{"value": "100", "currency": {{currency}}}}`, // nolint
							OutputPath: "loaded_address",
						},
					},
				},
			},
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "create_account",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "network",
						},
						{
							Type:       job.GenerateKey,
							Input:      `{"curve_type": "secp256k1"}`,
							OutputPath: "key",
						},
						{
							Type:       job.Derive,
							Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
							OutputPath: "account",
						},
						{
							Type:  job.SaveAccount,
							Input: `{"account_identifier": {{account.account_identifier}}, "keypair": {{key.public_key}}}`,
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// We should exit immediately, so we don't need to prepare
	// our helper or handler.
	go func() {
		err := c.ReturnFunds(ctx)
		assert.NoError(t, err)
		close(processCanceled)
	}()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestReturnFunds(t *testing.T) {
	ctx := context.Background()
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        string(job.RequestFunds),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "find_address",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit":1}`, // nolint
							OutputPath: "random_address",
						},
					},
				},
				{
					Name: "request",
					Actions: []*job.Action{
						{
							Type:       job.FindBalance,
							Input:      `{"account_identifier": {{random_address.account_identifier}}, "minimum_balance":{"value": "100", "currency": {{currency}}}}`, // nolint
							OutputPath: "loaded_address",
						},
					},
				},
			},
		},
		{
			Name:        string(job.CreateAccount),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "create_account",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "network",
						},
						{
							Type:       job.GenerateKey,
							Input:      `{"curve_type": "secp256k1"}`,
							OutputPath: "key",
						},
						{
							Type:       job.Derive,
							Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
							OutputPath: "account",
						},
						{
							Type:  job.SaveAccount,
							Input: `{"account_identifier": {{account.account_identifier}}, "keypair": {{key.public_key}}}`,
						},
					},
				},
			},
		},
		{
			Name:        string(job.ReturnFunds),
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "100", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "sender",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value": "0", "right_value":{{sender.balance.value}}}`,
							OutputPath: "sender_amount",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value":{{sender.balance.value}}, "right_value":"10"}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"test": "works"}`,
							OutputPath: "transfer.preprocess_metadata",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{{sender.account_identifier}},"amount":{"value":{{sender_amount}},"currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{"address":"address2"},"amount":{"value":{{recipient_amount}},"currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer.operations",
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// HeadBlockExists is false first
	helper.On("HeadBlockExists", ctx).Return(false).Once()
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Attempt to transfer
	dbTxFail := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail).Once()
	jobStorage.On("Ready", ctx, dbTxFail).Return([]*job.Job{}, nil).Once()
	jobStorage.On(
		"Processing",
		ctx,
		dbTxFail,
		string(job.ReturnFunds),
	).Return(
		[]*job.Job{},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTxFail,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "100",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()

	var j job.Job
	jobStorage.On(
		"Update",
		ctx,
		dbTxFail,
		mock.Anything,
	).Return(
		jobIdentifier,
		nil,
	).Run(
		func(args mock.Arguments) {
			j = *args.Get(2).(*job.Job)
			j.Identifier = jobIdentifier
		},
	).Once()

	// Construct Transaction
	network := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Testnet3",
	}
	currency := &types.Currency{
		Symbol:   "tBTC",
		Decimals: 8,
	}
	ops := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Type: "Vin",
			Account: &types.AccountIdentifier{
				Address: "address1",
			},
			Amount: &types.Amount{
				Value:    "-100",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Type: "Vout",
			Account: &types.AccountIdentifier{
				Address: "address2",
			},
			Amount: &types.Amount{
				Value:    "90",
				Currency: currency,
			},
		},
	}
	metadataOptions := map[string]interface{}{
		"metadata": "test",
	}
	helper.On(
		"Preprocess",
		ctx,
		network,
		ops,
		map[string]interface{}{
			"test": "works",
		},
	).Return(metadataOptions, nil, nil).Once()
	fetchedMetadata := map[string]interface{}{
		"tx_meta": "help",
	}
	helper.On(
		"Metadata",
		ctx,
		network,
		metadataOptions,
		[]*types.PublicKey{},
	).Return(fetchedMetadata, nil, nil).Once()

	signingPayloads := []*types.SigningPayload{
		{
			AccountIdentifier: &types.AccountIdentifier{Address: "address1"},
			Bytes:             []byte("blah"),
			SignatureType:     types.Ecdsa,
		},
	}
	helper.On(
		"Payloads",
		ctx,
		network,
		ops,
		fetchedMetadata,
		[]*types.PublicKey{},
	).Return(unsignedTx, signingPayloads, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		false,
		unsignedTx,
	).Return(ops, []*types.AccountIdentifier{}, nil, nil).Once()
	signatures := []*types.Signature{
		{
			SigningPayload: signingPayloads[0],
			PublicKey: &types.PublicKey{
				Bytes:     []byte("pubkey"),
				CurveType: types.Secp256k1,
			},
			SignatureType: types.Ecdsa,
			Bytes:         []byte("signature"),
		},
	}
	helper.On(
		"Sign",
		ctx,
		signingPayloads,
	).Return(signatures, nil).Once()
	helper.On(
		"Combine",
		ctx,
		network,
		unsignedTx,
		signatures,
	).Return(networkTx, nil).Once()
	helper.On(
		"Parse",
		ctx,
		network,
		true,
		networkTx,
	).Return(
		ops,
		[]*types.AccountIdentifier{{Address: "address1"}},
		nil,
		nil,
	).Once()
	txIdentifier := &types.TransactionIdentifier{Hash: "transaction hash"}
	helper.On(
		"Hash",
		ctx,
		network,
		networkTx,
	).Return(txIdentifier, nil).Once()
	helper.On(
		"Broadcast",
		ctx,
		dbTxFail,
		jobIdentifier,
		network,
		ops,
		txIdentifier,
		networkTx,
		int64(1),
		broadcastMetadata,
	).Return(nil).Once()
	handler.On("TransactionCreated", ctx, jobIdentifier, txIdentifier).Return(nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil).Once()

	// Start processor
	go func() {
		err := c.ReturnFunds(ctx)
		assert.NoError(t, err)
		close(processCanceled)
	}()

	// Wait for transfer to complete
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx).Once()
	jobStorage.On("Ready", ctx, dbTx).Return([]*job.Job{}, nil).Once()
	jobStorage.On(
		"Processing",
		ctx,
		dbTx,
		string(job.ReturnFunds),
	).Return(
		[]*job.Job{&j},
		nil,
	).Once()

	markConfirmed := make(chan struct{})
	jobStorage.On("Broadcasting", ctx, dbTx).Return([]*job.Job{
		&j,
	}, nil).Run(func(args mock.Arguments) {
		close(markConfirmed)
	}).Once()

	onchainOps := []*types.Operation{
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 0,
			},
			Status: types.String("success"),
			Type:   "Vin",
			Account: &types.AccountIdentifier{
				Address: "address1",
			},
			Amount: &types.Amount{
				Value:    "-100",
				Currency: currency,
			},
		},
		{
			OperationIdentifier: &types.OperationIdentifier{
				Index: 1,
			},
			Status: types.String("success"),
			Type:   "Vout",
			Account: &types.AccountIdentifier{
				Address: "address2",
			},
			Amount: &types.Amount{
				Value:    "90",
				Currency: currency,
			},
		},
	}
	go func() {
		<-markConfirmed
		dbTx2 := db.ReadTransaction(ctx)
		jobStorage.On("Get", ctx, dbTx2, jobIdentifier).Return(&j, nil).Once()
		jobStorage.On(
			"Update",
			ctx,
			dbTx2,
			mock.Anything,
		).Run(func(args mock.Arguments) {
			j = *args.Get(2).(*job.Job)
			j.Identifier = jobIdentifier
		}).Return(
			jobIdentifier,
			nil,
		)
		tx := &types.Transaction{
			TransactionIdentifier: txIdentifier,
			Operations:            onchainOps,
		}

		err = c.BroadcastComplete(ctx, dbTx2, jobIdentifier, tx)
		assert.NoError(t, err)
	}()

	// No balance remaining
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx3 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx3).Once()
	jobStorage.On("Ready", ctx, dbTx3).Return([]*job.Job{}, nil).Once()
	jobStorage.On(
		"Processing",
		ctx,
		dbTx3,
		string(job.ReturnFunds),
	).Return(
		[]*job.Job{},
		nil,
	).Once()
	helper.On("AllAccounts", ctx, dbTx3).Return([]*types.AccountIdentifier{
		{Address: "address1"},
	}, nil).Once()
	helper.On("LockedAccounts", ctx, dbTx3).Return([]*types.AccountIdentifier{}, nil).Once()
	helper.On(
		"Balance",
		ctx,
		dbTx3,
		&types.AccountIdentifier{Address: "address1"},
		&types.Currency{
			Symbol:   "tBTC",
			Decimals: 8,
		},
	).Return(
		&types.Amount{
			Value: "0",
			Currency: &types.Currency{
				Symbol:   "tBTC",
				Decimals: 8,
			},
		},
		nil,
	).Once()

	// Will exit this round because we've tried all workflows.
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx4 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx4).Once()
	jobStorage.On("Ready", ctx, dbTx4).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx4).Return([]*job.Job{}, nil).Once()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}

func TestNoReservedWorkflows(t *testing.T) {
	ctx := context.Background()
	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	handler := &mocks.Handler{}
	p := defaultParser(t)
	workflows := []*job.Workflow{
		{
			Name:        "transfer",
			Concurrency: 1,
			Scenarios: []*job.Scenario{
				{
					Name: "transfer",
					Actions: []*job.Action{
						{
							Type:       job.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "transfer.network",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"minimum_balance":{"value": "100", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "sender",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value": "0", "right_value":{{sender.balance.value}}}`,
							OutputPath: "sender_amount",
						},
						{
							Type:       job.FindBalance,
							Input:      `{"not_account_identifier":[{{sender.account_identifier}}], "minimum_balance":{"value": "0", "currency": {{currency}}}, "create_limit": 100}`, // nolint
							OutputPath: "recipient",
						},
						{
							Type:       job.Math,
							Input:      `{"operation":"subtraction", "left_value":{{sender.balance.value}}, "right_value":"10"}`,
							OutputPath: "recipient_amount",
						},
						{
							Type:       job.SetVariable,
							Input:      `"1"`,
							OutputPath: "transfer.confirmation_depth",
						},
						{
							Type:       job.SetVariable,
							Input:      `{"test": "works"}`,
							OutputPath: "transfer.preprocess_metadata",
						},
						{
							Type:       job.SetVariable,
							Input:      `[{"operation_identifier":{"index":0},"type":"Vin","account":{{sender.account_identifier}},"amount":{"value":{{sender_amount}},"currency":{{currency}}}},{"operation_identifier":{"index":1},"type":"Vout","account":{{recipient.account_identifier}},"amount":{"value":{{recipient_amount}},"currency":{{currency}}}}]`, // nolint
							OutputPath: "transfer.operations",
						},
					},
				},
				{
					Name: "print_transaction",
					Actions: []*job.Action{
						{
							Type:  job.PrintMessage,
							Input: `{{transfer.transaction}}`,
						},
					},
				},
			},
		},
	}

	c, err := New(
		jobStorage,
		helper,
		handler,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	// Create coordination channels
	processCanceled := make(chan struct{})

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := database.NewBadgerDatabase(
		ctx,
		dir,
		database.WithIndexCacheSize(database.TinyIndexCacheSize),
	)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Attempt to transfer
	// We use a "read" database transaction in this test because we mock
	// all responses from the database and "write" transactions require a
	// lock. While it would be possible to orchestrate these locks in this
	// test, it is simpler to just use a "read" transaction.
	dbTxFail := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTxFail).Once()
	jobStorage.On("Ready", ctx, dbTxFail).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Processing", ctx, dbTxFail, "transfer").Return([]*job.Job{}, nil).Once()
	helper.On("AllAccounts", ctx, dbTxFail).Return([]*types.AccountIdentifier{}, nil).Once()

	// Start processor
	go func() {
		err := c.Process(ctx)
		assert.True(t, errors.Is(err, ErrStalled))
		close(processCanceled)
	}()

	helper.On("HeadBlockExists", ctx).Return(true).Once()
	dbTx2 := db.ReadTransaction(ctx)
	helper.On("DatabaseTransaction", ctx).Return(dbTx2).Once()
	jobStorage.On("Ready", ctx, dbTx2).Return([]*job.Job{}, nil).Once()
	jobStorage.On("Broadcasting", ctx, dbTx2).Return([]*job.Job{}, nil).Once()

	<-processCanceled
	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}
