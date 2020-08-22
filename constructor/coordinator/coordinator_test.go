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
	"testing"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/executor"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/constructor/coordinator"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
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
		[]string{"Transfer"},
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
	)
}

func defaultParser(t *testing.T) *parser.Parser {
	asserter, err := simpleAsserterConfiguration()
	assert.NoError(t, err)

	return parser.New(asserter, nil)
}

func TestProcess_RequestCreate(t *testing.T) {
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	jobStorage := &mocks.JobStorage{}
	helper := &mocks.Helper{}
	p := defaultParser(t)
	workflows := []*executor.Workflow{
		{
			Name:        string(executor.RequestFunds),
			Concurrency: 1,
			Scenarios: []*executor.Scenario{
				{
					Name: "request_funds",
					Actions: []*executor.Action{
						{
							Type:       executor.SetVariable,
							Input:      `{"symbol":"tBTC", "decimals":8}`,
							OutputPath: "currency",
						},
						{ // ensure we have some balance that exists
							Type:       executor.FindBalance,
							Input:      `{"minimum_balance":{"value": "0", "currency": {{currency}}}}`, // nolint
							OutputPath: "random_address",
						},
						{
							Type:  executor.FindBalance,
							Input: `{"address": {{random_address.account.address}}, "wait": true, "minimum_balance":{"value": "100", "currency": {{currency}}}`, // nolint
						},
					},
				},
			},
		},
		{
			Name:        string(executor.CreateAccount),
			Concurrency: 1,
			Scenarios: []*executor.Scenario{
				{
					Name: "create_account",
					Actions: []*executor.Action{
						{
							Type:       executor.SetVariable,
							Input:      `{"network":"Testnet3", "blockchain":"Bitcoin"}`,
							OutputPath: "network",
						},
						{
							Type:       executor.GenerateKey,
							Input:      `{"curve_type": "secp256k1"}`,
							OutputPath: "key",
						},
						{
							Type:       executor.Derive,
							Input:      `{"network_identifier": {{network}}, "public_key": {{key.public_key}}}`,
							OutputPath: "address",
						},
						{
							Type:  executor.SaveAddress,
							Input: `{"address": {{address.address}}, "keypair": {{key.public_key}}}`,
						},
					},
				},
			},
		},
	}

	c, err := NewCoordinator(
		jobStorage,
		helper,
		p,
		workflows,
	)
	assert.NotNil(t, c)
	assert.NoError(t, err)

	dir, err := utils.CreateTempDir()
	assert.NoError(t, err)

	db, err := storage.NewBadgerStorage(ctx, dir, false)
	assert.NoError(t, err)
	assert.NotNil(t, db)

	// HeadBlockExists is false first
	helper.On("HeadBlockExists", ctx).Return(false).Once()
	helper.On("HeadBlockExists", ctx).Return(true).Once()

	// Determine should request_funds
	jobStorage.On("Ready", ctx).Return([]*executor.Job{}, nil).Once()
	helper.On("AllBroadcasts", ctx).Return([]*storage.Broadcast{}, nil).Once()
	helper.On("AllAddresses", ctx).Return([]string{}, nil).Once()
	jobStorage.On("Processing", ctx, "request_funds").Return(0, nil).Once()

	// Determine need address to request_funds
	helper.On("HeadBlockExists", ctx).Return(true).Once()
	jobStorage.On("Ready", ctx).Return([]*executor.Job{}, nil).Once()
	helper.On("AllBroadcasts", ctx).Return([]*storage.Broadcast{}, nil).Once()
	helper.On("AllAddresses", ctx).Return([]string{}, nil).Once()
	jobStorage.On("Processing", ctx, "create_account").Return(0, nil).Once()

	// Perform create_account
	helper.On(
		"Derive",
		ctx,
		&types.NetworkIdentifier{
			Blockchain: "Bitcoin",
			Network:    "Testnet3",
		},
		mock.Anything,
		(map[string]interface{})(nil),
	).Return("address1", nil, nil).Once()
	helper.On(
		"StoreKey",
		ctx,
		"address1",
		mock.Anything,
	).Return(nil).Once()
	dbTx := db.NewDatabaseTransaction(ctx, true)
	helper.On("DatabaseTransaction", ctx).Return(dbTx).Once()
	jobStorage.On("Update", ctx, dbTx, mock.Anything).Return("job1", nil).Once()
	helper.On("BroadcastAll", ctx).Return(nil)

	// Attempt to request funds on "address1"

	fundsProvided := make(chan struct{})
	processCanceled := make(chan struct{})
	go func() {
		err := c.Process(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
		close(processCanceled)
	}()

	<-fundsProvided
	time.Sleep(5 * time.Second)
	cancel()
	<-processCanceled

	jobStorage.AssertExpectations(t)
	helper.AssertExpectations(t)
}
