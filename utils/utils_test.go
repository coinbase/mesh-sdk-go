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

package utils

import (
	"context"
	"errors"
	"fmt"
	"math/big"
	"os"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/fetcher"
	mocks "github.com/coinbase/rosetta-sdk-go/mocks/utils"
	storageErrors "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestCreateAndRemoveTempDir(t *testing.T) {
	dir, err := CreateTempDir()
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	assert.NoError(t, err)

	customPath := path.Join(dir, "test", "test2")
	_, err = os.Stat(customPath)
	assert.True(t, os.IsNotExist(err))

	assert.NoError(t, EnsurePathExists(customPath))
	_, err = os.Stat(path.Join(dir, "test"))
	assert.NoError(t, err)

	_, err = os.Stat(customPath)
	assert.NoError(t, err)

	// Write to path
	curr := &types.Currency{
		Symbol:   "BTC",
		Decimals: 8,
	}

	currPath := path.Join(customPath, "curr.json")
	err = SerializeAndWrite(currPath, curr)
	assert.NoError(t, err)

	_, err = os.Stat(currPath)
	assert.NoError(t, err)

	// Check write equal to read
	var newCurr types.Currency
	err = LoadAndParse(currPath, &newCurr)
	assert.NoError(t, err)
	assert.Equal(t, curr, &newCurr)

	// Test that we error when unknown fields
	var newBlock types.Block
	err = LoadAndParse(currPath, &newBlock)
	assert.Error(t, err)
	assert.Equal(t, types.Block{}, newBlock)

	RemoveTempDir(dir)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestCreateCommandPath(t *testing.T) {
	dir, err := CreateTempDir()
	assert.NoError(t, err)

	_, err = os.Stat(dir)
	assert.NoError(t, err)

	net := &types.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Mainnet",
	}

	dp, err := CreateCommandPath(dir, "test", net)
	assert.NoError(t, err)

	customPath := path.Join(dir, "test", types.Hash(net))
	assert.Equal(t, customPath, dp)
	_, err = os.Stat(customPath)
	assert.NoError(t, err)

	RemoveTempDir(dir)

	_, err = os.Stat(dir)
	assert.True(t, os.IsNotExist(err))
}

func TestContainsString(t *testing.T) {
	var tests = map[string]struct {
		arr []string
		s   string

		contains bool
	}{
		"empty arr": {
			s: "hello",
		},
		"single arr": {
			arr:      []string{"hello"},
			s:        "hello",
			contains: true,
		},
		"single arr no elem": {
			arr: []string{"hello"},
			s:   "test",
		},
		"multiple arr with elem": {
			arr:      []string{"hello", "test"},
			s:        "test",
			contains: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.contains, ContainsString(test.arr, test.s))
		})
	}
}

func TestContainsAccountIdentifier(t *testing.T) {
	var tests = map[string]struct {
		arr []*types.AccountIdentifier
		s   *types.AccountIdentifier

		contains bool
	}{
		"empty arr": {
			s: &types.AccountIdentifier{Address: "hello"},
		},
		"single arr": {
			arr: []*types.AccountIdentifier{
				{Address: "hello"},
			},
			s:        &types.AccountIdentifier{Address: "hello"},
			contains: true,
		},
		"single arr sub account mismatch": {
			arr: []*types.AccountIdentifier{
				{
					Address: "hello",
					SubAccount: &types.SubAccountIdentifier{
						Address: "sub",
					},
				},
			},
			s: &types.AccountIdentifier{Address: "hello"},
		},
		"single arr no elem": {
			arr: []*types.AccountIdentifier{
				{Address: "hello"},
			},
			s: &types.AccountIdentifier{Address: "test"},
		},
		"multiple arr with elem": {
			arr: []*types.AccountIdentifier{
				{Address: "hello"},
				{Address: "test"},
			},
			s:        &types.AccountIdentifier{Address: "test"},
			contains: true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.contains, ContainsAccountIdentifier(test.arr, test.s))
		})
	}
}

func TestBigPow10(t *testing.T) {
	e := int32(12)
	v := big.NewFloat(10)

	for i := int32(0); i < e-1; i++ {
		v = new(big.Float).Mul(v, big.NewFloat(10))
	}

	assert.Equal(t, 0, new(big.Float).Sub(v, BigPow10(e)).Sign())
}

func TestPrettyAmount(t *testing.T) {
	var tests = map[string]struct {
		amount   *big.Int
		currency *types.Currency

		result string
	}{
		"no decimals": {
			amount:   big.NewInt(100),
			currency: &types.Currency{Symbol: "blah", Decimals: 0},
			result:   "100 blah",
		},
		"10 decimal": {
			amount:   big.NewInt(100),
			currency: &types.Currency{Symbol: "other", Decimals: 10},
			result:   "0.0000000100 other",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			assert.Equal(t, test.result, PrettyAmount(test.amount, test.currency))
		})
	}
}

func TestMilliseconds(t *testing.T) {
	assert.True(t, Milliseconds() > asserter.MinUnixEpoch)
	assert.True(t, Milliseconds() < asserter.MaxUnixEpoch)
}

func TestRandomNumber(t *testing.T) {
	t.Run("success", func(t *testing.T) {
		minAmount := big.NewInt(10)
		maxAmount := big.NewInt(13)

		// somewhat crude but its fast (should be infinitely small chance we don't get all possible
		// values in small range)
		for i := 0; i < 10000; i++ {
			result, err := RandomNumber(minAmount, maxAmount)
			assert.NoError(t, err)
			assert.NotEqual(t, -1, new(big.Int).Sub(result, minAmount).Sign())
			assert.Equal(t, 1, new(big.Int).Sub(maxAmount, result).Sign())
		}
	})

	t.Run("failure", func(t *testing.T) {
		result, err := RandomNumber(big.NewInt(0), big.NewInt(-10))
		assert.Nil(t, result)
		assert.Error(t, err)
	})
}

var (
	network = &types.NetworkIdentifier{
		Blockchain: "bitcoin",
		Network:    "mainnet",
	}

	blockIdentifier = &types.BlockIdentifier{
		Hash:  "block",
		Index: 1,
	}

	accountCoin = &types.AccountIdentifier{
		Address: "test",
	}

	currency = &types.Currency{
		Symbol:   "BLAH",
		Decimals: 2,
	}

	amountCoins = &types.Amount{
		Value:    "60",
		Currency: currency,
	}

	accountBalance = &types.AccountIdentifier{
		Address: "test2",
	}

	amountBalance = &types.Amount{
		Value:    "100",
		Currency: currency,
	}

	accBalanceRequest1 = &AccountBalanceRequest{
		Account:  accountCoin,
		Currency: currency,
		Network:  network,
	}

	accBalanceResp1 = &AccountBalance{
		Account: accountCoin,
		Amount:  amountCoins,
		Block:   blockIdentifier,
	}

	accBalanceRequest2 = &AccountBalanceRequest{
		Account:  accountBalance,
		Currency: currency,
		Network:  network,
	}

	accBalanceResp2 = &AccountBalance{
		Account: accountBalance,
		Amount:  amountBalance,
		Block:   blockIdentifier,
	}

	accBalanceRequest3 = &AccountBalanceRequest{
		Account: accountBalance,
		Network: network,
	}

	accBalanceResp3 = &AccountBalance{
		Account: accountBalance,
		Amount:  amountBalance,
		Block:   blockIdentifier,
	}
)

func TestGetAccountBalances(t *testing.T) {
	ctx := context.Background()
	mockHelper := &mocks.FetcherHelper{}

	// Mock fetcher behavior
	mockHelper.On(
		"AccountBalanceRetry",
		ctx,
		network,
		accountCoin,
		(*types.PartialBlockIdentifier)(nil),
		[]*types.Currency{currency},
	).Return(
		blockIdentifier,
		[]*types.Amount{amountCoins},
		nil,
		nil,
	).Once()

	mockHelper.On(
		"AccountBalanceRetry",
		ctx,
		network,
		accountBalance,
		(*types.PartialBlockIdentifier)(nil),
		[]*types.Currency{currency},
	).Return(
		blockIdentifier,
		[]*types.Amount{amountBalance},
		nil,
		nil,
	).Once()

	mockHelper.On(
		"AccountBalanceRetry",
		ctx,
		network,
		accountBalance,
		(*types.PartialBlockIdentifier)(nil),
		([]*types.Currency)(nil),
	).Return(
		blockIdentifier,
		[]*types.Amount{amountBalance},
		nil,
		nil,
	).Once()

	accBalances, err := GetAccountBalances(
		ctx,
		mockHelper,
		[]*AccountBalanceRequest{accBalanceRequest1, accBalanceRequest2, accBalanceRequest3},
	)

	assert.Nil(t, err)
	assert.Equal(t, accBalances[0], accBalanceResp1)
	assert.Equal(t, accBalances[1], accBalanceResp2)
	assert.Equal(t, accBalances[2], accBalanceResp3)

	// Error in fetcher
	mockHelper.On(
		"AccountBalanceRetry",
		ctx,
		network,
		accountBalance,
		(*types.PartialBlockIdentifier)(nil),
		[]*types.Currency{currency},
	).Return(
		nil,
		nil,
		nil,
		&fetcher.Error{
			Err: fmt.Errorf("invalid account balance"),
		},
	).Once()

	accBalances, err = GetAccountBalances(
		ctx,
		mockHelper,
		[]*AccountBalanceRequest{accBalanceRequest2},
	)
	assert.Nil(t, accBalances)
	assert.Error(t, err)
}

func TestCheckNetworkTip(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		helper   *mocks.FetcherHelper
		tipDelay int64

		expectedAtTip      bool
		expectedIdentifier *types.BlockIdentifier
		expectedError      error
	}{
		"at tip": {
			helper: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}

				mockHelper.On(
					"NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil),
				).Return(
					&types.NetworkStatusResponse{
						CurrentBlockTimestamp:  Milliseconds(),
						CurrentBlockIdentifier: blockIdentifier,
					},
					nil,
				).Once()

				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      true,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"not at tip": {
			helper: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}

				mockHelper.On(
					"NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil),
				).Return(
					&types.NetworkStatusResponse{
						CurrentBlockTimestamp:  Milliseconds() - 300*MillisecondsInSecond,
						CurrentBlockIdentifier: blockIdentifier,
					},
					nil,
				).Once()

				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      false,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"synced tip": {
			helper: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}

				mockHelper.On(
					"NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil),
				).Return(
					&types.NetworkStatusResponse{
						CurrentBlockTimestamp:  Milliseconds() - 300*MillisecondsInSecond,
						CurrentBlockIdentifier: blockIdentifier,
						SyncStatus: &types.SyncStatus{
							Synced: types.Bool(true),
						},
					},
					nil,
				).Once()

				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      true,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"not synced tip": {
			helper: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}

				mockHelper.On(
					"NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil),
				).Return(
					&types.NetworkStatusResponse{
						CurrentBlockTimestamp:  Milliseconds() - 300*MillisecondsInSecond,
						CurrentBlockIdentifier: blockIdentifier,
						SyncStatus: &types.SyncStatus{
							Synced: types.Bool(false),
						},
					},
					nil,
				).Once()

				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      false,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"error": {
			helper: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}

				mockHelper.On(
					"NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil),
				).Return(
					nil,
					&fetcher.Error{
						Err: fetcher.ErrRequestFailed,
					},
				).Once()

				return mockHelper
			}(),
			tipDelay:      100,
			expectedAtTip: false,
			expectedError: fetcher.ErrRequestFailed,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			atTip, tipBlock, err := CheckNetworkTip(ctx, network, test.tipDelay, test.helper)
			if test.expectedError != nil {
				assert.False(t, atTip)
				assert.Nil(t, tipBlock)
				assert.True(t, errors.Is(err, test.expectedError))
			} else {
				assert.Equal(t, test.expectedAtTip, atTip)
				assert.Equal(t, test.expectedIdentifier, tipBlock)
				assert.NoError(t, err)
			}
			test.helper.AssertExpectations(t)
		})
	}
}

func TestCheckStorageTip(t *testing.T) {
	ctx := context.Background()

	tests := map[string]struct {
		f        *mocks.FetcherHelper
		b        *mocks.BlockStorageHelper
		tipDelay int64

		expectedAtTip      bool
		expectedIdentifier *types.BlockIdentifier
		expectedError      error
	}{
		"stored block within tip delay": {
			f: &mocks.FetcherHelper{},
			b: func() *mocks.BlockStorageHelper {
				mockHelper := &mocks.BlockStorageHelper{}
				mockHelper.On(
					"GetBlockLazy",
					ctx,
					(*types.PartialBlockIdentifier)(nil),
				).Return(
					&types.BlockResponse{
						Block: &types.Block{
							BlockIdentifier: blockIdentifier,
							Timestamp:       Milliseconds(),
						},
					},
					nil,
				).Once()
				return mockHelper
			}(),
			tipDelay:           1000,
			expectedAtTip:      true,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"stored block not within tip delay, network synced ": {
			f: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}
				mockHelper.On("NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil)).Return(&types.NetworkStatusResponse{
					CurrentBlockTimestamp:  Milliseconds() - 300*MillisecondsInSecond,
					CurrentBlockIdentifier: blockIdentifier,
					SyncStatus: &types.SyncStatus{
						Synced: types.Bool(true),
					},
				}, nil)
				return mockHelper
			}(),
			b: func() *mocks.BlockStorageHelper {
				mockHelper := &mocks.BlockStorageHelper{}
				mockHelper.On(
					"GetBlockLazy",
					ctx,
					(*types.PartialBlockIdentifier)(nil),
				).Return(
					&types.BlockResponse{
						Block: &types.Block{
							BlockIdentifier: blockIdentifier,
							Timestamp:       Milliseconds() - 300*MillisecondsInSecond,
						},
					},
					nil,
				).Once()
				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      true,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"stored block not within tip delay, network not synced": {
			f: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}
				mockHelper.On("NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil)).Return(&types.NetworkStatusResponse{
					CurrentBlockTimestamp:  Milliseconds() - 300*MillisecondsInSecond,
					CurrentBlockIdentifier: blockIdentifier,
					SyncStatus: &types.SyncStatus{
						Synced: types.Bool(false),
					},
				}, nil).Once()
				return mockHelper
			}(),
			b: func() *mocks.BlockStorageHelper {
				mockHelper := &mocks.BlockStorageHelper{}
				mockHelper.On(
					"GetBlockLazy",
					ctx,
					(*types.PartialBlockIdentifier)(nil),
				).Return(
					&types.BlockResponse{
						Block: &types.Block{
							BlockIdentifier: blockIdentifier,
							Timestamp:       Milliseconds() - 300*MillisecondsInSecond,
						},
					},
					nil,
				).Once()
				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      false,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"stored block and network blocks different": {
			f: func() *mocks.FetcherHelper {
				mockHelper := &mocks.FetcherHelper{}
				mockHelper.On("NetworkStatusRetry",
					ctx,
					network,
					map[string]interface{}(nil)).Return(&types.NetworkStatusResponse{
					CurrentBlockTimestamp: Milliseconds() - 300*MillisecondsInSecond,
					CurrentBlockIdentifier: &types.BlockIdentifier{
						Hash:  "block",
						Index: 2,
					},
					SyncStatus: &types.SyncStatus{
						Synced: types.Bool(true),
					},
				}, nil).Once()
				return mockHelper
			}(),
			b: func() *mocks.BlockStorageHelper {
				mockHelper := &mocks.BlockStorageHelper{}
				mockHelper.On(
					"GetBlockLazy",
					ctx,
					(*types.PartialBlockIdentifier)(nil),
				).Return(
					&types.BlockResponse{
						Block: &types.Block{
							BlockIdentifier: blockIdentifier,
							Timestamp:       Milliseconds() - 300*MillisecondsInSecond,
						},
					},
					nil,
				).Once()
				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      false,
			expectedIdentifier: blockIdentifier,
			expectedError:      nil,
		},
		"no blocks in storage": {
			f: &mocks.FetcherHelper{},
			b: func() *mocks.BlockStorageHelper {
				mockHelper := &mocks.BlockStorageHelper{}
				mockHelper.On(
					"GetBlockLazy",
					ctx,
					(*types.PartialBlockIdentifier)(nil),
				).Return(
					nil,
					storageErrors.ErrHeadBlockNotFound,
				).Once()
				return mockHelper
			}(),
			tipDelay:           100,
			expectedAtTip:      false,
			expectedIdentifier: nil,
			expectedError:      nil,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			atTip, tipBlock, err := CheckStorageTip(ctx, network, test.tipDelay, test.f, test.b)
			if test.expectedError != nil {
				assert.False(t, atTip)
				assert.Nil(t, tipBlock)
				assert.True(t, errors.Is(err, test.expectedError))
			} else {
				assert.Equal(t, test.expectedAtTip, atTip)
				assert.Equal(t, test.expectedIdentifier, tipBlock)
				assert.NoError(t, err)
			}
			test.f.AssertExpectations(t)
			test.b.AssertExpectations(t)
		})
	}
}

func TestTimeToTip(t *testing.T) {
	tests := map[string]struct {
		blocksPerSecond float64
		lastSyncedIndex int64
		tipIndex        int64

		expectedTime string
	}{
		"far from tip": {
			blocksPerSecond: 6,
			lastSyncedIndex: 10,
			tipIndex:        10000,

			expectedTime: "27m45s",
		},
		"0 blocks per second": {
			blocksPerSecond: 0,
			lastSyncedIndex: 10,
			tipIndex:        1000,
			expectedTime:    "2750h0m0s",
		},
		"negative blocks remaining": {
			blocksPerSecond: 10,
			lastSyncedIndex: 100,
			tipIndex:        10,
			expectedTime:    "0s",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			timeToTip := TimeToTip(test.blocksPerSecond, test.lastSyncedIndex, test.tipIndex)
			assert.Equal(t, test.expectedTime, timeToTip.String())
		})
	}
}
