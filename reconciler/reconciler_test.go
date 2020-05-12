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

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/stretchr/testify/assert"
)

func TestContainsAccountCurrency(t *testing.T) {
	currency1 := &types.Currency{
		Symbol:   "Blah",
		Decimals: 2,
	}
	currency2 := &types.Currency{
		Symbol:   "Blah2",
		Decimals: 2,
	}
	accts := []*AccountCurrency{
		{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency1,
		},
		{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
				},
			},
			Currency: currency1,
		},
		{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: map[string]interface{}{
						"neat": "stuff",
					},
				},
			},
			Currency: currency1,
		},
	}

	t.Run("Non-existent account", func(t *testing.T) {
		assert.False(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "blah",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account", func(t *testing.T) {
		assert.True(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account with bad currency", func(t *testing.T) {
		assert.False(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency2,
		}))
	})

	t.Run("Account with subaccount", func(t *testing.T) {
		assert.True(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
				},
			},
			Currency: currency1,
		}))
	})

	t.Run("Account with subaccount and metadata", func(t *testing.T) {
		assert.True(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: map[string]interface{}{
						"neat": "stuff",
					},
				},
			},
			Currency: currency1,
		}))
	})

	t.Run("Account with subaccount and unique metadata", func(t *testing.T) {
		assert.False(t, ContainsAccountCurrency(accts, &AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "cool",
				SubAccount: &types.SubAccountIdentifier{
					Address: "test2",
					Metadata: map[string]interface{}{
						"neater": "stuff",
					},
				},
			},
			Currency: currency1,
		}))
	})
}

func TestExtractAmount(t *testing.T) {
	var (
		currency1 = &types.Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		currency2 = &types.Currency{
			Symbol:   "curr2",
			Decimals: 7,
		}

		amount1 = &types.Amount{
			Value:    "100",
			Currency: currency1,
		}

		amount2 = &types.Amount{
			Value:    "200",
			Currency: currency2,
		}

		balances = []*types.Amount{
			amount1,
			amount2,
		}

		badCurr = &types.Currency{
			Symbol:   "no curr",
			Decimals: 100,
		}
	)

	t.Run("Non-existent currency", func(t *testing.T) {
		result, err := ExtractAmount(balances, badCurr)
		assert.Nil(t, result)
		assert.EqualError(t, err, fmt.Errorf("could not extract amount for %+v", badCurr).Error())
	})

	t.Run("Simple account", func(t *testing.T) {
		result, err := ExtractAmount(balances, currency1)
		assert.Equal(t, amount1, result)
		assert.NoError(t, err)
	})

	t.Run("SubAccount", func(t *testing.T) {
		result, err := ExtractAmount(balances, currency2)
		assert.Equal(t, amount2, result)
		assert.NoError(t, err)
	})
}

func TestCompareBalance(t *testing.T) {
	var (
		account1 = &types.AccountIdentifier{
			Address: "blah",
		}

		account2 = &types.AccountIdentifier{
			Address: "blah",
			SubAccount: &types.SubAccountIdentifier{
				Address: "sub blah",
			},
		}

		currency1 = &types.Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		currency2 = &types.Currency{
			Symbol:   "curr2",
			Decimals: 7,
		}

		amount1 = &types.Amount{
			Value:    "100",
			Currency: currency1,
		}

		amount2 = &types.Amount{
			Value:    "200",
			Currency: currency2,
		}

		block0 = &types.BlockIdentifier{
			Hash:  "block0",
			Index: 0,
		}

		block1 = &types.BlockIdentifier{
			Hash:  "block1",
			Index: 1,
		}

		block2 = &types.BlockIdentifier{
			Hash:  "block2",
			Index: 2,
		}

		ctx = context.Background()

		mh = &MockReconcilerHelper{}
	)

	reconciler := NewReconciler(
		nil,
		mh,
		nil,
		nil,
		1,
		false,
		[]*AccountCurrency{},
	)

	t.Run("No head block yet", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, "", cachedBalance)
		assert.Equal(t, int64(0), headIndex)
		assert.Error(t, err)
	})

	// Update head block
	mh.HeadBlock = block0

	t.Run("Live block is ahead of head block", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, "", cachedBalance)
		assert.Equal(t, int64(0), headIndex)
		assert.EqualError(t, err, fmt.Errorf(
			"%w live block %d > head block %d",
			ErrHeadBlockBehindLive,
			1,
			0,
		).Error())
	})

	// Update head block
	mh.HeadBlock = &types.BlockIdentifier{
		Hash:  "hash2",
		Index: 2,
	}

	t.Run("Live block is not in store", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, "", cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), ErrBlockGone.Error())
	})

	// Add blocks to store behind head
	mh.StoredBlocks = map[string]*types.Block{}
	mh.StoredBlocks[block0.Hash] = &types.Block{
		BlockIdentifier:       block0,
		ParentBlockIdentifier: block0,
	}
	mh.StoredBlocks[block1.Hash] = &types.Block{
		BlockIdentifier:       block1,
		ParentBlockIdentifier: block0,
	}
	mh.StoredBlocks[block2.Hash] = &types.Block{
		BlockIdentifier:       block2,
		ParentBlockIdentifier: block1,
	}
	mh.BalanceAccount = account1
	mh.BalanceAmount = amount1
	mh.BalanceBlock = block1

	t.Run("Account updated after live block", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block0,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, "", cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.Contains(t, err.Error(), ErrAccountUpdated.Error())
	})

	t.Run("Account balance matches", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block1,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, amount1.Value, cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Account balance matches later live block", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, amount1.Value, cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Balances are not equal", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount2.Value,
			block2,
		)
		assert.Equal(t, "-100", difference)
		assert.Equal(t, amount1.Value, cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
	})

	t.Run("Compare balance for non-existent account", func(t *testing.T) {
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account2,
			currency1,
			amount2.Value,
			block2,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, "", cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.Error(t, err)
	})
}

type MockReconcilerHelper struct {
	HeadBlock    *types.BlockIdentifier
	StoredBlocks map[string]*types.Block

	BalanceAccount *types.AccountIdentifier
	BalanceAmount  *types.Amount
	BalanceBlock   *types.BlockIdentifier
}

func (h *MockReconcilerHelper) BlockExists(
	ctx context.Context,
	block *types.BlockIdentifier,
) (bool, error) {
	_, ok := h.StoredBlocks[block.Hash]
	if !ok {
		return false, nil
	}

	return true, nil
}

func (h *MockReconcilerHelper) CurrentBlock(
	ctx context.Context,
) (*types.BlockIdentifier, error) {
	if h.HeadBlock == nil {
		return nil, errors.New("head block is nil")
	}

	return h.HeadBlock, nil
}

func (h *MockReconcilerHelper) AccountBalance(
	ctx context.Context,
	account *types.AccountIdentifier,
	currency *types.Currency,
	headBlock *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	if h.BalanceAccount == nil || !reflect.DeepEqual(account, h.BalanceAccount) {
		return nil, nil, errors.New("account does not exist")
	}

	return h.BalanceAmount, h.BalanceBlock, nil
}
