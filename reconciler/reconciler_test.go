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

package reconciler

import (
	"context"
	"errors"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/reconciler"
	mockDatabase "github.com/coinbase/rosetta-sdk-go/mocks/storage/database"
	"github.com/coinbase/rosetta-sdk-go/parser"
	storageErrors "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func TestNewReconciler(t *testing.T) {
	var (
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "acct 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)
	var tests = map[string]struct {
		options []Option

		expected *Reconciler
	}{
		"no options": {
			expected: New(nil, nil, nil),
		},
		"with reconciler concurrency": {
			options: []Option{
				WithInactiveConcurrency(100),
				WithActiveConcurrency(200),
			},
			expected: func() *Reconciler {
				r := New(nil, nil, nil)
				r.InactiveConcurrency = 100
				r.ActiveConcurrency = 200

				return r
			}(),
		},
		"with interesting accounts": {
			options: []Option{
				WithInterestingAccounts([]*types.AccountCurrency{
					accountCurrency,
				}),
			},
			expected: func() *Reconciler {
				r := New(nil, nil, nil)
				r.interestingAccounts = []*types.AccountCurrency{
					accountCurrency,
				}

				return r
			}(),
		},
		"with seen accounts": {
			options: []Option{
				WithSeenAccounts([]*types.AccountCurrency{
					accountCurrency,
				}),
			},
			expected: func() *Reconciler {
				r := New(nil, nil, nil)
				r.inactiveQueue = []*InactiveEntry{
					{
						Entry: accountCurrency,
					},
				}
				r.seenAccounts = map[string]struct{}{
					types.Hash(accountCurrency): {},
				}

				return r
			}(),
		},
		"without lookupBalanceByBlock": {
			options: []Option{},
			expected: func() *Reconciler {
				r := New(nil, nil, nil)
				r.lookupBalanceByBlock = false
				r.changeQueue = make(chan *parser.BalanceChange, defaultBacklogSize)

				return r
			}(),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result := New(nil, nil, nil, test.options...)
			assert.ElementsMatch(t, test.expected.inactiveQueue, result.inactiveQueue)
			assert.Equal(t, test.expected.seenAccounts, result.seenAccounts)
			assert.ElementsMatch(t, test.expected.interestingAccounts, result.interestingAccounts)
			assert.Equal(t, test.expected.InactiveConcurrency, result.InactiveConcurrency)
			assert.Equal(t, test.expected.ActiveConcurrency, result.ActiveConcurrency)
			assert.Equal(t, test.expected.lookupBalanceByBlock, result.lookupBalanceByBlock)
			assert.Equal(t, cap(test.expected.changeQueue), cap(result.changeQueue))
		})
	}
}

func TestContainsAccountCurrency(t *testing.T) {
	currency1 := &types.Currency{
		Symbol:   "Blah",
		Decimals: 2,
	}
	currency2 := &types.Currency{
		Symbol:   "Blah2",
		Decimals: 2,
	}
	acctSlice := []*types.AccountCurrency{
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

	accts := map[string]struct{}{}
	for _, acct := range acctSlice {
		accts[types.Hash(acct)] = struct{}{}
	}

	t.Run("Non-existent account", func(t *testing.T) {
		assert.False(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "blah",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account", func(t *testing.T) {
		assert.True(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency1,
		}))
	})

	t.Run("Basic account with bad currency", func(t *testing.T) {
		assert.False(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "test",
			},
			Currency: currency2,
		}))
	})

	t.Run("Account with subaccount", func(t *testing.T) {
		assert.True(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
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
		assert.True(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
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
		assert.False(t, ContainsAccountCurrency(accts, &types.AccountCurrency{
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

		mh = &mocks.Helper{}
	)

	reconciler := New(
		mh,
		nil,
		nil,
	)

	t.Run("No head block yet", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(nil, errors.New("no head block")).Once()
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
		mtxn.AssertExpectations(t)
	})

	t.Run("Live block is ahead of head block", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block0, nil).Once()
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
			"live block %d > head block %d: %w",
			1,
			0,
			ErrHeadBlockBehindLive,
		).Error())
		mtxn.AssertExpectations(t)
	})

	t.Run("Live block is not in store", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block1).Return(false, nil).Once()
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
		mtxn.AssertExpectations(t)
	})

	t.Run("Account updated after live block", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block0).Return(true, nil).Once()
		mh.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account1,
			amount1.Currency,
			block0.Index,
		).Return(
			amount1,
			nil,
		).Once()
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount1.Value,
			block0,
		)
		assert.Equal(t, "0", difference)
		assert.Equal(t, amount1.Value, cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
		mtxn.AssertExpectations(t)
	})

	t.Run("Account balance matches", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block1).Return(true, nil).Once()
		mh.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account1,
			amount1.Currency,
			block1.Index,
		).Return(
			amount1,
			nil,
		).Once()
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
		mtxn.AssertExpectations(t)
	})

	t.Run("Account balance matches later live block", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block2).Return(true, nil).Once()
		mh.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account1,
			currency1,
			block2.Index,
		).Return(
			amount1,
			nil,
		).Once()
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
		mtxn.AssertExpectations(t)
	})

	t.Run("Balances are not equal", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block2).Return(true, nil).Once()
		mh.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account1,
			currency1,
			block2.Index,
		).Return(
			amount1,
			nil,
		).Once()
		difference, cachedBalance, headIndex, err := reconciler.CompareBalance(
			ctx,
			account1,
			currency1,
			amount2.Value,
			block2,
		)
		assert.Equal(t, "100", difference)
		assert.Equal(t, amount1.Value, cachedBalance)
		assert.Equal(t, int64(2), headIndex)
		assert.NoError(t, err)
		mtxn.AssertExpectations(t)
	})

	t.Run("Compare balance for non-existent account", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mh.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mh.On("CurrentBlock", ctx, mtxn).Return(block2, nil).Once()
		mh.On("CanonicalBlock", ctx, mtxn, block2).Return(true, nil).Once()
		mh.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account2,
			currency1,
			block2.Index,
		).Return(
			nil,
			storageErrors.ErrAccountMissing,
		).Once()
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
		mtxn.AssertExpectations(t)
	})

	mh.AssertExpectations(t)
}

func TestAccountReconciliation(t *testing.T) {
	var (
		account = &types.AccountIdentifier{
			Address: "blah",
		}

		currency = &types.Currency{
			Symbol:   "curr1",
			Decimals: 4,
		}

		block0 = &types.BlockIdentifier{
			Hash:  "block0",
			Index: 0,
		}

		block1 = &types.BlockIdentifier{
			Hash:  "block1",
			Index: 2,
		}

		ctx = context.Background()

		mockHelper  = &mocks.Helper{}
		mockHandler = &mocks.Handler{}
	)

	reconciler := New(
		mockHelper,
		mockHandler,
		nil,
	)

	t.Run("account is missing", func(t *testing.T) {
		mtxn := &mockDatabase.Transaction{}
		mtxn.On("Discard", ctx).Once()
		mockHelper.On("DatabaseTransaction", ctx).Return(mtxn).Once()
		mockHelper.On("CurrentBlock", ctx, mtxn).Return(block1, nil).Once()
		mockHelper.On("CanonicalBlock", ctx, mtxn, block0).Return(true, nil).Once()
		mockHelper.On(
			"ComputedBalance",
			ctx,
			mtxn,
			account,
			currency,
			block0.Index,
		).Return(
			nil,
			storageErrors.ErrAccountMissing,
		).Once()
		mockHandler.On(
			"ReconciliationSkipped",
			ctx,
			ActiveReconciliation,
			account,
			currency,
			AccountMissing,
		).Return(
			nil,
		).Once()
		err := reconciler.accountReconciliation(ctx, account, currency, "100", block0, false)
		assert.NoError(t, err)
		mockHelper.AssertExpectations(t)
		mockHandler.AssertExpectations(t)
	})
}

func assertContainsAllAccounts(t *testing.T, m map[string]struct{}, a []*types.AccountCurrency) {
	for _, account := range a {
		_, exists := m[types.Hash(account)]
		assert.True(t, exists)
	}
}

func TestInactiveAccountQueue(t *testing.T) {
	var (
		r = New(
			nil,
			nil,
			parser.New(nil, nil, nil),
			WithBalancePruning(), // test that not invoked for inactive reconciliation
		)
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		accountCurrency2 = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 2",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	t.Run("new account in active reconciliation", func(t *testing.T) {
		err := r.inactiveAccountQueue(
			false,
			accountCurrency,
			block,
			false,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(t, r.seenAccounts, []*types.AccountCurrency{accountCurrency})
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{
			{
				Entry:     accountCurrency,
				LastCheck: block,
			},
		})
	})

	t.Run("another new account in active reconciliation", func(t *testing.T) {
		err := r.inactiveAccountQueue(
			false,
			accountCurrency2,
			block2,
			false,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*types.AccountCurrency{accountCurrency, accountCurrency2},
		)
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{
			{
				Entry:     accountCurrency,
				LastCheck: block,
			},
			{
				Entry:     accountCurrency2,
				LastCheck: block2,
			},
		})
	})

	t.Run("previous account in active reconciliation", func(t *testing.T) {
		r.inactiveQueue = []*InactiveEntry{}

		err := r.inactiveAccountQueue(
			false,
			accountCurrency,
			block,
			false,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*types.AccountCurrency{accountCurrency, accountCurrency2},
		)
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{})
	})

	t.Run("previous account in inactive reconciliation", func(t *testing.T) {
		err := r.inactiveAccountQueue(
			true,
			accountCurrency,
			block,
			false,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*types.AccountCurrency{accountCurrency, accountCurrency2},
		)
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{
			{
				Entry:     accountCurrency,
				LastCheck: block,
			},
		})
	})

	t.Run("another previous account in inactive reconciliation", func(t *testing.T) {
		err := r.inactiveAccountQueue(
			true,
			accountCurrency2,
			block2,
			false,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*types.AccountCurrency{accountCurrency, accountCurrency2},
		)
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{
			{
				Entry:     accountCurrency,
				LastCheck: block,
			},
			{
				Entry:     accountCurrency2,
				LastCheck: block2,
			},
		})
	})
}

func mockReconcilerCalls(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	mtxn *mockDatabase.Transaction,
	lookupBalanceByBlock bool,
	accountCurrency *types.AccountCurrency,
	liveValue string,
	computedValue string,
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
	success bool,
	reconciliationType string,
	exemption *types.BalanceExemption,
	exemptionHit bool,
	exemptionThrows bool,
) {
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(headBlock, nil).Once()
	lookupIndex := liveBlock.Index
	if !lookupBalanceByBlock {
		lookupIndex = -1
	}

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		lookupIndex,
	).Return(
		&types.Amount{Value: liveValue, Currency: accountCurrency.Currency},
		headBlock,
		nil,
	).Once()
	mockHelper.On("CanonicalBlock", mock.Anything, mtxn, headBlock).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		mtxn,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock.Index,
	).Return(
		&types.Amount{Value: computedValue, Currency: accountCurrency.Currency},
		nil,
	).Once()
	if success {
		mockHandler.On(
			"ReconciliationSucceeded",
			mock.Anything,
			reconciliationType,
			accountCurrency.Account,
			accountCurrency.Currency,
			liveValue,
			headBlock,
		).Return(nil).Once()
	} else {
		if !exemptionHit {
			mockHandler.On(
				"ReconciliationFailed",
				mock.Anything,
				reconciliationType,
				accountCurrency.Account,
				accountCurrency.Currency,
				computedValue,
				liveValue,
				headBlock,
			).Return(errors.New("reconciliation failed")).Once()
		} else {
			if !exemptionThrows {
				mockHandler.On(
					"ReconciliationExempt",
					mock.Anything,
					reconciliationType,
					accountCurrency.Account,
					accountCurrency.Currency,
					computedValue,
					liveValue,
					headBlock,
					exemption,
				).Return(nil).Once()
			} else {
				mockHandler.On(
					"ReconciliationExempt",
					mock.Anything,
					reconciliationType,
					accountCurrency.Account,
					accountCurrency.Currency,
					computedValue,
					liveValue,
					headBlock,
					exemption,
				).Return(errors.New("reconciliation failed for exemption")).Once()
			}
		}
	}
}

func shardsEmpty(m *utils.ShardedMap, keys []string) bool {
	for _, k := range keys {
		s := m.Lock(k, false)
		m.Unlock(k)
		if len(s) > 0 {
			return false
		}
	}

	return true
}

func TestReconcile_SuccessOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		accountCurrency2 = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 2",
			},
			Currency: &types.Currency{
				Symbol:   "ETH",
				Decimals: 18,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
				WithInterestingAccounts([]*types.AccountCurrency{accountCurrency2}),
				WithBalancePruning(),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				nil,
				opts...,
			)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			mockHelper.On(
				"PruneBalances",
				mock.Anything,
				accountCurrency.Account,
				accountCurrency.Currency,
				block.Index-safeBalancePruneDepth,
			).Return(
				nil,
			).Once()
			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"100",
				"100",
				block2,
				block,
				true,
				ActiveReconciliation,
				nil,
				false,
				false,
			)

			mockHelper.On(
				"PruneBalances",
				mock.Anything,
				accountCurrency2.Account,
				accountCurrency2.Currency,
				block.Index-safeBalancePruneDepth,
			).Return(
				nil,
			).Once()
			mtxn2 := &mockDatabase.Transaction{}
			mtxn2.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn2,
				lookup,
				accountCurrency2,
				"250",
				"250",
				block2,
				block,
				true,
				ActiveReconciliation,
				nil,
				false,
				false,
			)

			mockHelper.On(
				"PruneBalances",
				mock.Anything,
				accountCurrency2.Account,
				accountCurrency2.Currency,
				block2.Index-safeBalancePruneDepth,
			).Return(
				nil,
			).Once()
			mtxn3 := &mockDatabase.Transaction{}
			mtxn3.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn3,
				lookup,
				accountCurrency2,
				"120",
				"120",
				block2,
				block2,
				true,
				ActiveReconciliation,
				nil,
				false,
				false,
			)

			assert.Equal(t, int64(-1), r.LastIndexReconciled())

			go func() {
				err := r.Reconcile(ctx)
				assert.Contains(t, context.Canceled.Error(), err.Error())
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:  accountCurrency.Account,
					Currency: accountCurrency.Currency,
					Block:    block,
				},
			})
			assert.NoError(t, err)
			err = r.QueueChanges(ctx, block2, []*parser.BalanceChange{
				{
					Account:  accountCurrency2.Account,
					Currency: accountCurrency2.Currency,
					Block:    block2,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)
			cancel()

			assert.Equal(t, block2.Index, r.LastIndexReconciled())
			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			assert.True(t, shardsEmpty(
				r.queueMap,
				[]string{
					types.Hash(accountCurrency),
					types.Hash(accountCurrency2),
				},
			))
			mtxn.AssertExpectations(t)
			mtxn2.AssertExpectations(t)
			mtxn3.AssertExpectations(t)
		})
	}
}

func TestReconcile_HighWaterMark(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		accountCurrency2 = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 2",
			},
			Currency: &types.Currency{
				Symbol:   "ETH",
				Decimals: 18,
			},
		}
		block200 = &types.BlockIdentifier{
			Hash:  "block 200",
			Index: 200,
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithInterestingAccounts([]*types.AccountCurrency{accountCurrency2}),
		WithDebugLogging(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// First call to QueueChanges
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()
	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(-1),
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		block200,
		nil,
	).Once()

	// Second call to QueueChanges
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn2,
		false,
		accountCurrency,
		"150",
		"150",
		block200,
		block200,
		true,
		ActiveReconciliation,
		nil,
		false,
		false,
	)
	mtxn3 := &mockDatabase.Transaction{}
	mtxn3.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn3,
		false,
		accountCurrency2,
		"120",
		"120",
		block200,
		block200,
		true,
		ActiveReconciliation,
		nil,
		false,
		false,
	)

	// Skip handler called
	mockHandler.On(
		"ReconciliationSkipped",
		mock.Anything,
		ActiveReconciliation,
		accountCurrency.Account,
		accountCurrency.Currency,
		HeadBehind,
	).Return(nil).Once()
	mockHandler.On(
		"ReconciliationSkipped",
		mock.Anything,
		ActiveReconciliation,
		accountCurrency2.Account,
		accountCurrency2.Currency,
		HeadBehind,
	).Return(nil).Once()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)
	err = r.QueueChanges(ctx, block200, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block200,
		},
	})
	assert.NoError(t, err)
	assert.Equal(t, 2, len(r.processQueue))
	assert.Equal(t, 0, r.QueueSize()) // queue size is 0 before starting worker

	go func() {
		err := r.Reconcile(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	time.Sleep(1 * time.Second)
	cancel()

	assert.Equal(t, 0, len(r.processQueue))
	assert.Equal(t, 0, r.QueueSize())

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
	mtxn2.AssertExpectations(t)
	mtxn3.AssertExpectations(t)
}

func TestReconcile_Orphan(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithLookupBalanceByBlock(),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		&types.Amount{
			Value:    "100",
			Currency: accountCurrency.Currency,
		},
		block,
		nil,
	).Once()
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(&types.BlockIdentifier{
		Index: 1,
		Hash:  "block 1a",
	}, nil).Once()
	mockHelper.On("CanonicalBlock", mock.Anything, mtxn, block).Return(false, nil).Once()
	mockHandler.On(
		"ReconciliationSkipped",
		mock.Anything,
		ActiveReconciliation,
		accountCurrency.Account,
		accountCurrency.Currency,
		BlockGone,
	).Return(nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
}

func TestReconcile_FailureOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				parser.New(nil, nil, nil),
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
				false,
				ActiveReconciliation,
				nil,
				false,
				false,
			)

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)
			assert.Equal(t, 1, len(r.processQueue))
			assert.Equal(t, 0, r.QueueSize()) // queue size is 0 before starting worker

			go func() {
				err := r.Reconcile(ctx)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "reconciliation failed")
			}()

			time.Sleep(1 * time.Second)
			assert.Equal(t, 0, len(r.processQueue))
			assert.Equal(t, 0, r.QueueSize())

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_ExemptOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		exemption = &types.BalanceExemption{
			ExemptionType: types.BalanceGreaterOrEqual,
			Currency:      accountCurrency.Currency,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()
			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				true,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.NoError(t, err)
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_ExemptAddressOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "addr",
				},
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		subAccountAddress = "addr"
		exemption         = &types.BalanceExemption{
			ExemptionType:     types.BalanceLessOrEqual,
			SubAccountAddress: &subAccountAddress,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"100",
				"105",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				true,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.NoError(t, err)
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_ExemptAddressDynamicActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "addr",
				},
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		subAccountAddress = "addr"
		exemption         = &types.BalanceExemption{
			ExemptionType:     types.BalanceDynamic,
			SubAccountAddress: &subAccountAddress,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"100",
				"105",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				true,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.NoError(t, err)
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_ExemptAddressDynamicActiveThrow(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "addr",
				},
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		subAccountAddress = "addr"
		exemption         = &types.BalanceExemption{
			ExemptionType:     types.BalanceDynamic,
			SubAccountAddress: &subAccountAddress,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"100",
				"105",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				true,
				true,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.Error(t, err)
				assert.Contains(t, "reconciliation failed for exemption", err.Error())
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_NotExemptOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		exemption = &types.BalanceExemption{
			ExemptionType: types.BalanceLessOrEqual,
			Currency:      accountCurrency.Currency,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				false,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "reconciliation failed")
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_NotExemptAddressOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		subAddr   = "addr"
		exemption = &types.BalanceExemption{
			ExemptionType:     types.BalanceGreaterOrEqual,
			SubAccountAddress: &subAddr,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				false,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "reconciliation failed")
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_NotExemptWrongAddressOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
				SubAccount: &types.SubAccountIdentifier{
					Address: "not addr",
				},
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		subAddr   = "addr"
		exemption = &types.BalanceExemption{
			ExemptionType:     types.BalanceGreaterOrEqual,
			SubAccountAddress: &subAddr,
		}
		p = parser.New(nil, nil, []*types.BalanceExemption{
			exemption,
		})
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				p,
				opts...,
			)
			ctx := context.Background()

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
				false,
				ActiveReconciliation,
				exemption,
				false,
				false,
			)

			go func() {
				err := r.Reconcile(ctx)
				assert.Error(t, err)
				assert.Contains(t, err.Error(), "reconciliation failed")
			}()

			err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
				{
					Account:    accountCurrency.Account,
					Currency:   accountCurrency.Currency,
					Difference: "100",
					Block:      block,
				},
			})
			assert.NoError(t, err)

			time.Sleep(1 * time.Second)

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
		})
	}
}

func TestReconcile_SuccessOnlyInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 2",
			Index: 2,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(0),
				WithInactiveConcurrency(1),
				WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
				WithDebugLogging(),
				WithInactiveFrequency(1),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				nil,
				opts...,
			)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()
			mtxn2 := &mockDatabase.Transaction{}
			mtxn2.On(
				"Discard",
				mock.Anything,
			).Run(
				func(args mock.Arguments) {
					cancel()
				},
			).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn2,
				lookup,
				accountCurrency,
				"100",
				"100",
				block,
				block,
				true,
				InactiveReconciliation,
				nil,
				false,
				false,
			)

			err := r.Reconcile(ctx)
			assert.Contains(t, context.Canceled.Error(), err.Error())

			mockHelper2 := &mocks.Helper{}
			mockHandler2 := &mocks.Handler{}
			r.helper = mockHelper2
			r.handler = mockHandler2
			ctx = context.Background()
			ctx, cancel = context.WithCancel(ctx)

			mtxn3 := &mockDatabase.Transaction{}
			mtxn3.On("Discard", mock.Anything).Once()
			mockHelper2.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
			mockHelper2.On("CurrentBlock", mock.Anything, mtxn3).Return(block2, nil).Once()
			mtxn4 := &mockDatabase.Transaction{}
			mtxn4.On("Discard", mock.Anything).Run(
				func(args mock.Arguments) {
					cancel()
				},
			).Once()
			mockHelper2.On("DatabaseTransaction", mock.Anything).Return(mtxn4).Once()
			mockReconcilerCalls(
				mockHelper2,
				mockHandler2,
				mtxn4,
				lookup,
				accountCurrency,
				"200",
				"200",
				block2,
				block2,
				true,
				InactiveReconciliation,
				nil,
				false,
				false,
			)

			err = r.Reconcile(ctx)
			assert.Error(t, err)
			assert.Contains(t, context.Canceled.Error(), err.Error())

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mockHelper2.AssertExpectations(t)
			mockHandler2.AssertExpectations(t)
			mtxn.AssertExpectations(t)
			mtxn2.AssertExpectations(t)
			mtxn3.AssertExpectations(t)
			mtxn4.AssertExpectations(t)
		})
	}
}

func TestReconcile_FailureOnlyInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			opts := []Option{
				WithActiveConcurrency(0),
				WithInactiveConcurrency(1),
				WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
				WithDebugLogging(),
				WithInactiveFrequency(1),
			}
			if lookup {
				opts = append(opts, WithLookupBalanceByBlock())
			}
			r := New(
				mockHelper,
				mockHandler,
				parser.New(nil, nil, nil),
				opts...,
			)
			ctx := context.Background()
			mtxn := &mockDatabase.Transaction{}
			mtxn.On("Discard", mock.Anything).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
			mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()
			mtxn2 := &mockDatabase.Transaction{}
			mtxn2.On(
				"Discard",
				mock.Anything,
			).Once()
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
			mockReconcilerCalls(
				mockHelper,
				mockHandler,
				mtxn2,
				lookup,
				accountCurrency,
				"100",
				"105",
				block,
				block,
				false,
				InactiveReconciliation,
				nil,
				false,
				false,
			)

			err := r.Reconcile(ctx)
			assert.Error(t, err)
			assert.Contains(t, err.Error(), "reconciliation failed")

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mtxn.AssertExpectations(t)
			mtxn2.AssertExpectations(t)
		})
	}
}

func TestReconcile_EnqueueCancel(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block.Index,
	).Return(
		nil,
		nil,
		context.Canceled,
	).Once()

	change := &parser.BalanceChange{
		Account:    accountCurrency.Account,
		Currency:   accountCurrency.Currency,
		Difference: "100",
		Block:      block,
	}
	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		change,
	})
	assert.NoError(t, err)
	assert.Equal(t, 1, len(r.processQueue))
	assert.Equal(t, 0, r.QueueSize()) // queue size is 0 before starting worker

	go func() {
		err := r.Reconcile(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	time.Sleep(1 * time.Second)
	assert.Equal(t, r.QueueSize(), 1)
	existingChange := <-r.changeQueue
	assert.Equal(t, change, existingChange)

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_ActiveIndexAtTipError(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithLookupBalanceByBlock(),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(true, nil).Once()
	mockHandler.On(
		"ReconciliationSkipped",
		mock.Anything,
		ActiveReconciliation,
		accountCurrency.Account,
		accountCurrency.Currency,
		TipFailure,
	).Return(nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_ActiveNotIndexAtTipError(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithLookupBalanceByBlock(),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(false, nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		expectErr := fmt.Errorf(
			"failed to lookup balance for currency %s of account %s at height %d",
			types.PrintStruct(accountCurrency.Currency),
			types.PrintStruct(accountCurrency.Account),
			int64(1),
		)
		assert.Contains(t, err.Error(), expectErr.Error())
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_ActiveErrorIndexAtTipError(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithLookupBalanceByBlock(),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(false, errors.New("blazz")).Once()

	go func() {
		err := r.Reconcile(ctx)
		expectErr := fmt.Errorf(
			"failed to lookup balance for currency %s of account %s at height %d",
			types.PrintStruct(accountCurrency.Currency),
			types.PrintStruct(accountCurrency.Account),
			int64(1),
		)
		assert.Contains(t, err.Error(), expectErr.Error())
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_FailureIndexAtTipInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(0),
		WithInactiveConcurrency(1),
		WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
		WithDebugLogging(),
		WithInactiveFrequency(1),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		parser.New(nil, nil, nil),
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(true, nil).Once()
	mockHandler.On(
		"ReconciliationSkipped",
		mock.Anything,
		InactiveReconciliation,
		accountCurrency.Account,
		accountCurrency.Currency,
		TipFailure,
	).Run(
		func(args mock.Arguments) {
			cancel()
		},
	).Return(nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		assert.True(t, errors.Is(err, context.Canceled))
	}()

	time.Sleep(1 * time.Second)
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
}

func TestReconcile_FailureNotIndexAtTipInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(0),
		WithInactiveConcurrency(1),
		WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
		WithDebugLogging(),
		WithInactiveFrequency(1),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		parser.New(nil, nil, nil),
		opts...,
	)
	ctx := context.Background()

	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(false, nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		expectErr := fmt.Errorf(
			"failed to lookup balance for currency %s of account %s at height %d",
			types.PrintStruct(accountCurrency.Currency),
			types.PrintStruct(accountCurrency.Account),
			int64(1),
		)
		assert.Contains(t, err.Error(), expectErr.Error())
	}()

	time.Sleep(1 * time.Second)
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
}

func TestReconcile_FailureErrorIndexAtTipInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(0),
		WithInactiveConcurrency(1),
		WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
		WithDebugLogging(),
		WithInactiveFrequency(1),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		parser.New(nil, nil, nil),
		opts...,
	)
	ctx := context.Background()

	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		int64(1),
	).Return(
		nil,
		nil,
		errors.New("blah"),
	).Once()
	mockHelper.On("IndexAtTip", mock.Anything, int64(1)).Return(false, errors.New("blah")).Once()

	go func() {
		err := r.Reconcile(ctx)
		expectErr := fmt.Errorf(
			"failed to lookup balance for currency %s of account %s at height %d",
			types.PrintStruct(accountCurrency.Currency),
			types.PrintStruct(accountCurrency.Account),
			int64(1),
		)
		assert.Contains(t, err.Error(), expectErr.Error())
	}()

	time.Sleep(1 * time.Second)
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
}

func mockReconcilerCallsDelay(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	accountCurrency *types.AccountCurrency,
	value string, // nolint:unparam
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
	liveDelay int,
	reconciliationType string,
) {
	mockHelper.On("CurrentBlock", mock.Anything, mock.Anything).Return(headBlock, nil).Once()
	lookupIndex := liveBlock.Index
	if liveDelay != -1 {
		mockHelper.On(
			"LiveBalance",
			mock.Anything,
			accountCurrency.Account,
			accountCurrency.Currency,
			lookupIndex,
		).Return(
			&types.Amount{Value: value, Currency: accountCurrency.Currency},
			headBlock,
			nil,
		).After(time.Duration(liveDelay) * time.Millisecond).Once()
	}
	mockHelper.On(
		"CanonicalBlock",
		mock.Anything,
		mock.Anything,
		headBlock,
	).Return(
		true,
		nil,
	).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock.Index,
	).Return(
		&types.Amount{Value: value, Currency: accountCurrency.Currency},
		nil,
	).Once()
	if len(reconciliationType) != 0 {
		mockHandler.On(
			"ReconciliationSucceeded",
			mock.Anything,
			reconciliationType,
			accountCurrency.Account,
			accountCurrency.Currency,
			value,
			headBlock,
		).Return(nil).Once()
	}
}

func TestPruningRaceCondition(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 200",
			Index: 200,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		accountCurrency2 = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 2",
			},
			Currency: &types.Currency{
				Symbol:   "ETH",
				Decimals: 18,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 300",
			Index: 300,
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(2),
		WithInactiveConcurrency(0),
		WithBalancePruning(),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"PruneBalances",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block.Index-safeBalancePruneDepth,
	).Run(
		func(args mock.Arguments) {
			cancel()
		},
	).Return(
		nil,
	).Once()
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		block2,
		block,
		200, // delay live response 200 ms
		ActiveReconciliation,
	)

	mockHelper.On(
		"PruneBalances",
		mock.Anything,
		accountCurrency2.Account,
		accountCurrency2.Currency,
		block2.Index-safeBalancePruneDepth,
	).Return(
		nil,
	).Once()
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn2,
		true,
		accountCurrency2,
		"120",
		"120",
		block2,
		block2,
		true,
		ActiveReconciliation,
		nil,
		false,
		false,
	)

	mtxn3 := &mockDatabase.Transaction{}
	mtxn3.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn3,
		true,
		accountCurrency,
		"100",
		"100",
		block2,
		block2,
		true,
		ActiveReconciliation,
		nil,
		false,
		false,
	)

	assert.Equal(t, int64(-1), r.LastIndexReconciled())

	d := make(chan struct{})
	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, context.Canceled.Error(), err.Error())
		close(d)
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)
	err = r.QueueChanges(ctx, block2, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block2,
		},
		{
			Account:  accountCurrency2.Account,
			Currency: accountCurrency2.Currency,
			Block:    block2,
		},
	})
	assert.NoError(t, err)

	<-d
	assert.Equal(t, block2.Index, r.LastIndexReconciled())
	assert.True(t, shardsEmpty(
		r.queueMap,
		[]string{
			types.Hash(accountCurrency),
			types.Hash(accountCurrency2),
		},
	))
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
	mtxn2.AssertExpectations(t)
	mtxn3.AssertExpectations(t)
}

func TestPruningHappyPath(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 200",
			Index: 200,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		block2 = &types.BlockIdentifier{
			Hash:  "block 300",
			Index: 300,
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(2),
		WithInactiveConcurrency(0),
		WithBalancePruning(),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"PruneBalances",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block.Index-safeBalancePruneDepth,
	).Return(
		nil,
	).Once()
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		block2,
		block,
		0,
		ActiveReconciliation,
	)

	mockHelper.On(
		"PruneBalances",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block2.Index-safeBalancePruneDepth,
	).Run(
		func(args mock.Arguments) {
			cancel()
		},
	).Return(
		nil,
	).Once()
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		block2,
		block2,
		200, // delay by 200 ms
		ActiveReconciliation,
	)

	assert.Equal(t, int64(-1), r.LastIndexReconciled())

	d := make(chan struct{})
	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, context.Canceled.Error(), err.Error())
		close(d)
	}()

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)
	err = r.QueueChanges(ctx, block2, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block2,
		},
	})
	assert.NoError(t, err)

	<-d
	assert.Equal(t, block2.Index, r.LastIndexReconciled())
	assert.True(t, shardsEmpty(
		r.queueMap,
		[]string{
			types.Hash(accountCurrency),
		},
	))
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
	mtxn2.AssertExpectations(t)
}

func TestPruningReorg(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 200",
			Index: 200,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
		blockB = &types.BlockIdentifier{
			Hash:  "block 200 B",
			Index: 200,
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(2),
		WithInactiveConcurrency(0),
		WithBalancePruning(),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		block,
		block,
		0,
		ActiveReconciliation,
	)

	mockHelper.On(
		"PruneBalances",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		blockB.Index-safeBalancePruneDepth,
	).Run(
		func(args mock.Arguments) {
			cancel()
		},
	).Return(
		nil,
	).Once()
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		blockB,
		blockB,
		200, // delay by 200 ms
		ActiveReconciliation,
	)

	assert.Equal(t, int64(-1), r.LastIndexReconciled())

	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)
	err = r.QueueChanges(ctx, blockB, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    blockB,
		},
	})
	assert.NoError(t, err)

	d := make(chan struct{})
	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, context.Canceled.Error(), err.Error())
		close(d)
	}()

	<-d
	assert.Equal(t, blockB.Index, r.LastIndexReconciled())
	assert.True(t, shardsEmpty(
		r.queueMap,
		[]string{
			types.Hash(accountCurrency),
		},
	))
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
	mtxn2.AssertExpectations(t)
}

func TestPruningRaceConditionInactive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 3000",
			Index: 3000,
		}
		blockOld = &types.BlockIdentifier{
			Hash:  "block 100",
			Index: 100,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(1),
		WithInactiveConcurrency(1),
		WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
		WithBalancePruning(),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)
	b := make(chan struct{})
	c := make(chan struct{})

	// Start inactive fetch
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On(
		"CurrentBlock",
		mock.Anything,
		mtxn,
	).Return(blockOld, nil).Once()

	// Hang on live balance fetch for inactive
	mtxn3 := &mockDatabase.Transaction{}
	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		blockOld.Index,
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		blockOld,
		nil,
	).Run(
		func(args mock.Arguments) {
			close(b)
			<-c

			// Finish inactive fetch
			mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
			mtxn3.On(
				"Discard",
				mock.Anything,
			).Run(
				func(args mock.Arguments) {
					cancel()
				},
			).Once()
			mockReconcilerCallsDelay(
				mockHelper,
				mockHandler,
				accountCurrency,
				"100",
				blockOld,
				blockOld,
				-1, // delay live response 0 ms
				InactiveReconciliation,
			)
		},
	).Once()

	d := make(chan struct{})
	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, context.Canceled.Error(), err.Error())
		close(d)
	}()

	<-b
	// Active balance fetch
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On(
		"Discard",
		mock.Anything,
	).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCallsDelay(
		mockHelper,
		mockHandler,
		accountCurrency,
		"100",
		block,
		block,
		0, // delay live response 0 ms
		"",
	)
	mockHandler.On(
		"ReconciliationSucceeded",
		mock.Anything,
		ActiveReconciliation,
		accountCurrency.Account,
		accountCurrency.Currency,
		"100",
		block,
	).Return(nil).Run(
		func(args mock.Arguments) {
			close(c)
		},
	).Once()

	// Queue changes for active thread
	err := r.QueueChanges(ctx, block, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block,
		},
	})
	assert.NoError(t, err)

	<-d
	assert.Equal(t, block.Index, r.LastIndexReconciled())
	assert.True(t, shardsEmpty(
		r.queueMap,
		[]string{
			types.Hash(accountCurrency),
		},
	))
	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
	mtxn.AssertExpectations(t)
	mtxn2.AssertExpectations(t)
	mtxn3.AssertExpectations(t)
}

func TestReconcile_SuccessOnlyInactiveOverride(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &types.AccountCurrency{
			Account: &types.AccountIdentifier{
				Address: "addr 1",
			},
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		}
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	opts := []Option{
		WithActiveConcurrency(0),
		WithInactiveConcurrency(1),
		WithSeenAccounts([]*types.AccountCurrency{accountCurrency}),
		WithDebugLogging(),
		WithInactiveFrequency(10),
		WithLookupBalanceByBlock(),
	}
	r := New(
		mockHelper,
		mockHandler,
		nil,
		opts...,
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// Reconcile initially
	mtxn := &mockDatabase.Transaction{}
	mtxn.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn).Return(block, nil).Once()
	mtxn2 := &mockDatabase.Transaction{}
	mtxn2.On(
		"Discard",
		mock.Anything,
	).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn2).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn2,
		true,
		accountCurrency,
		"100",
		"100",
		block,
		block,
		true,
		InactiveReconciliation,
		nil,
		false,
		false,
	)

	// Force Rreconciliation eventhough not required
	mtxn3 := &mockDatabase.Transaction{}
	mtxn3.On("Discard", mock.Anything).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn3).Once()
	mockHelper.On("CurrentBlock", mock.Anything, mtxn3).Return(block, nil).Once()

	mtxn4 := &mockDatabase.Transaction{}
	mtxn4.On("Discard", mock.Anything).Run(
		func(args mock.Arguments) {
			cancel()
		},
	).Once()
	mockHelper.On("DatabaseTransaction", mock.Anything).Return(mtxn4).Once()
	mockHelper.On(
		"ForceInactiveReconciliation",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block,
	).Return(
		true,
	).Once()
	mockReconcilerCalls(
		mockHelper,
		mockHandler,
		mtxn4,
		true,
		accountCurrency,
		"100",
		"100",
		block,
		block,
		true,
		InactiveReconciliation,
		nil,
		false,
		false,
	)
	err := r.Reconcile(ctx)
	assert.Contains(t, context.Canceled.Error(), err.Error())
}
