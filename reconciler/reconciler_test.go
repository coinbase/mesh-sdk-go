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
	"testing"
	"time"

	mocks "github.com/coinbase/rosetta-sdk-go/mocks/reconciler"
	"github.com/coinbase/rosetta-sdk-go/parser"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/mock"
)

func TestNewReconciler(t *testing.T) {
	var (
		accountCurrency = &AccountCurrency{
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
			expected: templateReconciler(),
		},
		"with reconciler concurrency": {
			options: []Option{
				WithInactiveConcurrency(100),
				WithActiveConcurrency(200),
			},
			expected: func() *Reconciler {
				r := templateReconciler()
				r.inactiveConcurrency = 100
				r.activeConcurrency = 200

				return r
			}(),
		},
		"with interesting accounts": {
			options: []Option{
				WithInterestingAccounts([]*AccountCurrency{
					accountCurrency,
				}),
			},
			expected: func() *Reconciler {
				r := templateReconciler()
				r.interestingAccounts = []*AccountCurrency{
					accountCurrency,
				}

				return r
			}(),
		},
		"with seen accounts": {
			options: []Option{
				WithSeenAccounts([]*AccountCurrency{
					accountCurrency,
				}),
			},
			expected: func() *Reconciler {
				r := templateReconciler()
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
		"with lookupBalanceByBlock": {
			options: []Option{
				WithLookupBalanceByBlock(false),
			},
			expected: func() *Reconciler {
				r := templateReconciler()
				r.lookupBalanceByBlock = false
				r.changeQueue = make(chan *parser.BalanceChange, backlogThreshold)

				return r
			}(),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			result := New(nil, nil, test.options...)
			assert.ElementsMatch(t, test.expected.inactiveQueue, result.inactiveQueue)
			assert.Equal(t, test.expected.seenAccounts, result.seenAccounts)
			assert.ElementsMatch(t, test.expected.interestingAccounts, result.interestingAccounts)
			assert.Equal(t, test.expected.inactiveConcurrency, result.inactiveConcurrency)
			assert.Equal(t, test.expected.activeConcurrency, result.activeConcurrency)
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
	acctSlice := []*AccountCurrency{
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
	)

	mh.On("CurrentBlock", ctx).Return(nil, errors.New("no head block")).Once()
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

	mh.On("CurrentBlock", ctx).Return(block0, nil).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block1).Return(false, nil).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block0).Return(true, nil).Once()
	mh.On(
		"ComputedBalance",
		ctx,
		account1,
		amount1.Currency,
		block2,
	).Return(
		amount1,
		block2,
		nil,
	).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block1).Return(true, nil).Once()
	mh.On(
		"ComputedBalance",
		ctx,
		account1,
		amount1.Currency,
		block2,
	).Return(
		amount1,
		block1,
		nil,
	).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block2).Return(true, nil).Once()
	mh.On(
		"ComputedBalance",
		ctx,
		account1,
		currency1,
		block2,
	).Return(
		amount1,
		block1,
		nil,
	).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block2).Return(true, nil).Once()
	mh.On(
		"ComputedBalance",
		ctx,
		account1,
		currency1,
		block2,
	).Return(
		amount1,
		block1,
		nil,
	).Once()
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

	mh.On("CurrentBlock", ctx).Return(block2, nil).Once()
	mh.On("BlockExists", ctx, block2).Return(true, nil).Once()
	mh.On(
		"ComputedBalance",
		ctx,
		account2,
		currency1,
		block2,
	).Return(
		nil,
		nil,
		errors.New("account missing"),
	).Once()
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

	mh.AssertExpectations(t)
}

func assertContainsAllAccounts(t *testing.T, m map[string]struct{}, a []*AccountCurrency) {
	for _, account := range a {
		_, exists := m[types.Hash(account)]
		assert.True(t, exists)
	}
}

func TestInactiveAccountQueue(t *testing.T) {
	var (
		r     = New(nil, nil)
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &AccountCurrency{
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
		accountCurrency2 = &AccountCurrency{
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
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(t, r.seenAccounts, []*AccountCurrency{accountCurrency})
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
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*AccountCurrency{accountCurrency, accountCurrency2},
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
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*AccountCurrency{accountCurrency, accountCurrency2},
		)
		assert.ElementsMatch(t, r.inactiveQueue, []*InactiveEntry{})
	})

	t.Run("previous account in inactive reconciliation", func(t *testing.T) {
		err := r.inactiveAccountQueue(
			true,
			accountCurrency,
			block,
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*AccountCurrency{accountCurrency, accountCurrency2},
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
		)
		assert.Nil(t, err)
		assertContainsAllAccounts(
			t,
			r.seenAccounts,
			[]*AccountCurrency{accountCurrency, accountCurrency2},
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

func TestReconcile_SuccessOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &AccountCurrency{
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

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(mockHelper, mockHandler, WithActiveConcurrency(1), WithInactiveConcurrency(0))
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		types.ConstructPartialBlockIdentifier(block),
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		block,
		nil,
	).Once()
	mockHelper.On("CurrentBlock", mock.Anything).Return(block2, nil).Once()
	mockHelper.On("BlockExists", mock.Anything, block).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block2,
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		block,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationSucceeded",
		mock.Anything,
		"ACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		"100",
		block,
	).Return(nil).Once()

	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, context.Canceled.Error(), err.Error())
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
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_FailureOnlyActive(t *testing.T) {
	var (
		block = &types.BlockIdentifier{
			Hash:  "block 1",
			Index: 1,
		}
		accountCurrency = &AccountCurrency{
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

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(mockHelper, mockHandler, WithActiveConcurrency(1), WithInactiveConcurrency(0))
	ctx := context.Background()

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		types.ConstructPartialBlockIdentifier(block),
	).Return(
		&types.Amount{Value: "105", Currency: accountCurrency.Currency},
		block,
		nil,
	).Once()
	mockHelper.On("CurrentBlock", mock.Anything).Return(block2, nil).Once()
	mockHelper.On("BlockExists", mock.Anything, block).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		block2,
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		block,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationFailed",
		mock.Anything,
		"ACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		"100",
		"105",
		block,
	).Return(errors.New("reconciliation failed")).Once()

	go func() {
		err := r.Reconcile(ctx)
		assert.Contains(t, "reconciliation failed", err.Error())
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
}

func templateReconciler() *Reconciler {
	return New(&mocks.Helper{}, &mocks.Handler{})
}
