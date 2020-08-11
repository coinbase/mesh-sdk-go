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
			expected: New(nil, nil),
		},
		"with reconciler concurrency": {
			options: []Option{
				WithInactiveConcurrency(100),
				WithActiveConcurrency(200),
			},
			expected: func() *Reconciler {
				r := New(nil, nil)
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
				r := New(nil, nil)
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
				r := New(nil, nil)
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
				r := New(nil, nil)
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

func mockSuccessfulReconcilerCalls(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	lookupBalanceByBlock bool,
	accountCurrency *AccountCurrency,
	value string,
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
) {
	mockHelper.On("CurrentBlock", mock.Anything).Return(headBlock, nil).Once()
	lookupBlock := types.ConstructPartialBlockIdentifier(liveBlock)
	if !lookupBalanceByBlock {
		lookupBlock = nil
	}

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		lookupBlock,
	).Return(
		&types.Amount{Value: value, Currency: accountCurrency.Currency},
		headBlock,
		nil,
	).Once()
	mockHelper.On("BlockExists", mock.Anything, headBlock).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock,
	).Return(
		&types.Amount{Value: value, Currency: accountCurrency.Currency},
		liveBlock,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationSucceeded",
		mock.Anything,
		"ACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		value,
		headBlock,
	).Return(nil).Once()
}

func mockFailedReconcilerCalls(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	lookupBalanceByBlock bool,
	accountCurrency *AccountCurrency,
	liveValue string,
	computedValue string,
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
) {
	mockHelper.On("CurrentBlock", mock.Anything).Return(headBlock, nil).Once()
	lookupBlock := types.ConstructPartialBlockIdentifier(liveBlock)
	if !lookupBalanceByBlock {
		lookupBlock = nil
	}

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		lookupBlock,
	).Return(
		&types.Amount{Value: liveValue, Currency: accountCurrency.Currency},
		headBlock,
		nil,
	).Once()
	mockHelper.On("BlockExists", mock.Anything, headBlock).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock,
	).Return(
		&types.Amount{Value: computedValue, Currency: accountCurrency.Currency},
		liveBlock,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationFailed",
		mock.Anything,
		"ACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		computedValue,
		liveValue,
		headBlock,
	).Return(errors.New("reconciliation failed")).Once()
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
		accountCurrency2 = &AccountCurrency{
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
			r := New(
				mockHelper,
				mockHandler,
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
				WithInterestingAccounts([]*AccountCurrency{accountCurrency2}),
				WithLookupBalanceByBlock(lookup),
			)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			mockSuccessfulReconcilerCalls(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency,
				"100",
				block2,
				block,
			)

			mockSuccessfulReconcilerCalls(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency2,
				"250",
				block2,
				block,
			)

			mockSuccessfulReconcilerCalls(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency2,
				"120",
				block2,
				block2,
			)

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

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
		})
	}
}

func TestReconcile_HighWaterMark(t *testing.T) {
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
		accountCurrency2 = &AccountCurrency{
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
	r := New(
		mockHelper,
		mockHandler,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
		WithInterestingAccounts([]*AccountCurrency{accountCurrency2}),
		WithLookupBalanceByBlock(false),
		WithDebugLogging(true),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	// First call to QueueChanges
	mockHelper.On("CurrentBlock", mock.Anything).Return(block, nil).Once()
	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		(*types.PartialBlockIdentifier)(nil),
	).Return(
		&types.Amount{Value: "100", Currency: accountCurrency.Currency},
		block200,
		nil,
	).Once()

	// Second call to QueueChanges
	mockSuccessfulReconcilerCalls(
		mockHelper,
		mockHandler,
		false,
		accountCurrency,
		"150",
		block200,
		block200,
	)
	mockSuccessfulReconcilerCalls(
		mockHelper,
		mockHandler,
		false,
		accountCurrency2,
		"120",
		block200,
		block200,
	)

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
	err = r.QueueChanges(ctx, block200, []*parser.BalanceChange{
		{
			Account:  accountCurrency.Account,
			Currency: accountCurrency.Currency,
			Block:    block200,
		},
	})
	assert.NoError(t, err)

	time.Sleep(1 * time.Second)
	cancel()

	mockHelper.AssertExpectations(t)
	mockHandler.AssertExpectations(t)
}

func TestReconcile_Orphan(t *testing.T) {
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
	)

	mockHelper := &mocks.Helper{}
	mockHandler := &mocks.Handler{}
	r := New(
		mockHelper,
		mockHandler,
		WithActiveConcurrency(1),
		WithInactiveConcurrency(0),
	)
	ctx := context.Background()
	ctx, cancel := context.WithCancel(ctx)

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		types.ConstructPartialBlockIdentifier(block),
	).Return(
		nil,
		nil,
		errors.New("cannot find block"),
	).Once()
	mockHelper.On("BlockExists", mock.Anything, block).Return(false, nil).Once()

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

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			r := New(
				mockHelper,
				mockHandler,
				WithActiveConcurrency(1),
				WithInactiveConcurrency(0),
				WithLookupBalanceByBlock(lookup),
			)
			ctx := context.Background()

			mockFailedReconcilerCalls(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency,
				"105",
				"100",
				block2,
				block,
			)

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
		})
	}
}

func mockSuccessfulReconcilerCallsINACTIVE(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	lookupBalanceByBlock bool,
	accountCurrency *AccountCurrency,
	value string,
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
) {
	lookupBlock := types.ConstructPartialBlockIdentifier(liveBlock)
	if !lookupBalanceByBlock {
		lookupBlock = nil
	}

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		lookupBlock,
	).Return(
		&types.Amount{Value: value, Currency: accountCurrency.Currency},
		headBlock,
		nil,
	).Once()
	mockHelper.On("BlockExists", mock.Anything, headBlock).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock,
	).Return(
		&types.Amount{Value: value, Currency: accountCurrency.Currency},
		liveBlock,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationSucceeded",
		mock.Anything,
		"INACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		value,
		headBlock,
	).Return(nil).Once()
}

func mockFailureReconcilerCallsINACTIVE(
	mockHelper *mocks.Helper,
	mockHandler *mocks.Handler,
	lookupBalanceByBlock bool,
	accountCurrency *AccountCurrency,
	liveValue string,
	computedValue string,
	headBlock *types.BlockIdentifier,
	liveBlock *types.BlockIdentifier,
) {
	lookupBlock := types.ConstructPartialBlockIdentifier(liveBlock)
	if !lookupBalanceByBlock {
		lookupBlock = nil
	}

	mockHelper.On(
		"LiveBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		lookupBlock,
	).Return(
		&types.Amount{Value: liveValue, Currency: accountCurrency.Currency},
		headBlock,
		nil,
	).Once()
	mockHelper.On("BlockExists", mock.Anything, headBlock).Return(true, nil).Once()
	mockHelper.On(
		"ComputedBalance",
		mock.Anything,
		accountCurrency.Account,
		accountCurrency.Currency,
		headBlock,
	).Return(
		&types.Amount{Value: computedValue, Currency: accountCurrency.Currency},
		liveBlock,
		nil,
	).Once()
	mockHandler.On(
		"ReconciliationFailed",
		mock.Anything,
		"INACTIVE",
		accountCurrency.Account,
		accountCurrency.Currency,
		computedValue,
		liveValue,
		headBlock,
	).Return(errors.New("reconciliation failed")).Once()
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
		accountCurrency = &AccountCurrency{
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
			r := New(
				mockHelper,
				mockHandler,
				WithActiveConcurrency(0),
				WithInactiveConcurrency(1),
				WithSeenAccounts([]*AccountCurrency{accountCurrency}),
				WithLookupBalanceByBlock(lookup),
				WithInactiveFrequency(1),
				WithDebugLogging(true),
			)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			mockHelper.On("CurrentBlock", mock.Anything).Return(block, nil)
			mockSuccessfulReconcilerCallsINACTIVE(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency,
				"100",
				block,
				block,
			)

			go func() {
				time.Sleep(2 * time.Second)
				cancel()
			}()

			err := r.Reconcile(ctx)
			assert.Contains(t, context.Canceled.Error(), err.Error())

			mockHelper2 := &mocks.Helper{}
			mockHandler2 := &mocks.Handler{}
			r.helper = mockHelper2
			r.handler = mockHandler2
			ctx = context.Background()
			ctx, cancel = context.WithCancel(ctx)

			mockHelper2.On("CurrentBlock", mock.Anything).Return(block2, nil)
			mockSuccessfulReconcilerCallsINACTIVE(
				mockHelper2,
				mockHandler2,
				lookup,
				accountCurrency,
				"200",
				block2,
				block2,
			)

			go func() {
				time.Sleep(2 * time.Second)
				cancel()
			}()
			err = r.Reconcile(ctx)
			assert.Contains(t, context.Canceled.Error(), err.Error())

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
			mockHelper2.AssertExpectations(t)
			mockHandler2.AssertExpectations(t)
		})
	}
}

func TestReconcile_FailureOnlyInactive(t *testing.T) {
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
	)

	lookupBalanceByBlocks := []bool{true, false}
	for _, lookup := range lookupBalanceByBlocks {
		t.Run(fmt.Sprintf("lookup balance by block %t", lookup), func(t *testing.T) {
			mockHelper := &mocks.Helper{}
			mockHandler := &mocks.Handler{}
			r := New(
				mockHelper,
				mockHandler,
				WithActiveConcurrency(0),
				WithInactiveConcurrency(1),
				WithSeenAccounts([]*AccountCurrency{accountCurrency}),
				WithLookupBalanceByBlock(lookup),
				WithInactiveFrequency(1),
				WithDebugLogging(true),
			)
			ctx := context.Background()
			ctx, cancel := context.WithCancel(ctx)

			mockHelper.On("CurrentBlock", mock.Anything).Return(block, nil)
			mockFailureReconcilerCallsINACTIVE(
				mockHelper,
				mockHandler,
				lookup,
				accountCurrency,
				"100",
				"105",
				block,
				block,
			)

			go func() {
				time.Sleep(2 * time.Second)
				cancel()
			}()

			err := r.Reconcile(ctx)
			assert.Contains(t, "reconciliation failed", err.Error())

			mockHelper.AssertExpectations(t)
			mockHandler.AssertExpectations(t)
		})
	}
}
