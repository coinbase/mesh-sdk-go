// Code generated by mockery v2.13.1. DO NOT EDIT.

package modules

import (
	big "math/big"

	asserter "github.com/coinbase/rosetta-sdk-go/asserter"

	context "context"

	database "github.com/coinbase/rosetta-sdk-go/storage/database"

	mock "github.com/stretchr/testify/mock"

	parser "github.com/coinbase/rosetta-sdk-go/parser"

	types "github.com/coinbase/rosetta-sdk-go/types"
)

// BalanceStorageHelper is an autogenerated mock type for the BalanceStorageHelper type
type BalanceStorageHelper struct {
	mock.Mock
}

// AccountBalance provides a mock function with given fields: ctx, account, currency, block
func (_m *BalanceStorageHelper) AccountBalance(ctx context.Context, account *types.AccountIdentifier, currency *types.Currency, block *types.BlockIdentifier) (*types.Amount, error) {
	ret := _m.Called(ctx, account, currency, block)

	var r0 *types.Amount
	if rf, ok := ret.Get(0).(func(context.Context, *types.AccountIdentifier, *types.Currency, *types.BlockIdentifier) *types.Amount); ok {
		r0 = rf(ctx, account, currency, block)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.Amount)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, *types.AccountIdentifier, *types.Currency, *types.BlockIdentifier) error); ok {
		r1 = rf(ctx, account, currency, block)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AccountsReconciled provides a mock function with given fields: ctx, dbTx
func (_m *BalanceStorageHelper) AccountsReconciled(ctx context.Context, dbTx database.Transaction) (*big.Int, error) {
	ret := _m.Called(ctx, dbTx)

	var r0 *big.Int
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction) *big.Int); ok {
		r0 = rf(ctx, dbTx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*big.Int)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction) error); ok {
		r1 = rf(ctx, dbTx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// AccountsSeen provides a mock function with given fields: ctx, dbTx
func (_m *BalanceStorageHelper) AccountsSeen(ctx context.Context, dbTx database.Transaction) (*big.Int, error) {
	ret := _m.Called(ctx, dbTx)

	var r0 *big.Int
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction) *big.Int); ok {
		r0 = rf(ctx, dbTx)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*big.Int)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction) error); ok {
		r1 = rf(ctx, dbTx)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Asserter provides a mock function with given fields:
func (_m *BalanceStorageHelper) Asserter() *asserter.Asserter {
	ret := _m.Called()

	var r0 *asserter.Asserter
	if rf, ok := ret.Get(0).(func() *asserter.Asserter); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*asserter.Asserter)
		}
	}

	return r0
}

// BalanceExemptions provides a mock function with given fields:
func (_m *BalanceStorageHelper) BalanceExemptions() []*types.BalanceExemption {
	ret := _m.Called()

	var r0 []*types.BalanceExemption
	if rf, ok := ret.Get(0).(func() []*types.BalanceExemption); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*types.BalanceExemption)
		}
	}

	return r0
}

// ExemptFunc provides a mock function with given fields:
func (_m *BalanceStorageHelper) ExemptFunc() parser.ExemptOperation {
	ret := _m.Called()

	var r0 parser.ExemptOperation
	if rf, ok := ret.Get(0).(func() parser.ExemptOperation); ok {
		r0 = rf()
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(parser.ExemptOperation)
		}
	}

	return r0
}

type mockConstructorTestingTNewBalanceStorageHelper interface {
	mock.TestingT
	Cleanup(func())
}

// NewBalanceStorageHelper creates a new instance of BalanceStorageHelper. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewBalanceStorageHelper(t mockConstructorTestingTNewBalanceStorageHelper) *BalanceStorageHelper {
	mock := &BalanceStorageHelper{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}
