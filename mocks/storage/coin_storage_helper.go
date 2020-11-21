// Code generated by mockery v1.0.0. DO NOT EDIT.

package storage

import (
	context "context"

	storage "github.com/syscoin/rosetta-sdk-go/storage"
	mock "github.com/stretchr/testify/mock"

	types "github.com/syscoin/rosetta-sdk-go/types"
)

// CoinStorageHelper is an autogenerated mock type for the CoinStorageHelper type
type CoinStorageHelper struct {
	mock.Mock
}

// CurrentBlockIdentifier provides a mock function with given fields: _a0, _a1
func (_m *CoinStorageHelper) CurrentBlockIdentifier(_a0 context.Context, _a1 storage.DatabaseTransaction) (*types.BlockIdentifier, error) {
	ret := _m.Called(_a0, _a1)

	var r0 *types.BlockIdentifier
	if rf, ok := ret.Get(0).(func(context.Context, storage.DatabaseTransaction) *types.BlockIdentifier); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*types.BlockIdentifier)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, storage.DatabaseTransaction) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
