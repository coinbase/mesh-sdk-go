// Code generated by mockery v2.13.1. DO NOT EDIT.

package database

import (
	mock "github.com/stretchr/testify/mock"

	database "github.com/coinbase/rosetta-sdk-go/storage/database"
)

// BadgerOption is an autogenerated mock type for the BadgerOption type
type BadgerOption struct {
	mock.Mock
}

// Execute provides a mock function with given fields: b
func (_m *BadgerOption) Execute(b *database.BadgerDatabase) {
	_m.Called(b)
}

type mockConstructorTestingTNewBadgerOption interface {
	mock.TestingT
	Cleanup(func())
}

// NewBadgerOption creates a new instance of BadgerOption. It also registers a testing interface on the mock and a cleanup function to assert the mocks expectations.
func NewBadgerOption(t mockConstructorTestingTNewBadgerOption) *BadgerOption {
	mock := &BadgerOption{}
	mock.Mock.Test(t)

	t.Cleanup(func() { mock.AssertExpectations(t) })

	return mock
}