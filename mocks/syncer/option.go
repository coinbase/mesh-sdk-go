// Code generated by mockery v0.0.0-dev. DO NOT EDIT.

package syncer

import (
	syncer "github.com/coinbase/rosetta-sdk-go/syncer"
	mock "github.com/stretchr/testify/mock"
)

// Option is an autogenerated mock type for the Option type
type Option struct {
	mock.Mock
}

// Execute provides a mock function with given fields: s
func (_m *Option) Execute(s *syncer.Syncer) {
	_m.Called(s)
}
