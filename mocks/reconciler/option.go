// Code generated by mockery. DO NOT EDIT.

package reconciler

import (
	mock "github.com/stretchr/testify/mock"

	reconciler "github.com/coinbase/rosetta-sdk-go/reconciler"
)

// Option is an autogenerated mock type for the Option type
type Option struct {
	mock.Mock
}

// Execute provides a mock function with given fields: r
func (_m *Option) Execute(r *reconciler.Reconciler) {
	_m.Called(r)
}
