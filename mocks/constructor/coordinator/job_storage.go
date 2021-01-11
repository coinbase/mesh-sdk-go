// Code generated by mockery. DO NOT EDIT.

package coordinator

import (
	context "context"

	mock "github.com/stretchr/testify/mock"

	job "github.com/coinbase/rosetta-sdk-go/constructor/job"
	database "github.com/coinbase/rosetta-sdk-go/storage/database"
)

// JobStorage is an autogenerated mock type for the JobStorage type
type JobStorage struct {
	mock.Mock
}

// Broadcasting provides a mock function with given fields: _a0, _a1
func (_m *JobStorage) Broadcasting(_a0 context.Context, _a1 database.Transaction) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Get provides a mock function with given fields: _a0, _a1, _a2
func (_m *JobStorage) Get(_a0 context.Context, _a1 database.Transaction, _a2 string) (*job.Job, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 *job.Job
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction, string) *job.Job); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).(*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Processing provides a mock function with given fields: _a0, _a1, _a2
func (_m *JobStorage) Processing(_a0 context.Context, _a1 database.Transaction, _a2 string) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction, string) []*job.Job); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction, string) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Ready provides a mock function with given fields: _a0, _a1
func (_m *JobStorage) Ready(_a0 context.Context, _a1 database.Transaction) ([]*job.Job, error) {
	ret := _m.Called(_a0, _a1)

	var r0 []*job.Job
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction) []*job.Job); ok {
		r0 = rf(_a0, _a1)
	} else {
		if ret.Get(0) != nil {
			r0 = ret.Get(0).([]*job.Job)
		}
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction) error); ok {
		r1 = rf(_a0, _a1)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}

// Update provides a mock function with given fields: _a0, _a1, _a2
func (_m *JobStorage) Update(_a0 context.Context, _a1 database.Transaction, _a2 *job.Job) (string, error) {
	ret := _m.Called(_a0, _a1, _a2)

	var r0 string
	if rf, ok := ret.Get(0).(func(context.Context, database.Transaction, *job.Job) string); ok {
		r0 = rf(_a0, _a1, _a2)
	} else {
		r0 = ret.Get(0).(string)
	}

	var r1 error
	if rf, ok := ret.Get(1).(func(context.Context, database.Transaction, *job.Job) error); ok {
		r1 = rf(_a0, _a1, _a2)
	} else {
		r1 = ret.Error(1)
	}

	return r0, r1
}
