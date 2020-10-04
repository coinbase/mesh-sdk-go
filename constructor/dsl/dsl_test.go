package dsl

import (
	"errors"
	"path"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"

	"github.com/stretchr/testify/assert"
)

func TestLoadFile(t *testing.T) {
	tests := map[string]struct {
		file              string
		expectedWorkflows []*job.Workflow
		expectedErr       error
	}{
		"simple example": {
			file: "simple.ros",
			expectedWorkflows: []*job.Workflow{
				{
					Name:        string(job.RequestFunds),
					Concurrency: job.ReservedWorkflowConcurrency,
					Scenarios: []*job.Scenario{
						{
							Name: "find_account",
							Actions: []*job.Action{
								{
									Type:       job.SetVariable,
									Input:      `{"symbol":"ETH","decimals":18}`,
									OutputPath: "currency",
								},
								{
									Type:       job.FindBalance,
									Input:      `{"minimum_balance":{"value": "0","currency": {{currency}}},"create_limit":1}`, // nolint
									OutputPath: "random_account",
								},
							},
						},
						{
							Name: "request",
							Actions: []*job.Action{
								{
									Type:       job.FindBalance,
									Input:      `{"account_identifier": {{random_account.account_identifier}},"minimum_balance":{"value": "10000000000000000","currency": {{currency}}}}`, // nolint
									OutputPath: "loaded_account",
								},
							},
						},
					},
				},
			},
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fullPath := path.Join("testdata", test.file)
			workflows, err := LoadFile(fullPath)
			assert.Equal(t, test.expectedWorkflows, workflows)
			if test.expectedErr != nil {
				assert.True(t, errors.Is(err, test.expectedErr))
			} else {
				assert.NoError(t, err)
			}
		})
	}
}
