package dsl

import (
	"errors"
	"path"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"

	"github.com/stretchr/testify/assert"
)

func TestParse(t *testing.T) {
	tests := map[string]struct {
		file                string
		expectedWorkflows   []*job.Workflow
		expectedErr         error
		expectedErrLine     int
		expectedErrContents string
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
		"multiple workflows": {
			file: "multiple_workflow.ros",
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
									Input:      `{"symbol":"ETH", "decimals":18}`,
									OutputPath: "currency",
								},
								{
									Type:       job.SetVariable,
									Input:      `{"symbol":"ETH", "decimals":18}`,
									OutputPath: "currency_2",
								},
								{ // ensure we have some balance that exists
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
									Type:       job.Math,
									Input:      `{"operation": "subtraction","left_value": "0","right_value": "100"}`,
									OutputPath: "math_1",
								},
								{
									Type:       job.Math,
									Input:      `{"operation": "addition","left_value": "10","right_value": {{math_1}}}`,
									OutputPath: "math_2",
								},
								{
									Type:       job.Math,
									Input:      `{"operation": "subtraction","left_value": {{math_3}},"right_value": "20"}`,
									OutputPath: "math_3",
								},
								{
									Type:       job.FindBalance,
									Input:      `{"account_identifier": {{random_account.account_identifier}},"minimum_balance":{"value": "10000000000000000","currency": {{currency}}}}`, // nolint
									OutputPath: "loaded_account",
								},
							},
						},
					},
				},
				{
					Name:        string(job.CreateAccount),
					Concurrency: job.ReservedWorkflowConcurrency,
					Scenarios: []*job.Scenario{
						{
							Name: "create_account",
							Actions: []*job.Action{
								{
									Type:       job.SetVariable,
									Input:      `{"network":"Ropsten", "blockchain":"Ethereum"}`,
									OutputPath: "network",
								},
								{
									Type:       job.GenerateKey,
									Input:      `{"curve_type": "secp256k1"}`,
									OutputPath: "key",
								},
								{
									Type:       job.Derive,
									Input:      `{"network_identifier": {{network}},"public_key": {{key.public_key}}}`,
									OutputPath: "account",
								},
								{
									Type:  job.SaveAccount,
									Input: `{"account_identifier": {{account.account_identifier}},"keypair": {{key}}}`,
								},
							},
						},
					},
				},
			},
		},
		"workflow error: missing concurrency": {
			file:                "missing_concurrency.ros",
			expectedErr:         ErrParsingWorkflowConcurrency,
			expectedErrLine:     1,
			expectedErrContents: "request_funds{",
		},
		"workflow error: non-integer concurrency": {
			file:                "invalid_concurrency.ros",
			expectedErr:         ErrParsingWorkflowConcurrency,
			expectedErrLine:     1,
			expectedErrContents: "request_funds(hello){",
		},
		"workflow error: missing name": {
			file:                "missing_workflow_name.ros",
			expectedErr:         ErrParsingWorkflowName,
			expectedErrLine:     1,
			expectedErrContents: "(1){",
		},
		"workflow error: syntax error": {
			file:                "missing_workflow_bracket.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     1,
			expectedErrContents: "request_funds(1)",
		},
		"scenario error: missing name": {
			file:                "missing_scenario_name.ros",
			expectedErr:         ErrParsingScenarioName,
			expectedErrLine:     2,
			expectedErrContents: "{",
		},
		"scenario error: missing bracket": {
			file:                "missing_scenario_bracket.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     2,
			expectedErrContents: "find_account",
		},
		"scenario error: syntax error": {
			file:                "trailing_text_scenario.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     2,
			expectedErrContents: "find_account{hello",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fullPath := path.Join("testdata", test.file)
			workflows, err := Parse(fullPath)
			assert.Equal(t, test.expectedWorkflows, workflows)
			if test.expectedErr != nil {
				assert.NotNil(t, err)
				err.Log()
				assert.True(t, errors.Is(err.Err, test.expectedErr))
				assert.Equal(t, test.expectedErrLine, err.Line)
				assert.Equal(t, test.expectedErrContents, err.LineContents)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
