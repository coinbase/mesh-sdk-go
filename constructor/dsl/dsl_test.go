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

package dsl

import (
	"context"
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
		"workflow error: closing bracket syntax error": {
			file:                "workflow_closing_syntax.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     26,
			expectedErrContents: ")",
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
		"scenario error: improper continue": {
			file:                "scenario_improper_continue.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     17,
			expectedErrContents: "request{",
		},
		"scenario error: unexpected end of input": {
			file:                "scenario_eof.ros",
			expectedErr:         ErrUnexpectedEOF,
			expectedErrLine:     14,
			expectedErrContents: "},",
		},
		"scenario error: unexpected end of input 2": {
			file:                "scenario_eof_2.ros",
			expectedErr:         ErrUnexpectedEOF,
			expectedErrLine:     15,
			expectedErrContents: "}",
		},
		"action error: invalid type": {
			file:                "action_invalid_type.ros",
			expectedErr:         ErrInvalidActionType,
			expectedErrLine:     8,
			expectedErrContents: "random_account = cool_stuff({",
		},
		"action error: set without output 1": {
			file:                "action_invalid_set_1.ros",
			expectedErr:         ErrCannotSetVariableWithoutOutput,
			expectedErrLine:     3,
			expectedErrContents: "set_variable({",
		},
		"action error: set without output 2": {
			file:                "action_invalid_set_2.ros",
			expectedErr:         ErrCannotSetVariableWithoutOutput,
			expectedErrLine:     3,
			expectedErrContents: "{",
		},
		"action error: invalid math symbol": {
			file:                "action_invalid_math.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     8,
			expectedErrContents: "math = 1 * 10;",
		},
		"action error: unexpected end of input": {
			file:                "action_eof.ros",
			expectedErr:         ErrUnexpectedEOF,
			expectedErrLine:     8,
			expectedErrContents: `"minimum_balance":{`,
		},
		"file error: file does not exist": {
			file:        "blah_blah.ros",
			expectedErr: ErrCannotOpenFile,
		},
		"file error: bad extension": {
			file:        "blah_blah.txt",
			expectedErr: ErrIncorrectExtension,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			fullPath := path.Join("testdata", test.file)
			workflows, err := Parse(context.Background(), fullPath)
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
