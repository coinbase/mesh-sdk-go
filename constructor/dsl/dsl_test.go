// Copyright 2024 Coinbase, Inc.
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
	"fmt"
	"path"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
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
									Type:  job.SetBlob,
									Input: `{"key":"currency","value":{{currency}}}`,
								},
								{
									Type:       job.GetBlob,
									Input:      `{"key":"currency"}`,
									OutputPath: "fetched_currency",
								},
								{
									Type:       job.LoadEnv,
									Input:      `"MIN_BALANCE"`,
									OutputPath: "env",
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
									Type:  job.PrintMessage,
									Input: `{{find_account.suggested_fee}}`,
								},
								{
									Type:       job.SetVariable,
									Input:      `"hello"`,
									OutputPath: "varA",
								},
								{
									Type:       job.SetVariable,
									Input:      `10`,
									OutputPath: "varB",
								},
								{
									Type:       job.SetVariable,
									Input:      `[{"type":"transfer"}]`,
									OutputPath: "varC",
								},
								{
									Type:       job.SetVariable,
									Input:      `"10"`,
									OutputPath: "varD",
								},
								{
									Type:       job.SetVariable,
									Input:      `{{find_account}}`,
									OutputPath: "varE",
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
									Input:      `{"operation": "subtraction","left_value": {{math_2}},"right_value": "20"}`,
									OutputPath: "math_3",
								},
								{
									Type:       job.Math,
									Input:      `{"operation": "multiplication","left_value": {{math_3}},"right_value": "5"}`,
									OutputPath: "math_4",
								},
								{
									Type:       job.Math,
									Input:      `{"operation": "division","left_value": {{math_4}},"right_value": "5"}`,
									OutputPath: "math_5",
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
			expectedErr:         fmt.Errorf("failed to convert string hello to int"),
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
		"workflow error: duplicate worklow": {
			file:                "duplicate_workflow.ros",
			expectedErr:         ErrDuplicateWorkflowName,
			expectedErrLine:     33,
			expectedErrContents: "request_funds(1){",
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
		"scenario error: duplicate scenario": {
			file:                "duplicate_scenario.ros",
			expectedErr:         ErrDuplicateScenarioName,
			expectedErrLine:     14,
			expectedErrContents: "find_account{",
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
		"action error: unexpected end of input": {
			file:                "action_eof.ros",
			expectedErr:         ErrUnexpectedEOF,
			expectedErrLine:     8,
			expectedErrContents: `"minimum_balance":{`,
		},
		"action error: variable undefined": {
			file:                "action_variable_undefined.ros",
			expectedErr:         ErrVariableUndefined,
			expectedErrLine:     10,
			expectedErrContents: `"currency": {{currency2}}`,
		},
		"action error: variable incorrectly formatted": {
			file:                "action_variable_format.ros",
			expectedErr:         ErrSyntax,
			expectedErrLine:     12,
			expectedErrContents: `"currency": {{currency}`,
		},
		"file error: file does not exist": {
			file:        "blah_blah.ros",
			expectedErr: fmt.Errorf("no such file or directory"),
		},
		"file error: bad extension": {
			file:        "blah_blah.txt",
			expectedErr: ErrIncorrectExtension,
		},
		"action: valid type": {
			file: "action_valid.ros",
			expectedWorkflows: []*job.Workflow{
				{
					Name:        string(job.CreateAccount),
					Concurrency: job.ReservedWorkflowConcurrency,
					Scenarios: []*job.Scenario{
						{
							Name: "create",
							Actions: []*job.Action{
								{
									Type:       job.SetVariable,
									Input:      `{"network":"chrysalis-devnet", "blockchain":"iota"}`,
									OutputPath: "network",
								},
								{
									Type:       job.GenerateKey,
									Input:      `{"curve_type": "edwards25519"}`,
									OutputPath: "key",
								},
								{
									Type:       job.Derive,
									Input:      `{"network_identifier": {{network}},"public_key": {{key.public_key}}}`,
									OutputPath: "account",
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
			workflows, err := Parse(context.Background(), fullPath)
			assert.Equal(t, test.expectedWorkflows, workflows)
			if test.expectedErr != nil {
				assert.NotNil(t, err)
				err.Log()
				assert.Contains(t, err.Err.Error(), test.expectedErr.Error())
				assert.Equal(t, test.expectedErrLine, err.Line)
				assert.Equal(t, test.expectedErrContents, err.LineContents)
			} else {
				assert.Nil(t, err)
			}
		})
	}
}
