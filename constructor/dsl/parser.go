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
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

const (
	split2              = 2
	openBrakcet         = "{"
	closeBracket        = "}"
	openDoubleBracket   = "{{"
	closeDoubleBracket  = "}}"
	endScenarioContinue = "},"
	quote               = "\""
	equal               = "="
	add                 = "+"
	subtract            = "-"
	multiply            = "*"
	divide              = "/"
	openParens          = "("
	closeParens         = ")"
	endLine             = ";"
	functionEndLine     = ");"
	commentMarker       = "//"
	pathSeparator       = "."
)

type parser struct {
	scanner      *bufio.Scanner
	lineNumber   int
	lastLineRead string
}

func newParser(f *os.File) *parser {
	return &parser{scanner: bufio.NewScanner(f)}
}

func (p *parser) scannerError() error {
	return p.scanner.Err()
}

func rootOutputPath(outputPath string) string {
	tokens := strings.Split(outputPath, pathSeparator)

	return tokens[0]
}

func checkForVariables(
	ctx context.Context,
	variables map[string]struct{},
	input string,
) ([]string, error) {
	missingVariables := []string{}
	for ctx.Err() == nil {
		tokens := strings.SplitN(input, openDoubleBracket, split2)
		if len(tokens) != split2 {
			return missingVariables, nil
		}

		tokens = strings.SplitN(tokens[1], closeDoubleBracket, split2)
		if len(tokens) != split2 {
			return nil, fmt.Errorf("variable is missing }}: %w", ErrSyntax)
		}

		if _, ok := variables[rootOutputPath(tokens[0])]; !ok {
			missingVariables = append(missingVariables, tokens[0])
		}

		input = tokens[1]
	}

	return nil, ctx.Err()
}

func wrapValue(input string) string {
	if strings.HasPrefix(input, openDoubleBracket) {
		return input
	}

	if strings.HasPrefix(input, quote) {
		return input
	}

	return fmt.Sprintf(`"%s"`, input)
}

func parseActionType(line string) (job.ActionType, string, string, error) {
	var outputPath string

	tokens := strings.SplitN(line, equal, split2)
	var remaining string
	if len(tokens) == split2 {
		outputPath = strings.TrimSpace(tokens[0])
		remaining = tokens[1]
	} else {
		remaining = tokens[0]
	}

	remaining = strings.TrimSpace(remaining)
	tokens = strings.SplitN(remaining, openParens, split2)
	if len(tokens) == split2 {
		thisAction := job.ActionType(tokens[0])
		switch thisAction {
		case job.SetVariable:
			if len(outputPath) == 0 {
				return "", "", "", ErrCannotSetVariableWithoutOutput
			}

			return job.SetVariable, outputPath, tokens[1], nil
		case job.GenerateKey, job.Derive, job.SaveAccount, job.PrintMessage,
			job.RandomString, job.Math, job.FindBalance, job.RandomNumber, job.Assert,
			job.FindCurrencyAmount, job.LoadEnv, job.HTTPRequest, job.SetBlob,
			job.GetBlob:
			return thisAction, outputPath, tokens[1], nil
		default:
			return "", "", "", ErrInvalidActionType
		}
	}

	// Attempt to parse native Math
	for symbol, mathOperation := range map[string]job.MathOperation{
		add:      job.Addition,
		subtract: job.Subtraction,
		multiply: job.Multiplication,
		divide:   job.Division,
	} {
		tokens = strings.SplitN(remaining, symbol, split2)

		if json.Valid([]byte(strings.TrimSuffix(remaining, ";"))) {
			break
		}
		if len(tokens) == split2 {
			if len(mathOperation) == 0 {
				return "", "", "", ErrInvalidMathSymbol
			}

			syntheticOutput := fmt.Sprintf(
				`{"operation": "%s","left_value": %s,"right_value": %s};`,
				mathOperation,
				wrapValue(strings.TrimSpace(tokens[0])),
				wrapValue(strings.TrimSuffix(strings.TrimSpace(tokens[1]), endLine)),
			)

			return job.Math, outputPath, syntheticOutput, nil
		}
	}

	// Attempt to parse native SetVariable
	if len(outputPath) > 0 {
		return job.SetVariable, outputPath, tokens[0], nil
	}

	return "", "", "", ErrCannotSetVariableWithoutOutput
}

func (p *parser) parseAction(
	ctx context.Context,
	variables map[string]struct{},
	previousLine string,
) (*job.Action, error) {
	var actionType job.ActionType
	var input, outputPath string

	argLineRead := false
	for ctx.Err() == nil {
		var line string
		if len(previousLine) > 0 && !argLineRead {
			line = previousLine
			argLineRead = true
		} else {
			var err error
			line, err = p.readLine(ctx)
			if errors.Is(err, ErrEOF) {
				return nil, ErrUnexpectedEOF
			}
			if err != nil {
				return nil, fmt.Errorf("failed to read action: %w", err)
			}
		}

		// if no action type, at first line
		if len(actionType) == 0 {
			var err error
			actionType, outputPath, line, err = parseActionType(line)
			if err != nil {
				return nil, fmt.Errorf("failed to parse action type: %w", err)
			}
		}

		// Ensure all referenced variables exist
		missingVariables, err := checkForVariables(ctx, variables, line)
		if err != nil {
			return nil, err
		}

		if len(missingVariables) > 0 {
			return nil, fmt.Errorf(
				"the number of missing variables %v > 0: %w",
				missingVariables,
				ErrVariableUndefined,
			)
		}

		// Clean input if in a function or if using native syntax
		// (i.e. set_variable or math).
		input += strings.TrimSuffix(strings.TrimSuffix(line, functionEndLine), endLine)
		if strings.HasSuffix(line, endLine) {
			variables[rootOutputPath(outputPath)] = struct{}{}
			return &job.Action{
				Type:       actionType,
				Input:      input,
				OutputPath: outputPath,
			}, nil
		}
	}

	return nil, ctx.Err()
}

func parseScenarioName(line string) (string, error) {
	tokens := strings.SplitN(line, openBrakcet, split2)
	if len(tokens) != split2 {
		return "", fmt.Errorf("scenario entrypoint does not contain {: %w", ErrSyntax)
	}

	if len(tokens[0]) == 0 {
		return "", ErrParsingScenarioName
	}

	if len(tokens[1]) != 0 {
		return "", fmt.Errorf("scenario entrypoint ends with %s, not {: %w", tokens[1], ErrSyntax)
	}

	return tokens[0], nil
}

func (p *parser) parseScenario(
	ctx context.Context,
	scenarioNames map[string]struct{},
	variables map[string]struct{},
) (*job.Scenario, bool, error) {
	line, err := p.readLine(ctx)
	if errors.Is(err, ErrEOF) {
		return nil, false, ErrUnexpectedEOF
	}
	if err != nil {
		return nil, false, fmt.Errorf("failed to read scenario: %w", err)
	}

	name, err := parseScenarioName(line)
	if err != nil {
		return nil, false, fmt.Errorf("failed to parse scenario name: %w", err)
	}

	if _, ok := scenarioNames[name]; ok {
		return nil, false, fmt.Errorf(
			"scenario name %s is incorrect: %w",
			name,
			ErrDuplicateScenarioName,
		)
	}

	actions := []*job.Action{}
	for ctx.Err() == nil {
		line, err := p.readLine(ctx)
		if err != nil {
			return nil, false, fmt.Errorf("failed to read scenario: %w", err)
		}
		if line == closeBracket {
			return &job.Scenario{
				Name:    name,
				Actions: actions,
			}, false, nil
		}
		if line == endScenarioContinue {
			return &job.Scenario{
				Name:    name,
				Actions: actions,
			}, true, nil
		}

		action, err := p.parseAction(ctx, variables, line)
		if err != nil {
			return nil, false, fmt.Errorf("failed to parse action: %w", err)
		}

		actions = append(actions, action)
	}

	return nil, false, ctx.Err()
}

func parseWorkflowName(line string) (string, int, error) {
	var workflowName string
	var workflowConcurrency int
	var err error

	tokens := strings.SplitN(line, openParens, split2)
	if len(tokens) != split2 {
		return "", -1, ErrParsingWorkflowConcurrency
	}

	workflowName = strings.TrimSpace(tokens[0])
	if len(workflowName) == 0 {
		return "", -1, ErrParsingWorkflowName
	}

	tokens = strings.SplitN(tokens[1], closeParens, split2)
	if len(tokens) != split2 {
		return "", -1, ErrParsingWorkflowConcurrency
	}

	workflowConcurrency, err = strconv.Atoi(tokens[0])
	if err != nil {
		return "", -1, fmt.Errorf("failed to convert string %s to int: %w", tokens[0], err)
	}

	if tokens[1] != openBrakcet {
		return "", -1, fmt.Errorf(
			"workflow entrypoint ends with %s, not {: %w",
			tokens[1],
			ErrSyntax,
		)
	}

	return workflowName, workflowConcurrency, nil
}

func (p *parser) parseWorkflow(
	ctx context.Context,
	workflowNames map[string]struct{},
) (*job.Workflow, error) {
	line, err := p.readLine(ctx)
	if err != nil {
		return nil, err
	}

	name, concurrency, err := parseWorkflowName(line)
	if err != nil {
		return nil, fmt.Errorf("failed to parse workflow name: %w", err)
	}

	if _, ok := workflowNames[name]; ok {
		return nil, fmt.Errorf("workflow name %s is incorrect: %w", name, ErrDuplicateWorkflowName)
	}

	scenarios := []*job.Scenario{}
	variables := map[string]struct{}{}
	scenarioNames := map[string]struct{}{}
	for ctx.Err() == nil {
		scenario, cont, err := p.parseScenario(ctx, scenarioNames, variables)
		if err != nil {
			return nil, err
		}

		scenarioNames[scenario.Name] = struct{}{}
		scenarios = append(scenarios, scenario)

		// Allow following scenarios to call injected variables
		variables[scenario.Name] = struct{}{}

		// If a comma is detected, we continue parsing scenarios
		if cont {
			continue
		}

		line, err := p.readLine(ctx)
		if errors.Is(err, ErrEOF) {
			return nil, ErrUnexpectedEOF
		}
		if err != nil {
			return nil, fmt.Errorf("failed to parse scenario: %w", err)
		}

		if line != closeBracket {
			return nil, fmt.Errorf(
				"expected workflow to end with }, but got %s: %w",
				line,
				ErrSyntax,
			)
		}

		return &job.Workflow{
			Name:        name,
			Concurrency: concurrency,
			Scenarios:   scenarios,
		}, nil
	}

	return nil, ctx.Err()
}

func (p *parser) readLine(ctx context.Context) (string, error) {
	for ctx.Err() == nil {
		success := p.scanner.Scan()
		if success {
			p.lineNumber++
			trimmedLine := strings.TrimSpace(p.scanner.Text())
			p.lastLineRead = trimmedLine

			// Remove comments
			tokens := strings.Split(trimmedLine, commentMarker)
			trimmedLine = tokens[0]

			// Skip empty lines
			if len(trimmedLine) == 0 {
				continue
			}

			return trimmedLine, nil
		}

		if p.scannerError() != nil {
			return "", fmt.Errorf("%s: %w", p.scannerError().Error(), ErrScanner)
		}

		return "", ErrEOF
	}

	return "", ctx.Err()
}
