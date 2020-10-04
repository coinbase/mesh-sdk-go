package dsl

import (
	"bufio"
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
	openDoubleBracket   = "{{"
	closeBracket        = "}"
	endScenarioContinue = "},"
	quote               = "\""
	equal               = "="
	add                 = "+"
	subtract            = "-"
	openParens          = "("
	closeParens         = ")"
	endLine             = ";"
	functionEndLine     = ");"
	commentMarker       = "//"
)

type parser struct {
	scanner      *bufio.Scanner
	lineNumber   int
	lastLineRead string
}

func (p *parser) scannerError() error {
	return p.scanner.Err()
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
			job.FindCurrencyAmount, job.LoadEnv, job.HTTPRequest:
			return thisAction, outputPath, tokens[1], nil
		default:
			return "", "", "", ErrInvalidActionType
		}
	}

	// Attempt to parse native Math
	for symbol, mathOperation := range map[string]job.MathOperation{
		add:      job.Addition,
		subtract: job.Subtraction,
	} {
		tokens = strings.SplitN(remaining, symbol, split2)
		if len(tokens) == split2 {
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
	if strings.HasPrefix(tokens[0], openBrakcet) {
		if len(outputPath) > 0 {
			return job.SetVariable, outputPath, tokens[0], nil
		}

		return "", "", "", ErrCannotSetVariableWithoutOutput
	}

	return "", "", "", ErrSyntax
}

func (p *parser) parseAction(previousLine string) (*job.Action, error) {
	var actionType job.ActionType
	var input, outputPath string

	argLineRead := false
	for {
		var line string
		if len(previousLine) > 0 && !argLineRead {
			line = previousLine
			argLineRead = true
		} else {
			var err error
			line, err = p.readLine()
			if err != nil {
				return nil, fmt.Errorf("%w: action parsing failed", err)
			}
		}

		// if no action type, at first line
		if len(actionType) == 0 {
			var err error
			actionType, outputPath, line, err = parseActionType(line)
			if err != nil {
				return nil, fmt.Errorf("%w: action type parsing failed", err)
			}
		}

		// Clean input if in a function or if using native syntax
		// (i.e. set_variable or math).
		input += strings.TrimSuffix(strings.TrimSuffix(line, functionEndLine), endLine)
		if strings.HasSuffix(line, endLine) {
			break
		}
	}

	return &job.Action{
		Type:       actionType,
		Input:      input,
		OutputPath: outputPath,
	}, nil
}

func parseScenarioName(line string) (string, error) {
	tokens := strings.SplitN(line, openBrakcet, split2)
	if len(tokens) != split2 {
		return "", fmt.Errorf("%w: scenario entrypoint does not contain {", ErrSyntax)
	}

	if len(tokens[0]) == 0 {
		return "", ErrParsingScenarioName
	}

	if len(tokens[1]) != 0 {
		return "", fmt.Errorf("%w: scenario entrypoint ends with %s, not {", ErrSyntax, tokens[1])
	}

	return tokens[0], nil
}

func (p *parser) parseScenario() (*job.Scenario, bool, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, false, errors.New("unexpected end of input")
	}
	name, err := parseScenarioName(line)
	if err != nil {
		return nil, false, fmt.Errorf("%w: unable to parse scenario name", err)
	}

	actions := []*job.Action{}
	for {
		line, err := p.readLine()
		if err != nil {
			return nil, false, fmt.Errorf("%w: scenario parsing failed", err)
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

		action, err := p.parseAction(line)
		if err != nil {
			return nil, false, fmt.Errorf("%w: unable to parse action", err)
		}

		actions = append(actions, action)
	}
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
		return "", -1, fmt.Errorf("%w: %s", ErrParsingWorkflowConcurrency, err.Error())
	}

	if tokens[1] != openBrakcet {
		return "", -1, fmt.Errorf("%w: workflow entrypoint ends with %s, not {", ErrSyntax, tokens[1])
	}

	return workflowName, workflowConcurrency, nil
}

func (p *parser) parseWorkflow() (*job.Workflow, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	name, concurrency, err := parseWorkflowName(line)
	if err != nil {
		return nil, fmt.Errorf("%w: could not parse workflow name", err)
	}

	scenarios := []*job.Scenario{}
	for {
		scenario, cont, err := p.parseScenario()
		if err != nil {
			return nil, err
		}

		scenarios = append(scenarios, scenario)
		if cont {
			continue
		}

		line, err := p.readLine()
		if err != nil {
			return nil, err
		}

		if line != closeBracket {
			return nil, fmt.Errorf("%w: expected workflow to end with }, but got %s", ErrSyntax, line)
		}

		return &job.Workflow{
			Name:        name,
			Concurrency: concurrency,
			Scenarios:   scenarios,
		}, nil
	}
}

func (p *parser) readLine() (string, error) {
	for {
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
			return "", fmt.Errorf("%w: %s", ErrScanner, p.scannerError().Error())
		}

		return "", ErrEOF
	}
}

// Parse loads a Rosetta constructor file and attempts
// to parse it into []*job.Workflow.
func Parse(file string) ([]*job.Workflow, *Error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, &Error{Err: err}
	}
	defer f.Close()

	p := &parser{scanner: bufio.NewScanner(f)}

	workflows := []*job.Workflow{}
	for {
		workflow, err := p.parseWorkflow()
		if errors.Is(err, ErrEOF) {
			return workflows, nil
		}
		if err != nil {
			return nil, &Error{
				Err:          err,
				Line:         p.lineNumber,
				LineContents: p.lastLineRead,
			}
		}

		workflows = append(workflows, workflow)
	}
}