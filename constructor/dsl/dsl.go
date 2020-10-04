package dsl

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/types"
)

type parser struct {
	scanner      *bufio.Scanner
	lineNumber   int
	lastLineRead string // used for error response
}

func extractOutputPathAndType(line string) (job.ActionType, string, string, error) {
	var outputPath string

	tokens := strings.SplitN(line, "=", 2)
	var remaining string
	if len(tokens) == 2 {
		outputPath = strings.TrimSpace(tokens[0])
		remaining = tokens[1]
	} else {
		remaining = tokens[0]
	}

	remaining = strings.TrimSpace(remaining)
	tokens = strings.SplitN(remaining, "(", 2)
	if len(tokens) == 2 {
		thisAction := job.ActionType(tokens[0])
		switch thisAction {
		case job.SetVariable, job.GenerateKey, job.Derive, job.SaveAccount, job.PrintMessage,
			job.RandomString, job.Math, job.FindBalance, job.RandomNumber, job.Assert,
			job.FindCurrencyAmount, job.LoadEnv, job.HTTPRequest:
			return thisAction, outputPath, tokens[1], nil
		default:
			return "", "", "", errors.New("invalid action type")
		}
	}

	// Attempt to parse Math
	tokens = strings.SplitN(remaining, "+", 2)
	if len(tokens) == 2 {
		return job.Math, outputPath, types.PrintStruct(&job.MathInput{
			Operation:  job.Addition,
			LeftValue:  strings.TrimSpace(tokens[0]),
			RightValue: strings.TrimSuffix(strings.TrimSpace(tokens[1]), ";"),
		}) + ";", nil
	}

	tokens = strings.SplitN(remaining, "-", 2)
	if len(tokens) == 2 {
		return job.Math, outputPath, types.PrintStruct(&job.MathInput{
			Operation:  job.Subtraction,
			LeftValue:  strings.TrimSpace(tokens[0]),
			RightValue: strings.TrimSuffix(strings.TrimSpace(tokens[1]), ";"),
		}) + ";", nil
	}

	// Attempt to parse SetVariable
	if strings.HasPrefix(tokens[0], "{") {
		if len(outputPath) > 0 {
			return job.SetVariable, outputPath, tokens[0], nil
		}

		return "", "", "", errors.New("unable to set variable with no output path")
	}

	return "", "", "", errors.New("parsing error")
}

func (p *parser) matchAction(previousLine string) (*job.Action, error) {
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
			actionType, outputPath, line, err = extractOutputPathAndType(line)
			if err != nil {
				return nil, fmt.Errorf("%w: unable to extract path and type", err)
			}
		}

		// Clean input if in a function or if using native syntax
		// (i.e. set_variable or math).
		input += strings.TrimSuffix(strings.TrimSuffix(line, ");"), ";")
		if strings.HasSuffix(line, ";") {
			break
		}
	}

	return &job.Action{
		Type:       actionType,
		Input:      input,
		OutputPath: outputPath,
	}, nil
}

func parseName(line string) (string, error) {
	tokens := strings.SplitN(line, "{", 2)
	if len(tokens) != 2 {
		return "", errors.New("parsing error")
	}

	if len(tokens[1]) != 0 {
		return "", errors.New("parsing error")
	}

	return tokens[0], nil
}

func (p *parser) matchScenario() (*job.Scenario, bool, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, false, errors.New("unexpected end of input")
	}
	name, err := parseName(line)
	if err != nil {
		return nil, false, fmt.Errorf("%w: unable to parse scenario name", err)
	}

	actions := []*job.Action{}
	for {
		line, err := p.readLine()
		if err != nil {
			return nil, false, fmt.Errorf("%w: scenario parsing failed", err)
		}
		if line == "}" {
			return &job.Scenario{
				Name:    name,
				Actions: actions,
			}, false, nil
		}
		if line == "}," {
			return &job.Scenario{
				Name:    name,
				Actions: actions,
			}, true, nil
		}

		action, err := p.matchAction(line)
		if err != nil {
			return nil, false, fmt.Errorf("%w: unable to parse action", err)
		}
		actions = append(actions, action)
	}
}

func parseNameConcurrency(line string) (string, int, error) {
	var workflowName string
	var workflowConcurrency int
	var err error

	tokens := strings.SplitN(line, "(", 2)
	if len(tokens) != 2 {
		return "", -1, errors.New("unable to read workflow name")
	}

	workflowName = strings.TrimSpace(tokens[0])

	tokens = strings.SplitN(tokens[1], ")", 2)
	if len(tokens) != 2 {
		return "", -1, errors.New("unable to read workflow concurrency")
	}

	workflowConcurrency, err = strconv.Atoi(tokens[0])
	if err != nil {
		return "", -1, fmt.Errorf("%w: workflow concurrency is not an integer", err)
	}

	if tokens[1] != "{" {
		return "", -1, fmt.Errorf("workflow entrypoint ends with %s, not {", tokens[1])
	}

	return workflowName, workflowConcurrency, nil
}

func (p *parser) matchWorkflow() (*job.Workflow, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	name, concurrency, err := parseNameConcurrency(line)
	if err != nil {
		return nil, fmt.Errorf("%w: could not parse name concurrency", err)
	}

	scenarios := []*job.Scenario{}
	for {
		scenario, cont, err := p.matchScenario()
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

		if line != "}" {
			return nil, fmt.Errorf("expected workflow to end with }, but got %s", line)
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
			tokens := strings.Split(trimmedLine, "//")
			trimmedLine = tokens[0]

			// Skip empty lines
			if len(trimmedLine) == 0 {
				continue
			}

			fmt.Println(trimmedLine)
			return trimmedLine, nil
		}

		if p.scanner.Err() != nil {
			return "", fmt.Errorf("%w: %s", ErrScanner, p.scanner.Err().Error())
		}

		return "", ErrEOF
	}
}

// LoadFile loads a file written in X.
func LoadFile(file string) ([]*job.Workflow, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	p := &parser{bufio.NewScanner(f), 1, ""}

	workflows := []*job.Workflow{}
	for {
		workflow, err := p.matchWorkflow()
		if errors.Is(err, ErrEOF) {
			return workflows, nil
		}
		if err != nil {
			return nil, err
		}

		workflows = append(workflows, workflow)
	}
}
