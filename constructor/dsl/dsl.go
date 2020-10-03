package dsl

import (
	"bufio"
	"errors"
	"fmt"
	"os"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

type parser struct {
	scanner    *bufio.Scanner
	lineNumber int

	currentWorkflow *job.Workflow
	currentScenario *job.Scenario
}

func extractOutputPathAndType(line string) (job.ActionType, string, string, error) {
	var outputPath string

	tokens := strings.SplitN(line, "=", 1)
	var remaining string
	if len(tokens) == 2 {
		outputPath = strings.TrimSpace(tokens[0])
		remaining = tokens[1]
	} else {
		remaining = tokens[0]
	}

	remaining = strings.TrimSpace(remaining)
	tokens = strings.SplitN(line, "(", 1)
	if len(tokens) == 2 {
		thisAction := job.ActionType(tokens[0])
		switch thisAction {
		case job.GenerateKey, job.Derive, job.SaveAccount, job.PrintMessage,
			job.RandomString, job.Math, job.FindBalance, job.RandomNumber, job.Assert,
			job.FindCurrencyAmount, job.LoadEnv, job.HTTPRequest:
			return thisAction, outputPath, tokens[1], nil
		default:
			return "", "", "", errors.New("invalid action type")
		}
	}

	if strings.HasPrefix(tokens[0], "{") {
		if len(outputPath) > 0 {
			return job.SetVariable, outputPath, tokens[0], nil
		}

		return "", "", "", errors.New("unable to set variable with no output path")
	}

	return "", "", "", errors.New("parsing error")
}

func (p *parser) matchAction() (*job.Action, error) {
	var actionType job.ActionType
	var input, outputPath string
	for {
		line, err := p.readLine()
		if err != nil {
			return nil, fmt.Errorf("%w: action parsing failed")
		}

		// if no action type, at first line
		if len(actionType) == 0 {
			actionType, outputPath, line, err = extractOutputPathAndType(line)
			if err != nil {
				return nil, errors.New("unable to extract path and type")
			}
		}

		if actionType == job.SetVariable {
			input += strings.TrimSuffix(line, ";")
			if strings.HasSuffix(line, ";") {
				break
			}
		} else {
			input += strings.TrimSuffix(line, ");")
			if strings.HasSuffix(line, ");") {
				break
			}
		}
	}

	return &job.Action{
		Type:       actionType,
		Input:      input,
		OutputPath: outputPath,
	}, nil
}

func parseName(line string) (string, error) {
	tokens := strings.SplitN(line, "{", 1)
	if len(tokens) != 1 {
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
		return nil, false, errors.New("unable to parse scenario name")
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
		action, err := p.matchAction()
		if err != nil {
			return nil, false, errors.New("unable to parse action")
		}
		actions = append(actions, action)
	}
}

func parseNameConcurrency(line string) (string, int, error) {
}

func (p *parser) matchWorkflow() (*job.Workflow, error) {
	line, err := p.readLine()
	if err != nil {
		return nil, err
	}

	name, concurrency, err := parseNameConcurrency(line)
	if err != nil {
		return nil, errors.New("could not parse name concurrency")
	}

	scenarios := []*job.Scenario{}
	for {
		scenario, cont, err := p.matchScenario()
		if err != nil {
			return nil, err
		}

		scenarios = append(scenarios, scenario)
		if !cont {
			return &job.Workflow{
				Name:        name,
				Concurrency: concurrency,
				Scenarios:   scenarios,
			}, nil
		}
	}
}

func (p *parser) readLine() (string, error) {
	for {
		success := p.scanner.Scan()
		if success {
			p.lineNumber++
			trimmedLine := strings.TrimSpace(p.scanner.Text())

			// Skip comments
			if strings.HasPrefix(trimmedLine, "//") {
				continue
			}

			// Skip empty lines
			if len(trimmedLine) == 0 {
				continue
			}

			return trimmedLine, nil
		}

		if p.scanner.Err() != nil {
			return "", fmt.Errorf("%w: %s", ErrScanner, p.scanner.Err().Error())
		}

		return "", ErrEOF
	}
}

func LoadFile(file string) ([]*job.Workflow, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	p := &parser{scanner: bufio.NewScanner(f), lineNumber: 1}

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
