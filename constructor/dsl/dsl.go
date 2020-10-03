package dsl

import (
	"bufio"
	"errors"
	"os"
	"regexp"
	"strings"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

type parser struct {
	scanner    *bufio.Scanner
	lineNumber int

	currentWorkflow *job.Workflow
	currentScenario *job.Scenario
}

// TODO: if //, skip line

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
	var err error
	for {
		line := p.readLine()

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

func parseName(line string) string {
}

func (p *parser) matchScenario() *job.Scenario {
	line := p.readLine()
	name := parseName(line)

	for {
		action := p.matchAction()
	}

}

func parseNameConcurrency(line string) (string, int) {
}

func (p *parser) matchWorkflow() *job.Workflow {
	line := p.readLine()
	name, concurrency := parseNameConcurrency(line)

	for {
		scenario := p.matchScenario()
	}
}

func (p *parser) readLine() string {
	p.scanner.Scan()
	p.lineNumber++
	return strings.TrimSpace(p.scanner.Text())
}

func LoadFile(file string) ([]*job.Workflow, error) {
	f, err := os.Open(file)

	p := &parser{bufio.NewScanner(f), 1}

	for {
		workflow := p.matchWorkflow()
	}
}
