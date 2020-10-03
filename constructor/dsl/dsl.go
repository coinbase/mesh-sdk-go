package dsl

import (
	"bufio"
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

}

func (p *parser) matchAction() (*job.Action, error) {
	var actionType job.ActionType
	var input, outputPath string
	for {
		line := p.readLine()

		// if no action type, at first line
		if len(actionType) == 0 {
			var err error
			actionType, outputPath, line, err = extractOutputPathAndType(line)
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
