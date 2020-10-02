package dsl

import (
	"bufio"
	"os"
	"regexp"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

type parser struct {
	scanner    *bufio.Scanner
	lineNumber int

	currentWorkflow *job.Workflow
	currentScenario *job.Scenario
}

func (p *parser) matchAction() *job.Action {
	var actionType, input, outputPath string
	var checkedActionType, checkedInput, checkedOutputPath bool
	for {
		line := p.readLine()
		if len(outputPath) == 0 && !checkedOutputPath {
		}
	}
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
	return p.scanner.Text()
}

func LoadFile(file string) ([]*job.Workflow, error) {
	f, err := os.Open(file)

	p := &parser{bufio.NewScanner(f), 1}

	for {
		workflow := p.matchWorkflow()
	}
}
