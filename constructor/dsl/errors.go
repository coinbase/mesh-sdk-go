package dsl

import (
	"errors"
	"fmt"

	"github.com/fatih/color"
)

type Error struct {
	Line         int
	LineContents string
	Err          error
}

func (e *Error) Log() {
	message := fmt.Sprintf("PARSING FAILED!\nMessage: %s\n\n", e.Err.Error())

	if e.Line > 0 {
		message = fmt.Sprintf("%sLine: %d\nLine Contents:%s\n\n", message, e.Line, e.LineContents)
	}

	color.Red(message)
}

var (
	ErrEOF     = errors.New("reached end of file")
	ErrScanner = errors.New("scanner error")

	ErrSyntax = errors.New("incorrect syntax")

	ErrParsingWorkflowName        = errors.New("cannot parse workflow name")
	ErrParsingWorkflowConcurrency = errors.New("cannot parse workflow concurrency")

	ErrParsingScenarioName = errors.New("cannot parse scenario name")
)
