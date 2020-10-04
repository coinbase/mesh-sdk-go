package dsl

import (
	"errors"
	"fmt"

	"github.com/fatih/color"
)

// Parsing Errors
var (
	ErrCannotOpenFile = errors.New("file cannot be opened")
	ErrEOF            = errors.New("reached end of file")
	ErrScanner        = errors.New("scanner error")

	ErrUnexpectedEOF = errors.New("unexpected EOF")

	ErrSyntax = errors.New("incorrect syntax")

	ErrParsingWorkflowName        = errors.New("cannot parse workflow name")
	ErrParsingWorkflowConcurrency = errors.New("cannot parse workflow concurrency")

	ErrParsingScenarioName = errors.New("cannot parse scenario name")

	ErrInvalidActionType              = errors.New("invalid action type")
	ErrCannotSetVariableWithoutOutput = errors.New("cannot set variable without output path")
)

// Error contains a parsing error and context about
// where the error occurred in the file.
type Error struct {
	Line         int
	LineContents string
	Err          error
}

// Log prints the *Error to the console in red.
func (e *Error) Log() {
	message := fmt.Sprintf("CONSTRUCTION FILE PARSING FAILED!\nMessage: %s\n\n", e.Err.Error())

	if e.Line > 0 {
		message = fmt.Sprintf("%sLine: %d\nLine Contents:%s\n\n", message, e.Line, e.LineContents)
	}

	color.Red(message)
}
