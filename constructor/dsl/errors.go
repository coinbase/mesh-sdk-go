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
	"errors"
	"fmt"

	"github.com/fatih/color"
)

// DSL Errors
var (
	ErrIncorrectExtension = errors.New("expected .ros file extension")

	ErrEOF     = errors.New("reached end of file")
	ErrScanner = errors.New("scanner error")

	ErrUnexpectedEOF = errors.New("unexpected EOF")

	ErrSyntax = errors.New("incorrect syntax")

	ErrParsingWorkflowName        = errors.New("cannot parse workflow name")
	ErrParsingWorkflowConcurrency = errors.New("cannot parse workflow concurrency")
	ErrDuplicateWorkflowName      = errors.New("duplicate workflow name")

	ErrParsingScenarioName   = errors.New("cannot parse scenario name")
	ErrDuplicateScenarioName = errors.New("duplicate scenario name")

	ErrInvalidActionType              = errors.New("invalid action type")
	ErrCannotSetVariableWithoutOutput = errors.New("cannot set variable without output path")
	ErrVariableUndefined              = errors.New("variable undefined")
	ErrInvalidMathSymbol              = errors.New("invalid math symbol")
)

// Error contains a parsing error and context about
// where the error occurred in the file.
type Error struct {
	Line         int    `json:"line"`
	LineContents string `json:"line_contents"`
	Err          error  `json:"err"`
}

// Log prints the *Error to the console in red.
func (e *Error) Log() {
	message := fmt.Sprintf("CONSTRUCTION FILE PARSING FAILED!\nMessage: %s\n\n", e.Err.Error())

	if e.Line > 0 {
		message = fmt.Sprintf("%sLine: %d\nLine Contents:%s\n\n", message, e.Line, e.LineContents)
	}

	color.Red(message)
}
