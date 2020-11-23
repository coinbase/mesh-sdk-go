// Copyright 2020 Coinbase, Inc.
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
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"path"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

const (
	// RosettaFileExtension is the file extension for all constructor files.
	RosettaFileExtension = ".ros"
)

// Parse loads a Rosetta constructor file and attempts
// to parse it into []*job.Workflow.
func Parse(ctx context.Context, file string) ([]*job.Workflow, *Error) {
	cleanedPath := path.Clean(file)
	fileExtension := path.Ext(cleanedPath)
	if fileExtension != RosettaFileExtension {
		return nil, &Error{
			Err: fmt.Errorf(
				"%w: expected %s, got %s",
				ErrIncorrectExtension,
				RosettaFileExtension,
				fileExtension,
			),
		}
	}

	f, err := os.Open(cleanedPath) // #nosec G304
	if err != nil {
		return nil, &Error{Err: fmt.Errorf("%w (%s): %s", ErrCannotOpenFile, cleanedPath, err)}
	}
	defer func() {
		if err := f.Close(); err != nil {
			log.Printf("%s: could not close %s\n", err.Error(), cleanedPath)
		}
	}()

	p := newParser(f)
	workflows := []*job.Workflow{}
	workflowNames := map[string]struct{}{}
	for ctx.Err() == nil {
		workflow, err := p.parseWorkflow(ctx, workflowNames)
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

		workflowNames[workflow.Name] = struct{}{}
		workflows = append(workflows, workflow)
	}

	return nil, &Error{Err: ctx.Err()}
}
