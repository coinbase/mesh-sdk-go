package dsl

import (
	"context"
	"errors"
	"fmt"
	"os"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
)

// Parse loads a Rosetta constructor file and attempts
// to parse it into []*job.Workflow.
func Parse(ctx context.Context, file string) ([]*job.Workflow, *Error) {
	f, err := os.Open(file) // #nosec G304
	if err != nil {
		return nil, &Error{Err: fmt.Errorf("%w: %s", ErrCannotOpenFile, err)}
	}
	defer f.Close()

	p := newParser(f)
	workflows := []*job.Workflow{}
	for ctx.Err() == nil {
		workflow, err := p.parseWorkflow(ctx)
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

	return nil, &Error{Err: ctx.Err()}
}
