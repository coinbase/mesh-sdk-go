package asserter

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Error ensures a *types.Error matches some error
// provided in `/network/options`.
func (a *Asserter) Error(
	err *types.Error,
) error {
	if a == nil {
		return ErrAsserterNotInitialized
	}

	if err := Error(err); err != nil {
		return err
	}

	val, ok := a.errorTypeMap[err.Code]
	if !ok {
		return fmt.Errorf(
			"%w: code %d",
			ErrErrorUnexpectedCode,
			err.Code,
		)
	}

	if val.Message != err.Message {
		return fmt.Errorf(
			"%w: expected %s actual %s",
			ErrErrorMessageMismatch,
			val.Message,
			err.Message,
		)
	}

	if val.Retriable != err.Retriable {
		return fmt.Errorf(
			"%w: expected %s actual %s",
			ErrErrorRetriableMismatch,
			val.Message,
			err.Message,
		)
	}

	return nil
}
