package asserter

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// Error ensures a *types.Error matches some error
// provided in `/network/options`. To ensure we don't
// silence errors on failure, we always include the *types.Error
// in the error message.
func (a *Asserter) Error(
	err *types.Error,
) error {
	if a == nil {
		return fmt.Errorf("%w: error: %s", ErrAsserterNotInitialized, types.PrintStruct(err))
	}

	if err := Error(err); err != nil {
		return fmt.Errorf("%w: error: %s", err, types.PrintStruct(err))
	}

	val, ok := a.errorTypeMap[err.Code]
	if !ok {
		return fmt.Errorf(
			"%w: code %d error: %s",
			ErrErrorUnexpectedCode,
			err.Code,
			types.PrintStruct(err),
		)
	}

	if val.Message != err.Message {
		return fmt.Errorf(
			"%w: expected %s actual %s error: %s",
			ErrErrorMessageMismatch,
			val.Message,
			err.Message,
			types.PrintStruct(err),
		)
	}

	if val.Retriable != err.Retriable {
		return fmt.Errorf(
			"%w: expected %s actual %s error: %s",
			ErrErrorRetriableMismatch,
			val.Message,
			err.Message,
			types.PrintStruct(err),
		)
	}

	return nil
}
