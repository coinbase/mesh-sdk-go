package errs

import "errors"

var (
	ErrCoinIsNil = errors.New("coin cannot be nil")

	ErrCoinIdentifierIsNil = errors.New("coin identifier cannot be nil")

	ErrCoinIdentifierNotSet = errors.New("coin identifier cannot be empty")

	ErrCoinChangeIsNil = errors.New("coin change cannot be nil")
)
