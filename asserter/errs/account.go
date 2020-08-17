package errs

import "errors"

var (
	ErrReturnedBlockHashMismatch = errors.New(
		"request block hash does not match response block hash",
	)

	ErrReturnedBlockIndexMismatch = errors.New(
		"request block index does not match response block index",
	)
)
