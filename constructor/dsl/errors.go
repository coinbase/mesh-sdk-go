package dsl

import (
	"errors"
)

type Error struct {
	Line int
	Err  error
}

var (
	ErrEOF     = errors.New("reached end of file")
	ErrScanner = errors.New("scanner error")
)
