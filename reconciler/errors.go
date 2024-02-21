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

package reconciler

import (
	"errors"

	utils "github.com/coinbase/rosetta-sdk-go/errors"
)

// Named error types for Reconciler errors
var (
	// ErrHeadBlockBehindLive is returned when the processed
	// head is behind the live head. Sometimes, it is
	// preferable to sleep and wait to catch up when
	// we are close to the live head (waitToCheckDiff).
	ErrHeadBlockBehindLive = errors.New("head block behind")

	// ErrBlockGone is returned when the processed block
	// head is greater than the live head but the block
	// does not exist in the store. This likely means
	// that the block was orphaned.
	ErrBlockGone = errors.New("block gone")
)

// Err takes an error as an argument and returns
// whether or not the error is one thrown by the reconciler package
func Err(err error) bool {
	reconcilerErrors := []error{
		ErrHeadBlockBehindLive,
		ErrBlockGone,
	}

	return utils.FindError(reconcilerErrors, err)
}
