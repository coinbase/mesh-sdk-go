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

package parser

import (
	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// Parser provides support for parsing Rosetta blocks.
type Parser struct {
	Asserter          *asserter.Asserter
	ExemptFunc        ExemptOperation
	BalanceExemptions []*types.BalanceExemption
}

// New creates a new Parser.
func New(
	asserter *asserter.Asserter,
	exemptFunc ExemptOperation,
	balanceExemptions []*types.BalanceExemption,
) *Parser {
	return &Parser{
		Asserter:          asserter,
		ExemptFunc:        exemptFunc,
		BalanceExemptions: balanceExemptions,
	}
}
