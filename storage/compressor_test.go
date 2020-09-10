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

package storage

import (
	"context"
	"fmt"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
	"golang.org/x/sync/errgroup"
)

func runCompressions(c *Compressor, t *testing.T) {
	for i := int64(0); i < 500; i++ {
		b := &types.BlockIdentifier{
			Index: i,
			Hash:  fmt.Sprintf("block %d", i),
		}

		bEnc, err := c.Encode("", b)
		assert.NoError(t, err)

		tx := &types.Transaction{
			TransactionIdentifier: &types.TransactionIdentifier{Hash: fmt.Sprintf("tx %d", i)},
		}
		txEnc, err := c.Encode("", tx)
		assert.NoError(t, err)

		block := &types.Block{
			BlockIdentifier:       b,
			ParentBlockIdentifier: b,
			Transactions:          []*types.Transaction{tx},
		}
		blockEnc, err := c.Encode("", block)
		assert.NoError(t, err)

		var bDec types.BlockIdentifier
		assert.NoError(t, c.Decode("", bEnc, &bDec, true))
		assert.Equal(t, types.Hash(b), types.Hash(bDec))

		var txDec types.Transaction
		assert.NoError(t, c.Decode("", txEnc, &txDec, true))
		assert.Equal(t, types.Hash(tx), types.Hash(txDec))

		var blockDec types.Block
		assert.NoError(t, c.Decode("", blockEnc, &blockDec, true))
		assert.Equal(t, types.Hash(block), types.Hash(blockDec))
	}
}

func TestCompressor(t *testing.T) {
	c, err := NewCompressor(nil, NewBufferPool())
	assert.NoError(t, err)

	g, _ := errgroup.WithContext(context.Background())

	for i := 0; i < 10; i++ {
		g.Go(func() error {
			runCompressions(c, t)

			return nil
		})
	}

	assert.NoError(t, g.Wait())
}
