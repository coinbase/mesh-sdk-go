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

package modules

import (
	"context"
	"math/big"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/utils"
)

func TestCounterStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	c := NewCounterStorage(database)

	t.Run("get unset counter", func(t *testing.T) {
		v, err := c.Get(ctx, "blah")
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(0))
	})

	t.Run("increase counter", func(t *testing.T) {
		v, err := c.Update(ctx, "blah", big.NewInt(100))
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(100))

		v, err = c.Get(ctx, "blah")
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(100))
	})

	t.Run("decrement counter", func(t *testing.T) {
		v, err := c.Update(ctx, "blah", big.NewInt(-50))
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(50))

		v, err = c.Get(ctx, "blah")
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(50))
	})

	t.Run("get unset counter after update", func(t *testing.T) {
		v, err := c.Get(ctx, "blah2")
		assert.NoError(t, err)
		assert.Equal(t, v, big.NewInt(0))
	})
}
