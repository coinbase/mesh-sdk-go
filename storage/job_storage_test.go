package storage

import (
	"context"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/stretchr/testify/assert"
)

func TestJobStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := NewBadgerStorage(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewJobStorage(database)

	t.Run("get non-existent job", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, false)
		defer dbTx.Discard(ctx)

		job, err := storage.Get(ctx, dbTx, "job1")
		assert.Error(t, err)
		assert.Nil(t, job)

		jobs, err := storage.Ready(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Broadcasting(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Processing(ctx, dbTx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Complete(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)
	})
}
