package storage

import (
	"context"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
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

	newJob := &job.Job{
		Workflow: "blah",
		Status:   job.Broadcasting,
	}
	t.Run("add job", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob)
		assert.NoError(t, err)
		assert.Equal(t, "0", jobIdentifier)

		retrievedJob, err := storage.Get(ctx, dbTx, jobIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newJob, retrievedJob)

		jobs, err := storage.Ready(ctx, dbTx)
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Broadcasting(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah2")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Complete(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob}, jobs)
	})

	newJob2 := &job.Job{
		Workflow: "blah2",
		Status:   job.Ready,
	}
	t.Run("add another job", func(t *testing.T) {
		dbTx := database.NewDatabaseTransaction(ctx, true)
		defer dbTx.Discard(ctx)

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob2)
		assert.NoError(t, err)
		assert.Equal(t, "1", jobIdentifier)

		retrievedJob, err := storage.Get(ctx, dbTx, jobIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newJob2, retrievedJob)

		jobs, err := storage.Ready(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		jobs, err = storage.Broadcasting(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah2")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Complete(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob2}, jobs)
	})
}
