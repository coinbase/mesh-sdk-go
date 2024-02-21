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
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func TestJobStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	storage := NewJobStorage(database)

	t.Run("get non-existent job", func(t *testing.T) {
		dbTx := database.ReadTransaction(ctx)
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

		jobs, err = storage.Completed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)
	})

	newJob := &job.Job{
		Workflow: "blah",
		Status:   job.Broadcasting,
	}
	t.Run("add job", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
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

		jobs, err = storage.Completed(ctx, "blah")
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
		dbTx := database.Transaction(ctx)
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

		jobs, err = storage.Completed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob2}, jobs)
	})

	newJob3 := &job.Job{
		Workflow: "blah",
		Status:   job.Completed,
	}
	t.Run("add another job", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		defer dbTx.Discard(ctx)

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob3)
		assert.NoError(t, err)
		assert.Equal(t, "2", jobIdentifier)

		retrievedJob, err := storage.Get(ctx, dbTx, jobIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newJob3, retrievedJob)

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

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Completed(ctx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob3}, jobs)

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob2}, jobs)

		jobs, err = storage.AllCompleted(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob3}, jobs)
	})

	t.Run("update job 1", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		defer dbTx.Discard(ctx)

		newJob.Status = job.Completed
		newJob.Identifier = "0"

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob)
		assert.NoError(t, err)
		assert.Equal(t, "0", jobIdentifier)

		retrievedJob, err := storage.Get(ctx, dbTx, jobIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newJob, retrievedJob)

		jobs, err := storage.Ready(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		jobs, err = storage.Broadcasting(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah2")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Completed(ctx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob3}, jobs)

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		jobs, err = storage.AllCompleted(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob3}, jobs)
	})

	t.Run("fail job 2", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		defer dbTx.Discard(ctx)

		newJob2.Status = job.Failed
		newJob2.Identifier = "1"

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob2)
		assert.NoError(t, err)
		assert.Equal(t, "1", jobIdentifier)

		retrievedJob, err := storage.Get(ctx, dbTx, jobIdentifier)
		assert.NoError(t, err)
		assert.Equal(t, newJob2, retrievedJob)

		jobs, err := storage.Ready(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.Broadcasting(ctx, dbTx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.Processing(ctx, dbTx, "blah2")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		assert.NoError(t, dbTx.Commit(ctx))

		jobs, err = storage.Failed(ctx, "blah")
		assert.NoError(t, err)
		assert.Len(t, jobs, 0)

		jobs, err = storage.Completed(ctx, "blah")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob3}, jobs)

		jobs, err = storage.Failed(ctx, "blah2")
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)

		jobs, err = storage.AllProcessing(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{}, jobs)

		jobs, err = storage.AllCompleted(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob, newJob3}, jobs)

		jobs, err = storage.AllFailed(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*job.Job{newJob2}, jobs)
	})

	t.Run("attempt to update job 2", func(t *testing.T) {
		dbTx := database.Transaction(ctx)
		defer dbTx.Discard(ctx)

		newJob2.Status = job.Completed
		newJob.Identifier = "1"

		jobIdentifier, err := storage.Update(ctx, dbTx, newJob2)
		assert.Error(t, err)
		assert.Equal(t, "", jobIdentifier)
	})
}
