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
	"fmt"
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/storage/errors"
)

const (
	jobNamespace         = "job"
	jobMetadataNamespace = "job-metadata"

	readyKey        = "ready"
	broadcastingKey = "broadcasting"
	processingKey   = "processing"
	completedKey    = "completed"
	failedKey       = "failed"
)

func getJobKey(identifier string) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", jobNamespace, identifier),
	)
}

func getJobMetadataKey(metadata string) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", jobMetadataNamespace, metadata),
	)
}

func getJobProcessingKey(workflow string) string {
	return fmt.Sprintf("%s/%s", processingKey, workflow)
}

func getJobFailedKey(workflow string) string {
	return fmt.Sprintf("%s/%s", failedKey, workflow)
}

func getJobCompletedKey(workflow string) string {
	return fmt.Sprintf("%s/%s", completedKey, workflow)
}

// JobStorage implements storage methods for managing
// jobs.
type JobStorage struct {
	db database.Database
}

// NewJobStorage returns a new instance of *JobStorage.
func NewJobStorage(db database.Database) *JobStorage {
	return &JobStorage{db: db}
}

func (j *JobStorage) getAllJobs(
	ctx context.Context,
	dbTx database.Transaction,
	k []byte,
) ([]*job.Job, error) {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("unable to get all jobs: %w", err)
	}

	jobs := []*job.Job{}
	if !exists {
		return jobs, nil
	}

	var identifiers map[string]struct{}
	err = j.db.Encoder().Decode("", v, &identifiers, true)
	if err != nil {
		return nil, fmt.Errorf("unable to decode jobs: %w", err)
	}

	for identifier := range identifiers {
		v, err := j.Get(ctx, dbTx, identifier)
		if err != nil {
			return nil, fmt.Errorf("unable to get job %s: %w", identifier, err)
		}

		jobs = append(jobs, v)
	}

	return jobs, nil
}

// Ready returns all ready *job.Job.
func (j *JobStorage) Ready(ctx context.Context, dbTx database.Transaction) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(readyKey))
}

// Broadcasting returns all broadcasting *job.Job.
func (j *JobStorage) Broadcasting(
	ctx context.Context,
	dbTx database.Transaction,
) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(broadcastingKey))
}

// Processing gets all processing *job.Job of a certain workflow.
func (j *JobStorage) Processing(
	ctx context.Context,
	dbTx database.Transaction,
	workflow string,
) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobProcessingKey(workflow)))
}

// AllProcessing gets all processing *job.Jobs.
func (j *JobStorage) AllProcessing(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(processingKey))
}

// Failed returns all failed *job.Job of a certain workflow.
func (j *JobStorage) Failed(ctx context.Context, workflow string) ([]*job.Job, error) {
	dbTx := j.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobFailedKey(workflow)))
}

// AllFailed returns all failed *job.Jobs.
func (j *JobStorage) AllFailed(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(failedKey))
}

// Completed gets all successfully completed *job.Job of a certain workflow.
func (j *JobStorage) Completed(ctx context.Context, workflow string) ([]*job.Job, error) {
	dbTx := j.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobCompletedKey(workflow)))
}

// AllCompleted gets all successfully completed *job.Jobs.
func (j *JobStorage) AllCompleted(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.ReadTransaction(ctx)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(completedKey))
}

func (j *JobStorage) getNextIdentifier(
	ctx context.Context,
	dbTx database.Transaction,
) (string, error) {
	k := getJobMetadataKey("identifier")
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return "", fmt.Errorf("unable to get job: %w", err)
	}

	// Get existing identifier
	var nextIdentifier int
	if exists {
		err = j.db.Encoder().Decode("", v, &nextIdentifier, true)
		if err != nil {
			return "", fmt.Errorf("unable to decode job: %w", err)
		}
	} else {
		nextIdentifier = 0
	}

	// Increment and save
	encoded, err := j.db.Encoder().Encode("", nextIdentifier+1)
	if err != nil {
		return "", fmt.Errorf("unable to encode job: %w", err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return "", fmt.Errorf("unable to set job: %w", err)
	}

	return strconv.Itoa(nextIdentifier), nil
}

func (j *JobStorage) updateIdentifiers(
	ctx context.Context,
	dbTx database.Transaction,
	k []byte,
	identifiers map[string]struct{},
) error {
	encoded, err := j.db.Encoder().Encode("", identifiers)
	if err != nil {
		return fmt.Errorf("unable to encode identifier: %w", err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return fmt.Errorf("unable to set identifiers: %w", err)
	}

	return nil
}

func (j *JobStorage) addJob(
	ctx context.Context,
	dbTx database.Transaction,
	k []byte,
	identifier string,
) error {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("unable to add job: %w", err)
	}

	var identifiers map[string]struct{}
	if exists {
		err = j.db.Encoder().Decode("", v, &identifiers, true)
		if err != nil {
			return fmt.Errorf("unable to decode job: %w", err)
		}
	} else {
		identifiers = map[string]struct{}{}
	}

	identifiers[identifier] = struct{}{}

	return j.updateIdentifiers(ctx, dbTx, k, identifiers)
}

func (j *JobStorage) removeJob(
	ctx context.Context,
	dbTx database.Transaction,
	k []byte,
	identifier string,
) error {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("unable to get job: %w", err)
	}

	var identifiers map[string]struct{}
	if !exists {
		return errors.ErrJobDoesNotExist
	}

	err = j.db.Encoder().Decode("", v, &identifiers, true)
	if err != nil {
		return fmt.Errorf("unable to decode job: %w", err)
	}

	if _, ok := identifiers[identifier]; !ok {
		return errors.ErrJobIdentifierNotFound
	}

	delete(identifiers, identifier)

	return j.updateIdentifiers(ctx, dbTx, k, identifiers)
}

func getAssociatedKeys(j *job.Job) [][]byte {
	keys := [][]byte{}
	if j == nil {
		return keys
	}

	isProcessing := false
	switch j.Status {
	case job.Ready:
		keys = append(keys, getJobMetadataKey(readyKey))
		isProcessing = true
	case job.Completed:
		keys = append(keys, getJobMetadataKey(getJobCompletedKey(j.Workflow)))
		keys = append(keys, getJobMetadataKey(completedKey))
	case job.Failed:
		keys = append(keys, getJobMetadataKey(getJobFailedKey(j.Workflow)))
		keys = append(keys, getJobMetadataKey(failedKey))
	case job.Broadcasting:
		keys = append(keys, getJobMetadataKey(broadcastingKey))
		isProcessing = true
	}
	if isProcessing {
		keys = append(keys, getJobMetadataKey(getJobProcessingKey(j.Workflow)))
		keys = append(keys, getJobMetadataKey(processingKey))
	}

	return keys
}

func (j *JobStorage) updateMetadata(
	ctx context.Context,
	dbTx database.Transaction,
	oldJob *job.Job,
	newJob *job.Job,
) error {
	removedKeys := getAssociatedKeys(oldJob)
	for _, key := range removedKeys {
		if err := j.removeJob(ctx, dbTx, key, oldJob.Identifier); err != nil {
			return fmt.Errorf("unable to remove job: %w", err)
		}
	}

	addedKeys := getAssociatedKeys(newJob)
	for _, key := range addedKeys {
		if err := j.addJob(ctx, dbTx, key, newJob.Identifier); err != nil {
			return fmt.Errorf("unable to add job: %w", err)
		}
	}

	return nil
}

// Update overwrites an existing *job.Job or creates a new one (and assigns an identifier).
func (j *JobStorage) Update(
	ctx context.Context,
	dbTx database.Transaction,
	v *job.Job,
) (string, error) {
	var oldJob *job.Job
	if len(v.Identifier) == 0 {
		newIdentifier, err := j.getNextIdentifier(ctx, dbTx)
		if err != nil {
			return "", fmt.Errorf("unable to get next job: %w", err)
		}

		v.Identifier = newIdentifier
	} else {
		var err error
		oldJob, err = j.Get(ctx, dbTx, v.Identifier)
		if err != nil {
			return "", fmt.Errorf("unable to get job: %w", err)
		}
	}

	if oldJob != nil && (oldJob.Status == job.Completed || oldJob.Status == job.Failed) {
		return "", fmt.Errorf("job %s is invalid: %w", v.Identifier, errors.ErrJobUpdateOldFailed)
	}

	k := getJobKey(v.Identifier)
	encoded, err := j.db.Encoder().Encode("", v)
	if err != nil {
		return "", fmt.Errorf("unable to encode job: %w", err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return "", fmt.Errorf("unable to set job: %w", err)
	}

	if err := j.updateMetadata(ctx, dbTx, oldJob, v); err != nil {
		return "", fmt.Errorf("unable to update job metadata: %w", err)
	}

	return v.Identifier, nil
}

// Get returns a *job.Job by its identifier.
func (j *JobStorage) Get(
	ctx context.Context,
	dbTx database.Transaction,
	identifier string,
) (*job.Job, error) {
	k := getJobKey(identifier)

	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("unable to get job: %w", err)
	}
	if !exists {
		return nil, errors.ErrJobDoesNotExist
	}

	var output job.Job
	err = j.db.Encoder().Decode("", v, &output, true)
	if err != nil {
		return nil, fmt.Errorf("unable to decode job: %w", err)
	}

	return &output, nil
}
