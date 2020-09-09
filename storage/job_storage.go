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
	"strconv"

	"github.com/coinbase/rosetta-sdk-go/constructor/job"
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
	db Database
}

// NewJobStorage returns a new instance of *JobStorage.
func NewJobStorage(db Database) *JobStorage {
	return &JobStorage{db: db}
}

func (j *JobStorage) getAllJobs(
	ctx context.Context,
	dbTx DatabaseTransaction,
	k []byte,
) ([]*job.Job, error) {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("%w by %s: %v", ErrJobsGetAllFailed, string(k), err)
	}

	jobs := []*job.Job{}
	if !exists {
		return jobs, nil
	}

	var identifiers map[string]struct{}
	err = j.db.Compressor().Decode("", v, &identifiers, true)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJobIdentifierDecodeFailed, err)
	}

	for identifier := range identifiers {
		v, err := j.Get(ctx, dbTx, identifier)
		if err != nil {
			return nil, fmt.Errorf("%w %s: %v", ErrJobGetFailed, identifier, err)
		}

		jobs = append(jobs, v)
	}

	return jobs, nil
}

// Ready returns all ready *job.Job.
func (j *JobStorage) Ready(ctx context.Context, dbTx DatabaseTransaction) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(readyKey))
}

// Broadcasting returns all broadcasting *job.Job.
func (j *JobStorage) Broadcasting(
	ctx context.Context,
	dbTx DatabaseTransaction,
) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(broadcastingKey))
}

// Processing gets all processing *job.Job of a certain workflow.
func (j *JobStorage) Processing(
	ctx context.Context,
	dbTx DatabaseTransaction,
	workflow string,
) ([]*job.Job, error) {
	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobProcessingKey(workflow)))
}

// AllProcessing gets all processing *job.Jobs.
func (j *JobStorage) AllProcessing(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(processingKey))
}

// Failed returns all failed *job.Job of a certain workflow.
func (j *JobStorage) Failed(ctx context.Context, workflow string) ([]*job.Job, error) {
	dbTx := j.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobFailedKey(workflow)))
}

// AllFailed returns all failed *job.Jobs.
func (j *JobStorage) AllFailed(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(failedKey))
}

// Completed gets all successfully completed *job.Job of a certain workflow.
func (j *JobStorage) Completed(ctx context.Context, workflow string) ([]*job.Job, error) {
	dbTx := j.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(getJobCompletedKey(workflow)))
}

// AllCompleted gets all successfully completed *job.Jobs.
func (j *JobStorage) AllCompleted(ctx context.Context) ([]*job.Job, error) {
	dbTx := j.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return j.getAllJobs(ctx, dbTx, getJobMetadataKey(completedKey))
}

func (j *JobStorage) getNextIdentifier(
	ctx context.Context,
	dbTx DatabaseTransaction,
) (string, error) {
	k := getJobMetadataKey("identifier")
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobGetFailed, err)
	}

	// Get existing identifier
	var nextIdentifier int
	if exists {
		err = j.db.Compressor().Decode("", v, &nextIdentifier, true)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrJobIdentifierDecodeFailed, err)
		}
	} else {
		nextIdentifier = 0
	}

	// Increment and save
	encoded, err := j.db.Compressor().Encode("", nextIdentifier+1)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobIdentifierEncodeFailed, err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobIdentifierUpdateFailed, err)
	}

	return strconv.Itoa(nextIdentifier), nil
}

func (j *JobStorage) updateIdentifiers(
	ctx context.Context,
	dbTx DatabaseTransaction,
	k []byte,
	identifiers map[string]struct{},
) error {
	encoded, err := j.db.Compressor().Encode("", identifiers)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrJobIdentifiersEncodeAllFailed, err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return fmt.Errorf("%w: %v", ErrJobIdentifiersSetAllFailed, err)
	}

	return nil
}

func (j *JobStorage) addJob(
	ctx context.Context,
	dbTx DatabaseTransaction,
	k []byte,
	identifier string,
) error {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("%w by %s: %v", ErrJobsGetAllFailed, string(k), err)
	}

	var identifiers map[string]struct{}
	if exists {
		err = j.db.Compressor().Decode("", v, &identifiers, true)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrJobIdentifierDecodeFailed, err)
		}
	} else {
		identifiers = map[string]struct{}{}
	}

	identifiers[identifier] = struct{}{}

	return j.updateIdentifiers(ctx, dbTx, k, identifiers)
}

func (j *JobStorage) removeJob(
	ctx context.Context,
	dbTx DatabaseTransaction,
	k []byte,
	identifier string,
) error {
	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return fmt.Errorf("%w by %s: %v", ErrJobsGetAllFailed, string(k), err)
	}

	var identifiers map[string]struct{}
	if !exists {
		return fmt.Errorf("%w %s from %s", ErrJobIdentifierRemoveFailed, identifier, string(k))
	}

	err = j.db.Compressor().Decode("", v, &identifiers, true)
	if err != nil {
		return fmt.Errorf("%w: unable to decode existing identifier", err)
	}

	if _, ok := identifiers[identifier]; !ok {
		return fmt.Errorf("%w: %s is not in %s", ErrJobIdentifierNotFound, identifier, string(k))
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
	dbTx DatabaseTransaction,
	oldJob *job.Job,
	newJob *job.Job,
) error {
	removedKeys := getAssociatedKeys(oldJob)
	for _, key := range removedKeys {
		if err := j.removeJob(ctx, dbTx, key, oldJob.Identifier); err != nil {
			return fmt.Errorf("%w %s: %v", ErrJobRemoveFailed, oldJob.Identifier, err)
		}
	}

	addedKeys := getAssociatedKeys(newJob)
	for _, key := range addedKeys {
		if err := j.addJob(ctx, dbTx, key, newJob.Identifier); err != nil {
			return fmt.Errorf("%w %s: %v", ErrJobAddFailed, newJob.Identifier, err)
		}
	}

	return nil
}

// Update overwrites an existing *job.Job or creates a new one (and assigns an identifier).
func (j *JobStorage) Update(
	ctx context.Context,
	dbTx DatabaseTransaction,
	v *job.Job,
) (string, error) {
	var oldJob *job.Job
	if len(v.Identifier) == 0 {
		newIdentifier, err := j.getNextIdentifier(ctx, dbTx)
		if err != nil {
			return "", fmt.Errorf("%w: %v", ErrJobIdentifierGetFailed, err)
		}

		v.Identifier = newIdentifier
	} else {
		var err error
		oldJob, err = j.Get(ctx, dbTx, v.Identifier)
		if err != nil {
			return "", fmt.Errorf("%w %s: %v", ErrJobGetFailed, v.Identifier, err)
		}
	}

	if oldJob != nil && (oldJob.Status == job.Completed || oldJob.Status == job.Failed) {
		return "", fmt.Errorf("%w %s", ErrJobUpdateOldFailed, v.Identifier)
	}

	k := getJobKey(v.Identifier)
	encoded, err := j.db.Compressor().Encode("", v)
	if err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobEncodeFailed, err)
	}

	if err := dbTx.Set(ctx, k, encoded, true); err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobUpdateFailed, err)
	}

	if err := j.updateMetadata(ctx, dbTx, oldJob, v); err != nil {
		return "", fmt.Errorf("%w: %v", ErrJobMetadataUpdateFailed, err)
	}

	return v.Identifier, nil
}

// Get returns a *job.Job by its identifier.
func (j *JobStorage) Get(
	ctx context.Context,
	dbTx DatabaseTransaction,
	identifier string,
) (*job.Job, error) {
	k := getJobKey(identifier)

	exists, v, err := dbTx.Get(ctx, k)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJobGetFailed, err)
	}
	if !exists {
		return nil, fmt.Errorf("%w: %v", ErrJobDoesNotExist, err)
	}

	var output job.Job
	err = j.db.Compressor().Decode("", v, &output, true)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrJobDecodeFailed, err)
	}

	return &output, nil
}
