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

package asserter

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

func TestNew(t *testing.T) {
	var (
		validNetwork = &types.NetworkIdentifier{
			Blockchain: "hello",
			Network:    "world",
		}

		validNetworkStatus = &types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: MinUnixEpoch + 1,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
		}

		validNetworkStatusSyncStatus = &types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: MinUnixEpoch + 1,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
			SyncStatus: &types.SyncStatus{
				CurrentIndex: types.Int64(100),
				Stage:        types.String("pre-sync"),
			},
		}

		invalidNetworkStatus = &types.NetworkStatusResponse{
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: MinUnixEpoch + 1,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
		}

		invalidNetworkStatusSyncStatus = &types.NetworkStatusResponse{
			GenesisBlockIdentifier: &types.BlockIdentifier{
				Index: 0,
				Hash:  "block 0",
			},
			CurrentBlockIdentifier: &types.BlockIdentifier{
				Index: 100,
				Hash:  "block 100",
			},
			CurrentBlockTimestamp: MinUnixEpoch + 1,
			Peers: []*types.Peer{
				{
					PeerID: "peer 1",
				},
			},
			SyncStatus: &types.SyncStatus{
				CurrentIndex: types.Int64(-100),
				Stage:        types.String("pre-sync"),
			},
		}

		validNetworkOptions = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
				HistoricalBalanceLookup: true,
			},
		}

		validNetworkOptionsWithStartIndex = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
				HistoricalBalanceLookup: true,
				TimestampStartIndex:     types.Int64(10),
			},
		}

		invalidNetworkOptions = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
			},
		}

		duplicateStatuses = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
					{
						Status:     "Success",
						Successful: false,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
			},
		}

		duplicateTypes = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
			},
		}

		negativeStartIndex = &types.NetworkOptionsResponse{
			Version: &types.Version{
				RosettaVersion: "1.4.0",
				NodeVersion:    "1.0",
			},
			Allow: &types.Allow{
				OperationStatuses: []*types.OperationStatus{
					{
						Status:     "Success",
						Successful: true,
					},
				},
				OperationTypes: []string{
					"Transfer",
				},
				Errors: []*types.Error{
					{
						Code:      1,
						Message:   "error",
						Retriable: true,
					},
				},
				HistoricalBalanceLookup: true,
				TimestampStartIndex:     types.Int64(-1),
			},
		}
	)

	var tests = map[string]struct {
		network            *types.NetworkIdentifier
		networkStatus      *types.NetworkStatusResponse
		networkOptions     *types.NetworkOptionsResponse
		validationFilePath string

		err          error
		skipLoadTest bool
	}{
		"valid responses": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     validNetworkOptions,
			validationFilePath: "",

			err: nil,
		},
		"valid responses (with sync status)": {
			network:            validNetwork,
			networkStatus:      validNetworkStatusSyncStatus,
			networkOptions:     validNetworkOptions,
			validationFilePath: "",

			err: nil,
		},
		"valid responses (with start index)": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     validNetworkOptionsWithStartIndex,
			validationFilePath: "",

			err: nil,
		},
		"invalid network status": {
			network:            validNetwork,
			networkStatus:      invalidNetworkStatus,
			networkOptions:     validNetworkOptions,
			validationFilePath: "",

			err: errors.New("BlockIdentifier is nil"),
		},
		"invalid network status (with SyncStatus)": {
			network:            validNetwork,
			networkStatus:      invalidNetworkStatusSyncStatus,
			networkOptions:     validNetworkOptions,
			validationFilePath: "",

			err:          errors.New("SyncStatus.CurrentIndex is negative"),
			skipLoadTest: true,
		},
		"invalid network options": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     invalidNetworkOptions,
			validationFilePath: "",

			err: errors.New("no Allow.OperationStatuses found"),
		},
		"duplicate operation statuses": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     duplicateStatuses,
			validationFilePath: "",

			err: errors.New("Allow.OperationStatuses contains a duplicate Success"),
		},
		"duplicate operation types": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     duplicateTypes,
			validationFilePath: "",

			err: errors.New("Allow.OperationTypes contains a duplicate Transfer"),
		},
		"invalid start index": {
			network:            validNetwork,
			networkStatus:      validNetworkStatus,
			networkOptions:     negativeStartIndex,
			validationFilePath: "",

			err: ErrTimestampStartIndexInvalid,
		},
	}

	for name, test := range tests {
		t.Run(fmt.Sprintf("%s with responses", name), func(t *testing.T) {
			asserter, err := NewClientWithResponses(
				test.network,
				test.networkStatus,
				test.networkOptions,
				test.validationFilePath,
			)

			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
				return
			}
			assert.NoError(t, err)

			assert.NotNil(t, asserter)
			configuration, err := asserter.ClientConfiguration()
			assert.NoError(t, err)
			assert.Equal(t, test.network, configuration.NetworkIdentifier)
			assert.Equal(
				t,
				test.networkStatus.GenesisBlockIdentifier,
				configuration.GenesisBlockIdentifier,
			)
			assert.ElementsMatch(
				t,
				test.networkOptions.Allow.OperationTypes,
				configuration.AllowedOperationTypes,
			)
			assert.ElementsMatch(
				t,
				test.networkOptions.Allow.OperationStatuses,
				configuration.AllowedOperationStatuses,
			)
			assert.ElementsMatch(t, test.networkOptions.Allow.Errors, configuration.AllowedErrors)
			if test.networkOptions.Allow.TimestampStartIndex != nil {
				assert.Equal(
					t,
					*test.networkOptions.Allow.TimestampStartIndex,
					configuration.AllowedTimestampStartIndex,
				)
			} else {
				assert.Equal(t, test.networkStatus.GenesisBlockIdentifier.Index+1, configuration.AllowedTimestampStartIndex)
			}
		})

		if test.skipLoadTest {
			continue
		}

		t.Run(fmt.Sprintf("%s with file", name), func(t *testing.T) {
			fileConfig := &Configuration{
				NetworkIdentifier:        test.network,
				GenesisBlockIdentifier:   test.networkStatus.GenesisBlockIdentifier,
				AllowedOperationTypes:    test.networkOptions.Allow.OperationTypes,
				AllowedOperationStatuses: test.networkOptions.Allow.OperationStatuses,
				AllowedErrors:            test.networkOptions.Allow.Errors,
			}
			if test.networkOptions.Allow.TimestampStartIndex != nil {
				fileConfig.AllowedTimestampStartIndex = *test.networkOptions.Allow.TimestampStartIndex
			} else if fileConfig.GenesisBlockIdentifier != nil {
				fileConfig.AllowedTimestampStartIndex = fileConfig.GenesisBlockIdentifier.Index + 1
			}

			tmpfile, err := ioutil.TempFile("", "test.json")
			assert.NoError(t, err)
			defer os.Remove(tmpfile.Name())

			file, err := json.MarshalIndent(fileConfig, "", " ")
			assert.NoError(t, err)

			_, err = tmpfile.Write(file)
			assert.NoError(t, err)
			assert.NoError(t, tmpfile.Close())

			asserter, err := NewClientWithFile(
				tmpfile.Name(),
			)

			if test.err != nil {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), test.err.Error())
				return
			}
			assert.NoError(t, err)

			assert.NotNil(t, asserter)
			configuration, err := asserter.ClientConfiguration()
			assert.NoError(t, err)
			assert.Equal(t, test.network, configuration.NetworkIdentifier)
			assert.Equal(
				t,
				test.networkStatus.GenesisBlockIdentifier,
				configuration.GenesisBlockIdentifier,
			)
			assert.ElementsMatch(
				t,
				test.networkOptions.Allow.OperationTypes,
				configuration.AllowedOperationTypes,
			)
			assert.ElementsMatch(
				t,
				test.networkOptions.Allow.OperationStatuses,
				configuration.AllowedOperationStatuses,
			)
			assert.ElementsMatch(t, test.networkOptions.Allow.Errors, configuration.AllowedErrors)
			if test.networkOptions.Allow.TimestampStartIndex != nil {
				assert.Equal(
					t,
					*test.networkOptions.Allow.TimestampStartIndex,
					configuration.AllowedTimestampStartIndex,
				)
			} else {
				assert.Equal(t, test.networkStatus.GenesisBlockIdentifier.Index+1, configuration.AllowedTimestampStartIndex)
			}
		})
	}

	t.Run("non-existent file", func(t *testing.T) {
		asserter, err := NewClientWithFile(
			"blah",
		)
		assert.Error(t, err)
		assert.Nil(t, asserter)
	})

	t.Run("file not formatted correctly", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test.json")
		assert.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		_, err = tmpfile.Write([]byte("blah"))
		assert.NoError(t, err)
		assert.NoError(t, tmpfile.Close())

		asserter, err := NewClientWithFile(
			tmpfile.Name(),
		)

		assert.Nil(t, asserter)
		assert.Error(t, err)
	})

	t.Run("default no validation file", func(t *testing.T) {
		asserter, err := NewClientWithResponses(
			validNetwork,
			validNetworkStatus,
			validNetworkOptions,
			"",
		)

		assert.NoError(t, err)
		assert.NotNil(t, asserter)
		assert.False(t, asserter.validations.Enabled)
	})

	t.Run("non existent validation file", func(t *testing.T) {
		asserter, err := NewClientWithResponses(
			validNetwork,
			validNetworkStatus,
			validNetworkOptions,
			"blah",
		)

		assert.Error(t, err)
		assert.Nil(t, asserter)
	})

	t.Run("wrong format of validation file", func(t *testing.T) {
		tmpfile, err := ioutil.TempFile("", "test.json")
		assert.NoError(t, err)
		defer os.Remove(tmpfile.Name())

		_, err = tmpfile.Write([]byte("blah"))
		assert.NoError(t, err)
		assert.NoError(t, tmpfile.Close())

		asserter, err := NewClientWithResponses(
			validNetwork,
			validNetworkStatus,
			validNetworkOptions,
			tmpfile.Name(),
		)

		assert.Error(t, err)
		assert.Nil(t, asserter)
	})
}
