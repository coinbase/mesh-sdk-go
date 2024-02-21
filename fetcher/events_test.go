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

package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var (
	maxSequence = int64(100)
	basicEvents = []*types.BlockEvent{
		{
			BlockIdentifier: basicBlock,
			Type:            types.ADDED,
			Sequence:        1,
		},
	}
)

func TestEventsBlocksRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier

		errorsBeforeSuccess int
		expectedMaxSeq      int64
		expectedEvents      []*types.BlockEvent
		expectedError       error
		retriableError      bool
		non500Error         bool

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"no failures": {
			network:           basicNetwork,
			expectedMaxSeq:    maxSequence,
			expectedEvents:    basicEvents,
			fetcherMaxRetries: 5,
		},
		"retry failures": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedMaxSeq:      maxSequence,
			expectedEvents:      basicEvents,
			fetcherMaxRetries:   5,
			retriableError:      true,
		},
		"non-500 retry failures": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedMaxSeq:      maxSequence,
			expectedEvents:      basicEvents,
			fetcherMaxRetries:   5,
			non500Error:         true,
		},
		"non-retriable error": {
			network:             basicNetwork,
			expectedMaxSeq:      -1,
			errorsBeforeSuccess: 2,
			fetcherMaxRetries:   5,
			expectedError:       ErrRequestFailed,
		},
		"exhausted retries": {
			network:             basicNetwork,
			errorsBeforeSuccess: 2,
			expectedMaxSeq:      -1,
			expectedError:       ErrExhaustedRetries,
			fetcherMaxRetries:   1,
			retriableError:      true,
		},
		"cancel context": {
			network:             basicNetwork,
			expectedMaxSeq:      -1,
			errorsBeforeSuccess: 6,
			expectedError:       context.Canceled,
			fetcherMaxRetries:   5,
			shouldCancel:        true,
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				tries       = 0
				assert      = assert.New(t)
				ctx, cancel = context.WithCancel(context.Background())
				endpoint    = "/events/blocks"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				expected := &types.EventsBlocksRequest{
					NetworkIdentifier: test.network,
				}
				var req *types.EventsBlocksRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&req))
				assert.Equal(expected, req)

				if test.shouldCancel {
					cancel()
				}

				if tries < test.errorsBeforeSuccess {
					if test.non500Error {
						w.Header().Set("Content-Type", "html/text; charset=UTF-8")
						w.WriteHeader(http.StatusGatewayTimeout)
						fmt.Fprintln(w, "blah")
					} else {
						w.Header().Set("Content-Type", "application/json; charset=UTF-8")
						w.WriteHeader(http.StatusInternalServerError)
						fmt.Fprintln(w, types.PrettyPrintStruct(&types.Error{
							Retriable: test.retriableError,
						}))
					}
					tries++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.EventsBlocksResponse{
						MaxSequence: test.expectedMaxSeq,
						Events:      test.expectedEvents,
					},
				))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
				WithRetryElapsedTime(5*time.Second),
				WithMaxRetries(test.fetcherMaxRetries),
			)
			maxSeq, events, err := f.EventsBlocksRetry(
				ctx,
				test.network,
				nil,
				nil,
			)
			assert.Equal(test.expectedMaxSeq, maxSeq)
			assert.Equal(test.expectedEvents, events)
			assert.True(checkError(err, test.expectedError))
		})
	}
}
