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

package fetcher

import (
	"context"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

func TestConstructionCombine(t *testing.T) {
	var tests = map[string]struct {
		network             *types.NetworkIdentifier
		unsignedTransaction string
		signatures          []*types.Signature

		expectedRequest *types.ConstructionCombineRequest

		response *types.ConstructionCombineResponse

		expectedNetworkTx string
		expectedErr       error
	}{
		"only populate address": {
			network:             basicNetwork,
			unsignedTransaction: "tx",
			signatures: []*types.Signature{
				{
					SigningPayload: &types.SigningPayload{
						Address:       &basicAccount.Address,
						SignatureType: types.Ed25519,
						Bytes:         []byte("blah"),
					},
				},
			},
			expectedRequest: &types.ConstructionCombineRequest{
				NetworkIdentifier:   basicNetwork,
				UnsignedTransaction: "tx",
				Signatures: []*types.Signature{
					{
						SigningPayload: &types.SigningPayload{
							Address:           &basicAccount.Address,
							AccountIdentifier: basicAccount,
							SignatureType:     types.Ed25519,
							Bytes:             []byte("blah"),
						},
					},
				},
			},
			response: &types.ConstructionCombineResponse{
				SignedTransaction: "signed_tx",
			},
			expectedNetworkTx: "signed_tx",
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			var (
				assert   = assert.New(t)
				ctx      = context.Background()
				endpoint = "/construction/combine"
			)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal(endpoint, r.URL.RequestURI())

				val, err := ioutil.ReadAll(r.Body)
				assert.NoError(err)
				assert.Equal(
					types.PrintStruct(test.expectedRequest),
					string(val[:len(val)-1]), // body ends with newline
				)

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(test.response))
			}))

			defer ts.Close()

			f := New(
				ts.URL,
			)
			networkTx, err := f.ConstructionCombine(
				ctx,
				test.network,
				test.unsignedTransaction,
				test.signatures,
			)
			assert.Equal(test.expectedNetworkTx, networkTx)
			assert.True(checkError(err, test.expectedErr))
		})
	}
}
