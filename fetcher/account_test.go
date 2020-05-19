package fetcher

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/stretchr/testify/assert"
)

var (
	basicNetwork = &types.NetworkIdentifier{
		Blockchain: "blockchain",
		Network:    "network",
	}

	basicAccount = &types.AccountIdentifier{
		Address: "address",
	}

	basicBlock = &types.BlockIdentifier{
		Index: 10,
		Hash:  "block 10",
	}

	basicPartialBlock = &types.PartialBlockIdentifier{
		Index: &basicBlock.Index,
	}

	basicAmounts = []*types.Amount{
		{
			Value: "1000",
			Currency: &types.Currency{
				Symbol:   "BTC",
				Decimals: 8,
			},
		},
	}
)

func TestAccountBalanceRetry(t *testing.T) {
	var tests = map[string]struct {
		network *types.NetworkIdentifier
		account *types.AccountIdentifier
		block   *types.PartialBlockIdentifier

		errorsBeforeSuccess int
		expectedBlock       *types.BlockIdentifier
		expectedAmounts     []*types.Amount
		expectedMetadata    map[string]interface{}
		expectedError       error

		fetcherMaxRetries uint64
		shouldCancel      bool
	}{
		"single failure": {
			network:             basicNetwork,
			account:             basicAccount,
			block:               nil,
			errorsBeforeSuccess: 1,
			expectedBlock:       basicBlock,
			expectedAmounts:     basicAmounts,
			expectedError:       nil,
			fetcherMaxRetries:   5,
			shouldCancel:        false,
		},
	}
	for name, test := range tests {
		runs := 0
		t.Run(name, func(t *testing.T) {
			assert := assert.New(t)
			ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				assert.Equal("POST", r.Method)
				assert.Equal("/account/balance", r.URL.RequestURI())

				expected := &types.AccountBalanceRequest{
					NetworkIdentifier: test.network,
					AccountIdentifier: test.account,
					BlockIdentifier:   test.block,
				}
				var accountRequest *types.AccountBalanceRequest
				assert.NoError(json.NewDecoder(r.Body).Decode(&accountRequest))
				assert.Equal(expected, accountRequest)
				if runs < test.errorsBeforeSuccess {
					w.Header().Set("Content-Type", "application/json; charset=UTF-8")
					w.WriteHeader(http.StatusInternalServerError)
					fmt.Fprintln(w, "{}")
					runs++
					return
				}

				w.Header().Set("Content-Type", "application/json; charset=UTF-8")
				w.WriteHeader(http.StatusOK)
				fmt.Fprintln(w, types.PrettyPrintStruct(
					&types.AccountBalanceResponse{
						BlockIdentifier: test.expectedBlock,
						Balances:        test.expectedAmounts,
						Metadata:        test.expectedMetadata,
					},
				))
			}))

			defer ts.Close()

			f := New(ts.URL, WithRetryElapsedTime(time.Second), WithMaxRetries(test.fetcherMaxRetries))
			block, amounts, metadata, err := f.AccountBalanceRetry(
				context.Background(),
				test.network,
				test.account,
				test.block,
			)
			assert.Equal(test.expectedBlock, block)
			assert.Equal(test.expectedAmounts, amounts)
			assert.Equal(test.expectedMetadata, metadata)
			assert.Equal(test.expectedError, err)
		})
	}
}
