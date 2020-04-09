package asserter

import (
	"errors"
	"testing"

	"github.com/coinbase/rosetta-sdk-go/models"

	"github.com/stretchr/testify/assert"
)

var (
	validNetworkIdentifier = &models.NetworkIdentifier{
		Blockchain: "Bitcoin",
		Network:    "Mainnet",
	}

	validAccountIdentifier = &models.AccountIdentifier{
		Address: "acct1",
	}

	validBlockIndex             = int64(1000)
	validPartialBlockIdentifier = &models.PartialBlockIdentifier{
		Index: &validBlockIndex,
	}

	validBlockIdentifier = &models.BlockIdentifier{
		Index: validBlockIndex,
		Hash:  "block 1",
	}

	validTransactionIdentifier = &models.TransactionIdentifier{
		Hash: "tx1",
	}

	validMethod = "transfer"
)

func TestAccountBalanceRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.AccountBalanceRequest
		err     error
	}{
		"valid request": {
			request: &models.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("AccountBalanceRequest is nil"),
		},
		"missing network": {
			request: &models.AccountBalanceRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing account": {
			request: &models.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("Account is nil"),
		},
		"valid historical request": {
			request: &models.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			err: nil,
		},
		"invalid historical request": {
			request: &models.AccountBalanceRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				BlockIdentifier:   &models.PartialBlockIdentifier{},
			},
			err: errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := AccountBalanceRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestBlockRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.BlockRequest
		err     error
	}{
		"valid request": {
			request: &models.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   validPartialBlockIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("BlockRequest is nil"),
		},
		"missing network": {
			request: &models.BlockRequest{
				BlockIdentifier: validPartialBlockIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing block identifier": {
			request: &models.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("PartialBlockIdentifier is nil"),
		},
		"invalid PartialBlockIdentifier request": {
			request: &models.BlockRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   &models.PartialBlockIdentifier{},
			},
			err: errors.New("neither PartialBlockIdentifier.Hash nor PartialBlockIdentifier.Index is set"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := BlockRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestBlockTransactionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.BlockTransactionRequest
		err     error
	}{
		"valid request": {
			request: &models.BlockTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("BlockTransactionRequest is nil"),
		},
		"missing network": {
			request: &models.BlockTransactionRequest{
				BlockIdentifier:       validBlockIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing block identifier": {
			request: &models.BlockTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("BlockIdentifier is nil"),
		},
		"invalid BlockIdentifier request": {
			request: &models.BlockTransactionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				BlockIdentifier:   &models.BlockIdentifier{},
			},
			err: errors.New("BlockIdentifier.Hash is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := BlockTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestTransactionConstructionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.TransactionConstructionRequest
		err     error
	}{
		"valid request": {
			request: &models.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
			},
			err: nil,
		},
		"valid request with method": {
			request: &models.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
				AccountIdentifier: validAccountIdentifier,
				Method:            &validMethod,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("TransactionConstructionRequest is nil"),
		},
		"missing network": {
			request: &models.TransactionConstructionRequest{
				AccountIdentifier: validAccountIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"missing account identifier": {
			request: &models.TransactionConstructionRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: errors.New("Account is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionConstructionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestTransactionSubmitRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.TransactionSubmitRequest
		err     error
	}{
		"valid request": {
			request: &models.TransactionSubmitRequest{
				SignedTransaction: "tx",
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("TransactionSubmitRequest is nil"),
		},
		"empty tx": {
			request: &models.TransactionSubmitRequest{},
			err:     errors.New("TransactionSubmitRequest.SignedTransaction is empty"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := TransactionSubmitRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestMempoolRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.MempoolRequest
		err     error
	}{
		"valid request": {
			request: &models.MempoolRequest{
				NetworkIdentifier: validNetworkIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("MempoolRequest is nil"),
		},
		"empty tx": {
			request: &models.MempoolRequest{},
			err:     errors.New("NetworkIdentifier is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := MempoolRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestMempoolTransactionRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.MempoolTransactionRequest
		err     error
	}{
		"valid request": {
			request: &models.MempoolTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("MempoolTransactionRequest is nil"),
		},
		"missing network": {
			request: &models.MempoolTransactionRequest{
				TransactionIdentifier: validTransactionIdentifier,
			},
			err: errors.New("NetworkIdentifier is nil"),
		},
		"invalid TransactionIdentifier request": {
			request: &models.MempoolTransactionRequest{
				NetworkIdentifier:     validNetworkIdentifier,
				TransactionIdentifier: &models.TransactionIdentifier{},
			},
			err: errors.New("TransactionIdentifier.Hash is missing"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := MempoolTransactionRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}

func TestNetworkStatusRequest(t *testing.T) {
	var tests = map[string]struct {
		request *models.NetworkStatusRequest
		err     error
	}{
		"valid request": {
			request: &models.NetworkStatusRequest{},
			err:     nil,
		},
		"nil request": {
			request: nil,
			err:     errors.New("NetworkStatusRequest is nil"),
		},
	}

	for name, test := range tests {
		t.Run(name, func(t *testing.T) {
			err := NetworkStatusRequest(test.request)
			assert.Equal(t, test.err, err)
		})
	}
}
