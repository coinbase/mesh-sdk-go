// Copyright 2022 Coinbase, Inc.
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

package keys

import (
	"encoding/json"
	"fmt"
	"testing"

	"github.com/coinbase/kryptology/pkg/signatures/schnorr/mina"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/suite"

	"github.com/coinbase/rosetta-sdk-go/types"
)

var signerPallas Signer
var keypair *KeyPair
var txnBytes []byte

type ts struct {
	suite.Suite
}

func TestSuite(t *testing.T) {
	suite.Run(t, new(ts))
}

func init() {
	keypair, _ = ImportPrivateKey(
		"A80F3DE13EE5AE01119E7D98A8F2317070BFB6D2A1EA712EE1B55EE7B938AD1D",
		"pallas",
	)
	signerPallas, _ = keypair.Signer()

	unsignedTxStr := "{\"randomOracleInput\":\"000000033769356015133A338518173BE9C263D6E463538ACDF11D523" +
		"DDEB8C82467093E3769356015133A338518173BE9C263D6E463538ACDF11D523DDEB8C82467093E167031AAE689272378D" +
		"05042083C66C593EF025060E4C8CA1CBD022E47C72D220000025701154880000000008000000000000000400000007FFFFFFF" +
		"C0000000000000000000000000000000000000000000000000000000000000000000060000000000000001BC6CD9C400000000\"," +
		"\"signerInput\":{\"prefix\":[\"3769356015133A338518173BE9C263D6E463538ACDF11D523DDEB8C82467093E\"," +
		"\"3769356015133A338518173BE9C263D6E463538ACDF11D523DDEB8C82467093E\"," +
		"\"167031AAE689272378D05042083C66C593EF025060E4C8CA1CBD022E47C72D22\"]," +
		"\"suffix\":[\"0000000000000007FFFFFFFC0000000400000000000000020000000002255100\"," +
		"\"0000000003000000000000000000000000000000000000000000000000000000\"," +
		"\"000000000000000000000000000000000000000000000000047366C7B0000000\"]}," +
		"\"payment\":{\"to\":\"B62qoLLD2LK2pL2dq2oDHh6ohdaYusgTEYRUZ43Y41Kk9Rgen4v643x\"," +
		"\"from\":\"B62qooQQ952uaoUSTQP3sZCviGmsWeusBwhg3qVF1Ww662sgzimA25Q\",\"fee\":\"18000000\"," +
		"\"token\":\"1\",\"nonce\":\"1\",\"memo\":null,\"amount\":\"2389498102\",\"valid_until\":\"4294967295\"}," +
		"\"stakeDelegation\":null,\"createToken\":null,\"createTokenAccount\":null,\"mintTokens\":null}"
	txnBytes = []byte(unsignedTxStr)
}

func (s *ts) TestParseSigningPayload() {
	// Also used in CSS, for parity testing
	fromAddress := "B62qkuFDYD82nxNNgGm1aJcSAErLZgAb19A9skSPtAxbBHsUZxsMbhU"
	toAddress := "B62qo7Ddbw8SXo55bTH6yJAASgQ6owtMYSw5tkuPJJ6GLJ36zvUnEpG"

	toPublicKey := &mina.PublicKey{}
	_ = toPublicKey.ParseAddress(toAddress)

	fromPublicKey := &mina.PublicKey{}
	_ = fromPublicKey.ParseAddress(fromAddress)

	var (
		amount                    = "34"
		validUntil                = "78"
		memo                      = "memo"
		signingPayloadWithPayment = SigningPayload{
			Payment: &PayloadFieldsPayment{
				To:         toAddress,
				From:       fromAddress,
				Fee:        "12",
				Amount:     &amount,
				Nonce:      "56",
				ValidUntil: &validUntil,
				Memo:       &memo,
			},
		}
		signingPayloadWithNoPaymentOrDelegation                           = SigningPayload{}
		signingPayloadWithPaymentAndNullValidUntilAndNullMemo = SigningPayload{
			Payment: &PayloadFieldsPayment{
				To:         toAddress,
				From:       fromAddress,
				Fee:        "12",
				Amount:     &amount,
				Nonce:      "56",
				ValidUntil: nil,
				Memo:       nil,
			},
		}
		signingPayloadWithPaymentAndInvalidFromPublicKey = SigningPayload{
			Payment: &PayloadFieldsPayment{
				To:         toAddress,
				From:       "InvalidFrom",
				Fee:        "12",
				Nonce:      "56",
				ValidUntil: &validUntil,
				Memo:       &memo,
			},
		}
		signingPayloadWithPaymentAndInvalidToPublicKey = SigningPayload{
			Payment: &PayloadFieldsPayment{
				To:         "InvalidTo",
				From:       fromAddress,
				Fee:        "12",
				Nonce:      "56",
				ValidUntil: &validUntil,
				Memo:       &memo,
			},
		}
	)

	s.Run("successful to parse when payment exists", func() {
		payloadBinary, _ := json.Marshal(signingPayloadWithPayment)
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             payloadBinary,
			SignatureType:     types.SchnorrPoseidon,
		}
		transaction, err := ParseSigningPayload(payload)
		s.Require().NoError(err)
		transactionBinary, err := transaction.MarshalBinary()
		s.Require().NoError(err)

		expectedTransaction := mina.Transaction{
			Fee:        12,
			FeeToken:   1,
			FeePayerPk: fromPublicKey,
			Nonce:      56,
			ValidUntil: 78,
			Memo:       "memo",
			Tag:        [3]bool{false, false, false},
			SourcePk:   fromPublicKey,
			ReceiverPk: toPublicKey,
			TokenId:    1,
			Amount:     34,
			Locked:     false,
			NetworkId:  mina.TestNet,
		}
		expectedTransactionBinary, err := expectedTransaction.MarshalBinary()
		s.Require().NoError(err)
		s.Require().Equal(expectedTransactionBinary, transactionBinary)
	})

	s.Run("successful to parse when payment exists with null valid_until and memo", func() {
		payloadBinary, _ := json.Marshal(signingPayloadWithPaymentAndNullValidUntilAndNullMemo)
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             payloadBinary,
			SignatureType:     types.SchnorrPoseidon,
		}

		transaction, err := ParseSigningPayload(payload)
		s.Require().NoError(err)
		transactionBinary, err := transaction.MarshalBinary()
		s.Require().NoError(err)

		expectedTransaction := mina.Transaction{
			Fee:        12,
			FeeToken:   1,
			FeePayerPk: fromPublicKey,
			Nonce:      56,
			ValidUntil: 4294967295,
			Memo:       "",
			Tag:        [3]bool{false, false, false},
			SourcePk:   fromPublicKey,
			ReceiverPk: toPublicKey,
			TokenId:    1,
			Amount:     34,
			Locked:     false,
			NetworkId:  mina.TestNet,
		}
		expectedTransactionBinary, err := expectedTransaction.MarshalBinary()
		s.Require().NoError(err)
		s.Require().Equal(expectedTransactionBinary, transactionBinary)
	})

	s.Run("failed to parse when payment or stake delegation does not exist", func() {
		payloadBinary, _ := json.Marshal(signingPayloadWithNoPaymentOrDelegation)
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             payloadBinary,
			SignatureType:     types.SchnorrPoseidon,
		}
		transaction, err := ParseSigningPayload(payload)
		s.Require().Error(err)

		s.Require().Nil(transaction)
	})

	s.Run("failed to parse when payload json is invalid", func() {
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             []byte{0x12, 0x34},
			SignatureType:     types.SchnorrPoseidon,
		}
		transaction, err := ParseSigningPayload(payload)
		s.Require().Error(err)
		s.Require().Nil(transaction)
	})

	s.Run("failed to parse when from public key in payment is invalid", func() {
		payloadBinary, _ := json.Marshal(signingPayloadWithPaymentAndInvalidFromPublicKey)
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             payloadBinary,
			SignatureType:     types.SchnorrPoseidon,
		}
		transactionBinary, err := ParseSigningPayload(payload)
		s.Require().Error(err)
		s.Require().Nil(transactionBinary)
	})

	s.Run("failed to parse when to public key in payment is invalid", func() {
		payloadBinary, _ := json.Marshal(signingPayloadWithPaymentAndInvalidToPublicKey)
		payload := &types.SigningPayload{
			AccountIdentifier: &types.AccountIdentifier{Address: "test"},
			Bytes:             payloadBinary,
			SignatureType:     types.SchnorrPoseidon,
		}
		transactionBinary, err := ParseSigningPayload(payload)
		s.Require().Error(err)
		s.Require().Nil(transactionBinary)
	})
}

func TestSignPallas(t *testing.T) {
	type payloadTest struct {
		payload *types.SigningPayload
		err     bool
		errMsg  error
	}

	var payloadTests = []payloadTest{
		{mockPayload(txnBytes, types.SchnorrPoseidon), false, nil},
		{mockPayload(txnBytes, ""), false, nil},
		{mockPayload(txnBytes, types.Ecdsa), true, ErrSignUnsupportedPayloadSignatureType},
		{
			mockPayload(txnBytes, types.EcdsaRecovery),
			true,
			ErrSignUnsupportedPayloadSignatureType,
		},
	}
	for _, test := range payloadTests {
		signature, err := signerPallas.Sign(test.payload, types.SchnorrPoseidon)

		if !test.err {
			assert.NoError(t, err)
			assert.Len(t, signature.Bytes, 64)
			assert.Equal(t, signerPallas.PublicKey(), signature.PublicKey)
		} else {
			assert.Contains(t, err.Error(), test.errMsg.Error())
		}
	}
}

func TestVerifyPallas(t *testing.T) {
	type signatureTest struct {
		signature *types.Signature
		errMsg    error
	}

	payload := mockPayload(txnBytes, types.SchnorrPoseidon)
	testSignature, err := signerPallas.Sign(payload, types.SchnorrPoseidon)
	assert.NoError(t, err)

	simpleBytes := make([]byte, 32)
	copy(simpleBytes, "hello")

	var signatureTests = []signatureTest{
		{mockSignature(
			types.Ecdsa,
			signerPallas.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.EcdsaRecovery,
			signerPallas.PublicKey(),
			txnBytes,
			simpleBytes), ErrVerifyUnsupportedPayloadSignatureType},
		{mockSignature(
			types.SchnorrPoseidon,
			signerPallas.PublicKey(),
			simpleBytes,
			testSignature.Bytes), fmt.Errorf("failed to parse signing payload")},
	}

	for _, test := range signatureTests {
		err := signerPallas.Verify(test.signature)
		assert.Contains(t, err.Error(), test.errMsg.Error())
	}

	// happy path
	goodSignature := mockSignature(
		types.SchnorrPoseidon,
		keypair.PublicKey,
		txnBytes,
		testSignature.Bytes,
	)

	assert.Equal(t, nil, signerPallas.Verify(goodSignature))
}
