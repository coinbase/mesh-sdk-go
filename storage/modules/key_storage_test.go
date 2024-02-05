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

	"github.com/ethereum/go-ethereum/common"
	"github.com/stretchr/testify/assert"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

func hash(message string) []byte {
	messageHashBytes := common.BytesToHash([]byte(message)).Bytes()
	return messageHashBytes
}

func TestKeyStorage(t *testing.T) {
	ctx := context.Background()

	newDir, err := utils.CreateTempDir()
	assert.NoError(t, err)
	defer utils.RemoveTempDir(newDir)

	database, err := newTestBadgerDatabase(ctx, newDir)
	assert.NoError(t, err)
	defer database.Close(ctx)

	k := NewKeyStorage(database)

	kp1, err := keys.GenerateKeypair(types.Edwards25519)
	assert.NoError(t, err)

	kp2, err := keys.GenerateKeypair(types.Secp256k1)
	assert.NoError(t, err)

	t.Run("get non-existent key", func(t *testing.T) {
		v, err := k.Get(ctx, &types.AccountIdentifier{Address: "blah"})
		assert.Error(t, err)
		assert.Nil(t, v)

		accounts, err := k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		assert.Len(t, accounts, 0)
	})

	t.Run("store and get key", func(t *testing.T) {
		err = k.Store(ctx, &types.AccountIdentifier{Address: "addr1"}, kp1)
		assert.NoError(t, err)

		v, err := k.Get(ctx, &types.AccountIdentifier{Address: "addr1"})
		assert.NoError(t, err)
		assert.Equal(t, kp1, v)

		// ensure account with subaccount is not accessible
		v, err = k.Get(ctx, &types.AccountIdentifier{
			Address: "addr1",
			SubAccount: &types.SubAccountIdentifier{
				Address: "blah",
			},
		})
		assert.Error(t, err)
		assert.Nil(t, v)

		accounts, err := k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		assert.Equal(t, []*types.AccountIdentifier{
			{
				Address: "addr1",
			},
		}, accounts)
	})

	t.Run("attempt overwrite", func(t *testing.T) {
		err = k.Store(ctx, &types.AccountIdentifier{Address: "addr1"}, kp2)
		assert.Error(t, err)

		v, err := k.Get(ctx, &types.AccountIdentifier{Address: "addr1"})
		assert.NoError(t, err)
		assert.Equal(t, kp1, v)
	})

	t.Run("store and get another key", func(t *testing.T) {
		err = k.Store(ctx, &types.AccountIdentifier{Address: "addr2"}, kp2)
		assert.NoError(t, err)

		v, err := k.Get(ctx, &types.AccountIdentifier{Address: "addr2"})
		assert.NoError(t, err)
		assert.Equal(t, kp2, v)

		accounts, err := k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		assert.ElementsMatch(t, []*types.AccountIdentifier{
			{Address: "addr1"},
			{Address: "addr2"},
		}, accounts)
	})

	t.Run("sign payloads", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				AccountIdentifier: &types.AccountIdentifier{Address: "addr1"},
				Bytes:             hash("msg1"),
				SignatureType:     types.Ed25519,
			},
			{
				AccountIdentifier: &types.AccountIdentifier{Address: "addr2"},
				Bytes:             hash("msg2"),
				SignatureType:     types.Ecdsa,
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.NoError(t, err)
		assert.Len(t, sigs, 2)
		assert.NoError(t, (&keys.SignerEdwards25519{}).Verify(sigs[0]))
		assert.NoError(t, (&keys.SignerSecp256k1{}).Verify(sigs[1]))
	})

	t.Run("missing address in sign", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				AccountIdentifier: &types.AccountIdentifier{Address: "addr3"},
				Bytes:             hash("msg3"),
				SignatureType:     types.Ed25519,
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.Error(t, err)
		assert.Nil(t, sigs)
	})

	t.Run("missing signature type in sign", func(t *testing.T) {
		payloads := []*types.SigningPayload{
			{
				AccountIdentifier: &types.AccountIdentifier{Address: "addr1"},
				Bytes:             hash("msg1"),
			},
		}

		sigs, err := k.Sign(ctx, payloads)
		assert.Error(t, err)
		assert.Nil(t, sigs)
	})

	t.Run("imports accounts", func(t *testing.T) {
		accounts, err := k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		startingLen := len(accounts)

		prefundedAccs := []*PrefundedAccount{
			{
				PrivateKeyHex:     "0e842a16b2d39a4dff5c63688513cb2109e30c3c30bc4eb502cc54f4614493f6",
				AccountIdentifier: &types.AccountIdentifier{Address: "add1"},
				CurveType:         types.Edwards25519,
			},
			{
				PrivateKeyHex:     "42efc44bdf7b2d4d45ddd6ddb727ed498c91e7070914c9ed0d80af680ff42b3e",
				AccountIdentifier: &types.AccountIdentifier{Address: "add2"},
				CurveType:         types.Edwards25519,
			},
			{
				PrivateKeyHex:     "01ea48249742650907004331e85536f868e2d3959434ba751d8aa230138a9707",
				AccountIdentifier: &types.AccountIdentifier{Address: "add3"},
				CurveType:         types.Edwards25519,
			},
		}

		err = k.ImportAccounts(ctx, prefundedAccs)
		assert.NoError(t, err)
		accounts, err = k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		endLen := len(accounts)
		assert.Equal(t, endLen, startingLen+len(prefundedAccs))
	})

	t.Run("does not import same key twice", func(t *testing.T) {
		prefundedAccs := []*PrefundedAccount{
			{
				PrivateKeyHex:     "17d08f5fe8c77af811caa0c9a187e668ce3b74a99acc3f6d976f075fa8e0be55",
				AccountIdentifier: &types.AccountIdentifier{Address: "badadd"},
				CurveType:         types.Edwards25519,
			},
		}

		// No error when importing key for the first time
		err = k.ImportAccounts(ctx, prefundedAccs)
		assert.NoError(t, err)
		accounts, err := k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		startingLen := len(accounts)

		// No error when trying to import the key again
		err = k.ImportAccounts(ctx, prefundedAccs)
		assert.NoError(t, err)
		accounts, err = k.GetAllAccounts(ctx)
		assert.NoError(t, err)
		endLen := len(accounts)

		assert.Equal(t, endLen, startingLen)
	})
}
