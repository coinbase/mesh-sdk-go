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
	"bytes"
	"context"
	"errors"
	"fmt"
	"math/rand"
	"time"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
)

// WARNING: KEY STORAGE USING THIS PACKAGE IS NOT SECURE!!!! ONLY USE
// FOR TESTING!!!!

// PrefundedAccount is used to load prefunded addresses into key storage.
type PrefundedAccount struct {
	PrivateKeyHex string                   `json:"privkey"`
	Account       *types.AccountIdentifier `json:"account"`
	CurveType     types.CurveType          `json:"curve_type"`
	Currency      *types.Currency          `json:"currency"`
}

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	keyNamespace = "key"
)

func getAccountKey(account *types.AccountIdentifier) []byte {
	return []byte(
		fmt.Sprintf("%s/%s", keyNamespace, types.Hash(account)),
	)
}

// KeyStorage implements key storage methods
// on top of a Database and DatabaseTransaction interface.
type KeyStorage struct {
	db Database
}

// NewKeyStorage returns a new KeyStorage.
func NewKeyStorage(
	db Database,
) *KeyStorage {
	return &KeyStorage{
		db: db,
	}
}

// Key is the struct stored in key storage. This
// is public so that accounts can be loaded from
// a configuration file.
type Key struct {
	Account *types.AccountIdentifier `json:"account"`
	KeyPair *keys.KeyPair            `json:"keypair"`
}

func parseKey(buf []byte) (*Key, error) {
	var k Key
	err := getDecoder(bytes.NewReader(buf)).Decode(&k)
	if err != nil {
		return nil, err
	}

	return &k, nil
}

func serializeKey(k *Key) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := getEncoder(buf).Encode(k)
	if err != nil {
		return nil, err
	}

	return buf.Bytes(), nil
}

// StoreTransactional stores a key in a database transaction.
func (k *KeyStorage) StoreTransactional(
	ctx context.Context,
	account *types.AccountIdentifier,
	keyPair *keys.KeyPair,
	dbTx DatabaseTransaction,
) error {
	exists, _, err := dbTx.Get(ctx, getAccountKey(account))
	if err != nil {
		return fmt.Errorf("%w: %s %v", ErrAddrCheckIfExistsFailed, types.PrintStruct(account), err)
	}

	if exists {
		return fmt.Errorf(
			"%w: account %s already exists",
			ErrAddrExists,
			types.PrintStruct(account),
		)
	}

	val, err := serializeKey(&Key{
		Account: account,
		KeyPair: keyPair,
	})
	if err != nil {
		return fmt.Errorf("%w: %v", ErrSerializeKeyFailed, err)
	}

	err = dbTx.Set(ctx, getAccountKey(account), val, true)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrStoreKeyFailed, err)
	}

	return nil
}

// Store saves a keys.KeyPair for a given address. If the address already
// exists, an error is returned.
func (k *KeyStorage) Store(
	ctx context.Context,
	account *types.AccountIdentifier,
	keyPair *keys.KeyPair,
) error {
	dbTx := k.db.NewDatabaseTransaction(ctx, true)
	defer dbTx.Discard(ctx)

	if err := k.StoreTransactional(ctx, account, keyPair, dbTx); err != nil {
		return fmt.Errorf("%w: unable to store key", err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("%w: %v", ErrCommitKeyFailed, err)
	}

	return nil
}

// GetTransactional returns a *keys.KeyPair for an AccountIdentifier in a
// DatabaseTransaction, if it exists.
func (k *KeyStorage) GetTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
	account *types.AccountIdentifier,
) (*keys.KeyPair, error) {
	exists, rawKey, err := dbTx.Get(ctx, getAccountKey(account))
	if err != nil {
		return nil, fmt.Errorf("%w: %s %v", ErrAddrGetFailed, types.PrintStruct(account), err)
	}

	if !exists {
		return nil, fmt.Errorf("%w: %s", ErrAddrNotFound, types.PrintStruct(account))
	}

	key, err := parseKey(rawKey)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrParseSavedKeyFailed, err)
	}

	return key.KeyPair, nil
}

// Get returns a *keys.KeyPair for an AccountIdentifier, if it exists.
func (k *KeyStorage) Get(
	ctx context.Context,
	account *types.AccountIdentifier,
) (*keys.KeyPair, error) {
	transaction := k.db.NewDatabaseTransaction(ctx, false)
	defer transaction.Discard(ctx)

	return k.GetTransactional(ctx, transaction, account)
}

// GetAllAccountsTransactional returns all AccountIdentifiers in key storage.
func (k *KeyStorage) GetAllAccountsTransactional(
	ctx context.Context,
	dbTx DatabaseTransaction,
) ([]*types.AccountIdentifier, error) {
	rawKeys, err := dbTx.Scan(ctx, []byte(keyNamespace), false)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrKeyScanFailed, err)
	}

	accounts := make([]*types.AccountIdentifier, len(rawKeys))
	for i, rawKey := range rawKeys {
		kp, err := parseKey(rawKey.Value)
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrParseKeyPairFailed, err)
		}

		accounts[i] = kp.Account
	}

	return accounts, nil
}

// GetAllAccounts returns all AccountIdentifiers in key storage.
func (k *KeyStorage) GetAllAccounts(ctx context.Context) ([]*types.AccountIdentifier, error) {
	dbTx := k.db.NewDatabaseTransaction(ctx, false)
	defer dbTx.Discard(ctx)

	return k.GetAllAccountsTransactional(ctx, dbTx)
}

// Sign attempts to sign a slice of *types.SigningPayload with the keys in KeyStorage.
func (k *KeyStorage) Sign(
	ctx context.Context,
	payloads []*types.SigningPayload,
) ([]*types.Signature, error) {
	signatures := make([]*types.Signature, len(payloads))
	for i, payload := range payloads {
		keyPair, err := k.Get(ctx, payload.AccountIdentifier)
		if err != nil {
			return nil, fmt.Errorf(
				"%w for %s: %v",
				ErrKeyGetFailed,
				types.PrintStruct(payload.AccountIdentifier),
				err,
			)
		}

		signer, err := keyPair.Signer()
		if err != nil {
			return nil, fmt.Errorf("%w: %v", ErrSignerCreateFailed, err)
		}

		if len(payload.SignatureType) == 0 {
			return nil, fmt.Errorf("%w %d", ErrDetermineSigTypeFailed, i)
		}

		signature, err := signer.Sign(payload, payload.SignatureType)
		if err != nil {
			return nil, fmt.Errorf("%w for %d: %v", ErrSignPayloadFailed, i, err)
		}

		signatures[i] = signature
	}

	return signatures, nil
}

// RandomAccount returns a random account from all accounts.
func (k *KeyStorage) RandomAccount(ctx context.Context) (*types.AccountIdentifier, error) {
	accounts, err := k.GetAllAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrAddrsGetAllFailed, err)
	}

	if len(accounts) == 0 {
		return nil, ErrNoAddrAvailable
	}

	return accounts[rand.Intn(len(accounts))], nil
}

// ImportAccounts loads a set of prefunded accounts into key storage.
func (k *KeyStorage) ImportAccounts(ctx context.Context, accounts []*PrefundedAccount) error {
	// Import prefunded account and save to database
	for _, acc := range accounts {
		keyPair, err := keys.ImportPrivKey(acc.PrivateKeyHex, acc.CurveType)
		if err != nil {
			return fmt.Errorf("%w: %v", ErrAddrImportFailed, err)
		}

		// Skip if key already exists
		err = k.Store(ctx, acc.Account, keyPair)
		if errors.Is(err, ErrAddrExists) {
			continue
		}
		if err != nil {
			return fmt.Errorf("%w: %v", ErrPrefundedAcctStoreFailed, err)
		}
	}
	return nil
}
