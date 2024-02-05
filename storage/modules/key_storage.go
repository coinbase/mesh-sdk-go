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
	"errors"
	"fmt"
	"math/big"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	storageErrs "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// WARNING: KEY STORAGE USING THIS PACKAGE IS NOT SECURE!!!! ONLY USE
// FOR TESTING!!!!

// PrefundedAccount is used to load prefunded addresses into key storage.
type PrefundedAccount struct {
	PrivateKeyHex     string                   `json:"privkey"`
	AccountIdentifier *types.AccountIdentifier `json:"account_identifier"`
	CurveType         types.CurveType          `json:"curve_type"`
	Currency          *types.Currency          `json:"currency"`
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
// on top of a database.Database and database.Transaction interface.
type KeyStorage struct {
	db database.Database
}

// NewKeyStorage returns a new KeyStorage.
func NewKeyStorage(
	db database.Database,
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

// StoreTransactional stores a key in a database transaction.
func (k *KeyStorage) StoreTransactional(
	ctx context.Context,
	account *types.AccountIdentifier,
	keyPair *keys.KeyPair,
	dbTx database.Transaction,
) error {
	exists, _, err := dbTx.Get(ctx, getAccountKey(account))
	if err != nil {
		return fmt.Errorf(
			"unable to get key: %w",
			err,
		)
	}

	if exists {
		return storageErrs.ErrAddrExists
	}

	val, err := k.db.Encoder().Encode("", &Key{
		Account: account,
		KeyPair: keyPair,
	})
	if err != nil {
		return fmt.Errorf("unable to encode key: %w", err)
	}

	err = dbTx.Set(ctx, getAccountKey(account), val, true)
	if err != nil {
		return fmt.Errorf("unable to set key: %w", err)
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
	dbTx := k.db.Transaction(ctx)
	defer dbTx.Discard(ctx)

	if err := k.StoreTransactional(ctx, account, keyPair, dbTx); err != nil {
		return fmt.Errorf("unable to store key for account %s: %w", types.PrintStruct(keyPair), err)
	}

	if err := dbTx.Commit(ctx); err != nil {
		return fmt.Errorf("unable to commit new key: %w", err)
	}

	return nil
}

// GetTransactional returns a *keys.KeyPair for an AccountIdentifier in a
// database.Transaction, if it exists.
func (k *KeyStorage) GetTransactional(
	ctx context.Context,
	dbTx database.Transaction,
	account *types.AccountIdentifier,
) (*keys.KeyPair, error) {
	exists, rawKey, err := dbTx.Get(ctx, getAccountKey(account))
	if err != nil {
		return nil, fmt.Errorf(
			"unable to get key: %w",
			err,
		)
	}

	if !exists {
		return nil, storageErrs.ErrAddrNotFound
	}

	var kp Key
	if err := k.db.Encoder().Decode("", rawKey, &kp, true); err != nil {
		return nil, fmt.Errorf("unable to decode key: %w", err)
	}

	return kp.KeyPair, nil
}

// Get returns a *keys.KeyPair for an AccountIdentifier, if it exists.
func (k *KeyStorage) Get(
	ctx context.Context,
	account *types.AccountIdentifier,
) (*keys.KeyPair, error) {
	transaction := k.db.ReadTransaction(ctx)
	defer transaction.Discard(ctx)

	return k.GetTransactional(ctx, transaction, account)
}

// GetAllAccountsTransactional returns all AccountIdentifiers in key storage.
func (k *KeyStorage) GetAllAccountsTransactional(
	ctx context.Context,
	dbTx database.Transaction,
) ([]*types.AccountIdentifier, error) {
	accounts := []*types.AccountIdentifier{}
	_, err := dbTx.Scan(
		ctx,
		[]byte(keyNamespace),
		[]byte(keyNamespace),
		func(key []byte, v []byte) error {
			var kp Key
			// We should not reclaim memory during a scan!!
			if err := k.db.Encoder().Decode("", v, &kp, false); err != nil {
				return fmt.Errorf("unable to decode key: %w", err)
			}

			accounts = append(accounts, kp.Account)
			return nil
		},
		false,
		false,
	)
	if err != nil {
		return nil, fmt.Errorf("database scan failed: %w", err)
	}

	return accounts, nil
}

// GetAllAccounts returns all AccountIdentifiers in key storage.
func (k *KeyStorage) GetAllAccounts(ctx context.Context) ([]*types.AccountIdentifier, error) {
	dbTx := k.db.ReadTransaction(ctx)
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
				"unable to get key for account %s: %w",
				types.PrintStruct(payload.AccountIdentifier),
				err,
			)
		}

		signer, err := keyPair.Signer()
		if err != nil {
			return nil, fmt.Errorf("unable to create signer: %w", err)
		}

		if len(payload.SignatureType) == 0 {
			return nil, storageErrs.ErrDetermineSigTypeFailed
		}

		signature, err := signer.Sign(payload, payload.SignatureType)
		if err != nil {
			return nil, fmt.Errorf("unable to to sign payload: %w", err)
		}

		signatures[i] = signature
	}

	return signatures, nil
}

// RandomAccount returns a random account from all accounts.
func (k *KeyStorage) RandomAccount(ctx context.Context) (*types.AccountIdentifier, error) {
	accounts, err := k.GetAllAccounts(ctx)
	if err != nil {
		return nil, fmt.Errorf("unable to get all accounts: %w", err)
	}

	if len(accounts) == 0 {
		return nil, storageErrs.ErrNoAddrAvailable
	}

	randomNumber, err := utils.RandomNumber(big.NewInt(0), big.NewInt(int64(len(accounts))))
	if err != nil {
		return nil, fmt.Errorf("unable to generate random account: %w", err)
	}

	return accounts[randomNumber.Int64()], nil
}

// ImportAccounts loads a set of prefunded accounts into key storage.
func (k *KeyStorage) ImportAccounts(ctx context.Context, accounts []*PrefundedAccount) error {
	for _, acc := range accounts {
		keyPair, err := keys.ImportPrivateKey(acc.PrivateKeyHex, acc.CurveType)
		if err != nil {
			return fmt.Errorf("unable to import prefunded account: %w", err)
		}

		// Skip if key already exists
		err = k.Store(ctx, acc.AccountIdentifier, keyPair)
		if errors.Is(err, storageErrs.ErrAddrExists) {
			continue
		}
		if err != nil {
			return fmt.Errorf("unable to store prefunded account: %w", err)
		}
	}
	return nil
}
