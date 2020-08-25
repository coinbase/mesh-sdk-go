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

package worker

import (
	"context"
	"time"

	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	// BalanceWaitTime is the amount of time
	// we wait between balance checks.
	BalanceWaitTime = 5 * time.Second
)

// Helper is used by the worker to process Jobs.
type Helper interface {
	// StoreKey is called to persist an
	// address + KeyPair.
	StoreKey(
		context.Context,
		storage.DatabaseTransaction,
		string,
		*keys.KeyPair,
	) error

	// AllAddresses returns a slice of all known addresses.
	AllAddresses(
		context.Context,
		storage.DatabaseTransaction,
	) ([]string, error)

	// LockedAccounts is a slice of all addresses currently sending or receiving
	// funds.
	LockedAddresses(
		context.Context,
		storage.DatabaseTransaction,
	) ([]string, error)

	// Balance returns the balance
	// for a provided address.
	Balance(
		context.Context,
		storage.DatabaseTransaction,
		*types.AccountIdentifier,
		*types.Currency,
	) ([]*types.Amount, error)

	// Coins returns all *types.Coin owned by an address.
	Coins(
		context.Context,
		storage.DatabaseTransaction,
		*types.AccountIdentifier,
		*types.Currency,
	) ([]*types.Coin, error)

	// Derive returns a new address for a provided publicKey.
	Derive(
		context.Context,
		*types.NetworkIdentifier,
		*types.PublicKey,
		map[string]interface{},
	) (string, map[string]interface{}, error)
}

// Worker processes jobs.
type Worker struct {
	helper Helper
}
