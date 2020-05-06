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

package types

import (
	"crypto/sha256"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"math/big"
)

// ConstructPartialBlockIdentifier constructs a *PartialBlockIdentifier
// from a *BlockIdentifier.
//
// It is useful to have this helper when making block requests
// with the fetcher.
func ConstructPartialBlockIdentifier(
	blockIdentifier *BlockIdentifier,
) *PartialBlockIdentifier {
	return &PartialBlockIdentifier{
		Hash:  &blockIdentifier.Hash,
		Index: &blockIdentifier.Index,
	}
}

// hashBytes returns a hex-encoded sha256 hash of the provided
// byte slice.
func hashBytes(data []byte) string {
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to hash data %s", err, string(data)))
	}

	return fmt.Sprintf("%x", h.Sum(nil))
}

// Hash returns a deterministic hash for any interface.
// This works because Golang's JSON marshaler sorts all map keys, recursively.
// Source: https://golang.org/pkg/encoding/json/#Marshal
// Inspiration:
// https://github.com/onsi/gomega/blob/c0be49994280db30b6b68390f67126d773bc5558/matchers/match_json_matcher.go#L16
//
// It is important to note that any interface that is a slice
// or contains slices will not be equal if the slice ordering is
// different.
func Hash(i interface{}) string {
	// Convert interface to JSON object (not necessarily ordered if struct
	// contains json.RawMessage)
	a, err := json.Marshal(i)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to marshal %+v", err, i))
	}

	// Convert JSON object to interface (all json.RawMessage converted to go types)
	var b interface{}
	if err := json.Unmarshal(a, &b); err != nil {
		log.Fatal(fmt.Errorf("%w: unable to unmarshal %+v", err, a))
	}

	// Convert interface to JSON object (all map keys ordered)
	c, err := json.Marshal(b)
	if err != nil {
		log.Fatal(fmt.Errorf("%w: unable to marshal %+v", err, b))
	}

	return hashBytes(c)
}

// AddValues adds string amounts using
// big.Int.
func AddValues(
	a string,
	b string,
) (string, error) {
	aVal, ok := new(big.Int).SetString(a, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", a)
	}

	bVal, ok := new(big.Int).SetString(b, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", b)
	}

	newVal := new(big.Int).Add(aVal, bVal)
	return newVal.String(), nil
}

// SubtractValues subtracts a-b using
// big.Int.
func SubtractValues(
	a string,
	b string,
) (string, error) {
	aVal, ok := new(big.Int).SetString(a, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", a)
	}

	bVal, ok := new(big.Int).SetString(b, 10)
	if !ok {
		return "", fmt.Errorf("%s is not an integer", b)
	}

	newVal := new(big.Int).Sub(aVal, bVal)
	return newVal.String(), nil
}

// AccountString returns a human-readable representation of a
// *types.AccountIdentifier.
func AccountString(account *AccountIdentifier) (string, error) {
	if account.SubAccount == nil {
		return account.Address, nil
	}

	if account.SubAccount.Metadata == nil {
		return fmt.Sprintf(
			"%s:%s",
			account.Address,
			account.SubAccount.Address,
		), nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal(account.SubAccount.Metadata, &m); err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%s:%s:%+v",
		account.Address,
		account.SubAccount.Address,
		m,
	), nil
}

// CurrencyString returns a human-readable representation
// of a *types.Currency.
func CurrencyString(currency *Currency) (string, error) {
	if currency.Metadata == nil {
		return fmt.Sprintf("%s:%d", currency.Symbol, currency.Decimals), nil
	}

	var m map[string]interface{}
	if err := json.Unmarshal(currency.Metadata, &m); err != nil {
		return "", err
	}

	return fmt.Sprintf(
		"%s:%d:%+v",
		currency.Symbol,
		currency.Decimals,
		m,
	), nil
}

// PrettyPrintStruct marshals a struct to JSON and returns
// it as a string.
func PrettyPrintStruct(val interface{}) string {
	prettyStruct, err := json.MarshalIndent(
		val,
		"",
		" ",
	)
	if err != nil {
		log.Fatal(err)
	}

	return string(prettyStruct)
}

// JSONRawMessage returns a json.RawMessage given an interface.
func JSONRawMessage(i interface{}) (json.RawMessage, error) {
	if i == nil {
		return nil, errors.New("interface is nil")
	}

	v, err := json.Marshal(i)
	if err != nil {
		return nil, err
	}

	return json.RawMessage(v), nil
}
