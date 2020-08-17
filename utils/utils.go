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

package utils

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"math/rand"
	"os"
	"path"
	"time"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/fatih/color"
)

func init() {
	rand.Seed(time.Now().UTC().UnixNano())
}

const (
	// DefaultFilePermissions specifies that the user can
	// read and write the file.
	DefaultFilePermissions = 0600

	// AllFilePermissions specifies anyone can do anything
	// to the file.
	AllFilePermissions = 0777

	base10            = 10
	bigFloatPrecision = 512

	// NanosecondsInMillisecond is the number
	// of nanoseconds in a millisecond.
	NanosecondsInMillisecond = 1000000

	// MillisecondsInSecond is the number
	// of milliseconds in a second.
	MillisecondsInSecond = 1000

	// OneHundred is the number 100.
	OneHundred = 100
)

var (
	// ErrNetworkNotSupported is returned when the network
	// you are attempting to connect to is not supported.
	ErrNetworkNotSupported = errors.New("network not supported")

	// OneHundredInt is a big.Int of value 100.
	OneHundredInt = big.NewInt(OneHundred)

	// ZeroInt is a big.Int of value 0.
	ZeroInt = big.NewInt(0)
)

// CreateTempDir creates a directory in
// /tmp for usage within testing.
func CreateTempDir() (string, error) {
	storageDir, err := ioutil.TempDir("", "rosetta-cli")
	if err != nil {
		return "", err
	}

	color.Cyan("Using temporary directory %s", storageDir)
	return storageDir, nil
}

// RemoveTempDir deletes a directory at
// a provided path for usage within testing.
func RemoveTempDir(dir string) {
	if err := os.RemoveAll(dir); err != nil {
		log.Fatal(err)
	}
}

// EnsurePathExists creates directories along
// a path if they do not exist.
func EnsurePathExists(path string) error {
	if err := os.MkdirAll(path, os.FileMode(AllFilePermissions)); err != nil {
		return fmt.Errorf("%w: unable to create data and network directory", err)
	}

	return nil
}

// Equal returns a boolean indicating if two
// interfaces are equal.
func Equal(a interface{}, b interface{}) bool {
	return types.Hash(a) == types.Hash(b)
}

// SerializeAndWrite attempts to serialize the provided object
// into a file at filePath.
func SerializeAndWrite(filePath string, object interface{}) error {
	err := ioutil.WriteFile(
		filePath,
		[]byte(types.PrettyPrintStruct(object)),
		os.FileMode(DefaultFilePermissions),
	)
	if err != nil {
		return fmt.Errorf("%w: unable to write to file path %s", err, filePath)
	}

	return nil
}

// LoadAndParse reads the file at the provided path
// and attempts to unmarshal it into output.
func LoadAndParse(filePath string, output interface{}) error {
	b, err := ioutil.ReadFile(path.Clean(filePath))
	if err != nil {
		return fmt.Errorf("%w: unable to load file %s", err, filePath)
	}

	// To prevent silent erroring, we explicitly
	// reject any unknown fields.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&output); err != nil {
		return fmt.Errorf("%w: unable to unmarshal", err)
	}

	return nil
}

// CreateCommandPath creates a unique path for a command and network within a data directory. This
// is used to avoid collision when using multiple commands on multiple networks
// when the same storage resources are used. If the derived path does not exist,
// we run os.MkdirAll on the path.
func CreateCommandPath(
	dataDirectory string,
	cmd string,
	network *types.NetworkIdentifier,
) (string, error) {
	dataPath := path.Join(dataDirectory, cmd, types.Hash(network))
	if err := EnsurePathExists(dataPath); err != nil {
		return "", fmt.Errorf("%w: cannot populate path", err)
	}

	return dataPath, nil
}

// CheckNetworkSupported checks if a Rosetta implementation supports a given
// *types.NetworkIdentifier. If it does, the current network status is returned.
func CheckNetworkSupported(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
) (*types.NetworkStatusResponse, error) {
	networks, fetchErr := fetcher.NetworkList(ctx, nil)
	if fetchErr != nil {
		return nil, fmt.Errorf("%w: unable to fetch network list", fetchErr.Err)
	}

	networkMatched := false
	supportedNetworks := []*types.NetworkIdentifier{}
	for _, availableNetwork := range networks.NetworkIdentifiers {
		if types.Hash(availableNetwork) == types.Hash(networkIdentifier) {
			networkMatched = true
			break
		}

		supportedNetworks = append(supportedNetworks, availableNetwork)
	}

	if !networkMatched {
		color.Yellow("Supported networks: %s", types.PrettyPrintStruct(supportedNetworks))
		return nil, fmt.Errorf(
			"%w: %s is not available",
			ErrNetworkNotSupported,
			types.PrettyPrintStruct(networkIdentifier),
		)
	}

	status, fetchErr := fetcher.NetworkStatusRetry(
		ctx,
		networkIdentifier,
		nil,
	)
	if fetchErr != nil {
		return nil, fmt.Errorf("%w: unable to get network status", fetchErr.Err)
	}

	return status, nil
}

// BigPow10 computes the value of 10^e.
// Inspired by:
// https://steemit.com/tutorial/@gopher23/power-and-root-functions-using-big-float-in-golang
func BigPow10(e int32) *big.Float {
	a := big.NewFloat(base10)
	result := Zero().Copy(a)
	for i := int32(0); i < e-1; i++ {
		result = Zero().Mul(result, a)
	}
	return result
}

// Zero returns a float with 256 bit precision.
func Zero() *big.Float {
	r := big.NewFloat(0)
	r.SetPrec(bigFloatPrecision)
	return r
}

// RandomNumber returns some number in the range [minimum, maximum).
// Source: https://golang.org/pkg/math/big/#Int.Rand
func RandomNumber(minimum *big.Int, maximum *big.Int) *big.Int {
	source := rand.New(rand.NewSource(time.Now().UnixNano()))
	transformed := new(big.Int).Sub(maximum, minimum)
	addition := new(big.Int).Rand(source, transformed)

	return new(big.Int).Add(minimum, addition)
}

// ContainsString returns a boolean indicating
// whether the string s is in arr.
func ContainsString(arr []string, s string) bool {
	for _, v := range arr {
		if v == s {
			return true
		}
	}

	return false
}

// PrettyAmount returns a currency amount in native format with
// its symbol.
func PrettyAmount(amount *big.Int, currency *types.Currency) string {
	nativeUnits := new(big.Float).SetInt(amount)
	precision := currency.Decimals
	if precision > 0 {
		divisor := BigPow10(precision)
		nativeUnits = new(big.Float).Quo(nativeUnits, divisor)
	}

	return fmt.Sprintf(
		"%s %s",
		nativeUnits.Text('f', int(precision)),
		currency.Symbol,
	)
}

// Milliseconds gets the current time in milliseconds.
func Milliseconds() int64 {
	nanos := time.Now().UnixNano()
	return nanos / NanosecondsInMillisecond
}

// CurrencyBalance returns the balance of an account
// for a particular currency.
func CurrencyBalance(
	ctx context.Context,
	network *types.NetworkIdentifier,
	fetcher *fetcher.Fetcher,
	account *types.AccountIdentifier,
	currency *types.Currency,
	block *types.BlockIdentifier,
) (*types.Amount, *types.BlockIdentifier, error) {
	var lookupBlock *types.PartialBlockIdentifier
	if block != nil {
		lookupBlock = types.ConstructPartialBlockIdentifier(block)
	}

	liveBlock, liveBalances, _, _, fetchErr := fetcher.AccountBalanceRetry(
		ctx,
		network,
		account,
		lookupBlock,
	)
	if fetchErr != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to lookup acccount balance for %s",
			fetchErr.Err,
			types.PrettyPrintStruct(account),
		)
	}

	liveAmount, err := types.ExtractAmount(liveBalances, currency)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: could not get %s currency balance for %s",
			err,
			types.PrettyPrintStruct(currency),
			types.PrettyPrintStruct(account),
		)
	}

	return liveAmount, liveBlock, nil
}
