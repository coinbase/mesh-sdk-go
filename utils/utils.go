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
	"crypto/rand"
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"log"
	"math/big"
	"os"
	"path"
	"runtime"
	"time"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/fatih/color"
)

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

	// minBlocksPerSecond is the minimum blocks per second
	// to consider when estimating time to tip if the provided
	// estimate is 0.
	minBlocksPerSecond = 0.0001
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
	storageDir, err := ioutil.TempDir("", "")
	if err != nil {
		return "", err
	}

	color.Cyan("Using temporary directory %s", storageDir)
	return storageDir, nil
}

// RemoveTempDir deletes a directory at
// a provided path for usage within testing.
func RemoveTempDir(dir string) {
	color.Yellow("Removing temporary directory %s", dir)
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

// FetcherHelper is used by util functions to mock Fetcher
type FetcherHelper interface {
	NetworkList(
		ctx context.Context,
		metadata map[string]interface{},
	) (*types.NetworkListResponse, *fetcher.Error)

	NetworkStatusRetry(
		ctx context.Context,
		network *types.NetworkIdentifier,
		metadata map[string]interface{},
	) (*types.NetworkStatusResponse, *fetcher.Error)

	AccountBalanceRetry(
		ctx context.Context,
		network *types.NetworkIdentifier,
		account *types.AccountIdentifier,
		block *types.PartialBlockIdentifier,
		currencies []*types.Currency,
	) (*types.BlockIdentifier, []*types.Amount, map[string]interface{}, *fetcher.Error)
}

// CheckNetworkSupported checks if a Rosetta implementation supports a given
// *types.NetworkIdentifier. If it does, the current network status is returned.
func CheckNetworkSupported(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	helper FetcherHelper,
) (*types.NetworkStatusResponse, error) {
	networks, fetchErr := helper.NetworkList(ctx, nil)
	if fetchErr != nil {
		return nil, fmt.Errorf("%w: unable to fetch network list", fetchErr.Err)
	}

	networkMatched, supportedNetworks := fetcher.CheckNetworkListForNetwork(
		networks,
		networkIdentifier,
	)
	if !networkMatched {
		color.Yellow("Supported networks: %s", types.PrettyPrintStruct(supportedNetworks))
		return nil, fmt.Errorf(
			"%w: %s is not available",
			ErrNetworkNotSupported,
			types.PrettyPrintStruct(networkIdentifier),
		)
	}

	status, fetchErr := helper.NetworkStatusRetry(
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
// Source: https://golang.org/pkg/crypto/rand/#Int
func RandomNumber(minimum *big.Int, maximum *big.Int) *big.Int {
	transformed := new(big.Int).Sub(maximum, minimum)
	addition, err := rand.Int(rand.Reader, transformed)
	if err != nil {
		log.Fatalf("cannot get random number: %v", err)
	}

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

// ContainsAccountIdentifier returns a boolean indicating
// whether the struct s is in arr.
func ContainsAccountIdentifier(arr []*types.AccountIdentifier, s *types.AccountIdentifier) bool {
	for _, v := range arr {
		if types.Hash(v) == types.Hash(s) {
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
// for a particular currency at a particular height.
// It is up to the caller to determine if the retrieved
// block has the expected hash for the requested index.
func CurrencyBalance(
	ctx context.Context,
	network *types.NetworkIdentifier,
	helper FetcherHelper,
	account *types.AccountIdentifier,
	currency *types.Currency,
	index int64,
) (*types.Amount, *types.BlockIdentifier, error) {
	var lookupBlock *types.PartialBlockIdentifier
	if index >= 0 {
		lookupBlock = &types.PartialBlockIdentifier{
			Index: &index,
		}
	}

	liveBlock, liveBalances, _, fetchErr := helper.AccountBalanceRetry(
		ctx,
		network,
		account,
		lookupBlock,
		[]*types.Currency{currency},
	)
	if fetchErr != nil {
		return nil, nil, fetchErr.Err
	}

	liveAmount, err := types.ExtractAmount(liveBalances, currency)
	if err != nil {
		formattedError := fmt.Errorf(
			"%w: could not get %s currency balance for %s",
			err,
			types.PrettyPrintStruct(currency),
			types.PrettyPrintStruct(account),
		)

		return nil, nil, formattedError
	}

	return liveAmount, liveBlock, nil
}

// AccountBalanceRequest defines the required information
// to get an account's balance.
type AccountBalanceRequest struct {
	Account  *types.AccountIdentifier
	Network  *types.NetworkIdentifier
	Currency *types.Currency
}

// AccountBalance defines an account's balance,
// including either balance or coins, as well as
// the block which this balance was fetched at.
type AccountBalance struct {
	Account *types.AccountIdentifier
	Amount  *types.Amount
	Coins   []*types.Coin
	Block   *types.BlockIdentifier
}

// GetAccountBalances returns an array of AccountBalances
// for an array of AccountBalanceRequests
func GetAccountBalances(
	ctx context.Context,
	fetcher FetcherHelper,
	balanceRequests []*AccountBalanceRequest,
) ([]*AccountBalance, error) {
	var accountBalances []*AccountBalance
	for _, balanceRequest := range balanceRequests {
		amount, block, err := CurrencyBalance(
			ctx,
			balanceRequest.Network,
			fetcher,
			balanceRequest.Account,
			balanceRequest.Currency,
			-1,
		)

		if err != nil {
			return nil, err
		}

		accountBalance := &AccountBalance{
			Account: balanceRequest.Account,
			Amount:  amount,
			Block:   block,
		}

		accountBalances = append(accountBalances, accountBalance)
	}

	return accountBalances, nil
}

// AtTip returns a boolean indicating if a block timestamp
// is within tipDelay from the current time.
func AtTip(
	tipDelay int64,
	blockTimestamp int64,
) bool {
	currentTime := Milliseconds()
	tipCutoff := currentTime - (tipDelay * MillisecondsInSecond)

	return blockTimestamp >= tipCutoff
}

// CheckAtTip returns a boolean indicating if a
// Rosetta implementation is at tip.
func CheckAtTip(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	helper FetcherHelper,
	tipDelay int64,
) (bool, error) {
	status, fetchErr := helper.NetworkStatusRetry(
		ctx,
		networkIdentifier,
		nil,
	)
	if fetchErr != nil {
		return false, fmt.Errorf("%w: unable to get network status", fetchErr.Err)
	}

	return AtTip(tipDelay, status.CurrentBlockTimestamp), nil
}

// ContextSleep sleeps for the provided duration and returns
// an error if context is canceled.
func ContextSleep(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case <-timer.C:
			return nil
		}
	}
}

// MemoryUsage contains memory usage stats converted
// to MBs.
type MemoryUsage struct {
	Heap               float64 `json:"heap"`
	Stack              float64 `json:"stack"`
	OtherSystem        float64 `json:"other_system"`
	System             float64 `json:"system"`
	GarbageCollections uint32  `json:"garbage_collections"`
}

// MonitorMemoryUsage returns a collection of memory usage
// stats in MB. It will also run garbage collection if the heap
// is greater than maxHeapUsage in MB.
func MonitorMemoryUsage(
	ctx context.Context,
	maxHeapUsage int,
) *MemoryUsage {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)

	usage := &MemoryUsage{
		Heap:               BtoMb(float64(m.HeapAlloc)),
		Stack:              BtoMb(float64(m.StackInuse)),
		OtherSystem:        BtoMb(float64(m.OtherSys)),
		System:             BtoMb(float64(m.Sys)),
		GarbageCollections: m.NumGC,
	}

	if maxHeapUsage != -1 && usage.Heap > float64(maxHeapUsage) {
		runtime.GC()
	}

	return usage
}

// TimeToTip returns the estimate time to tip given
// the current sync speed.
func TimeToTip(
	blocksPerSecond float64,
	lastSyncedIndex int64,
	tipIndex int64,
) time.Duration {
	if blocksPerSecond <= 0 { // ensure we don't divide by 0
		blocksPerSecond = minBlocksPerSecond
	}

	remainingBlocks := tipIndex - lastSyncedIndex
	if remainingBlocks < 0 { // ensure we don't get negative time
		remainingBlocks = 0
	}

	secondsRemaining := int64(float64(remainingBlocks) / blocksPerSecond)

	return time.Duration(secondsRemaining) * time.Second
}
