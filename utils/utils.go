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

	"github.com/fatih/color"

	"github.com/coinbase/rosetta-sdk-go/fetcher"
	storageErrors "github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
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

	// MaxEntrySizePerTxn is the maximum number of entries
	// in one transaction object. This is used for bootstrap
	// balances process to avoid TxnTooBig error when l0_in_memory_enabled=false
	// as well as reduce the running time.
	MaxEntrySizePerTxn = 600
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
		return "", fmt.Errorf("failed to create temporary directory: %w", err)
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
		return fmt.Errorf("unable to create data and network directory: %w", err)
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
		return fmt.Errorf("unable to write to file path %s: %w", filePath, err)
	}

	return nil
}

// LoadAndParse reads the file at the provided path
// and attempts to unmarshal it into output.
func LoadAndParse(filePath string, output interface{}) error {
	b, err := ioutil.ReadFile(path.Clean(filePath))
	if err != nil {
		return fmt.Errorf("unable to load file %s: %w", filePath, err)
	}

	// To prevent silent erroring, we explicitly
	// reject any unknown fields.
	dec := json.NewDecoder(bytes.NewReader(b))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&output); err != nil {
		return fmt.Errorf("unable to unmarshal: %w", err)
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
		return "", fmt.Errorf("failed to create path %s: %w", dataPath, err)
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

	AccountCoinsRetry(
		ctx context.Context,
		network *types.NetworkIdentifier,
		acct *types.AccountIdentifier,
		includeMempool bool,
		currencies []*types.Currency,
	) (*types.BlockIdentifier, []*types.Coin, map[string]interface{}, *fetcher.Error)
}

type BlockStorageHelper interface {
	GetBlockLazy(
		ctx context.Context,
		blockIdentifier *types.PartialBlockIdentifier,
	) (*types.BlockResponse, error)
	// todo add all relevant BlockStorage functions
	// to this interface.
}

// CheckNetworkTip returns a boolean indicating if the block returned by
// network/status is at tip. It also returns the block identifier
// returned by network/status.
// Note that the tipDelay param takes tip delay in seconds.
// Block returned by network/status is considered to be at tip if one of the
// following two conditions is met:
// (1) the block was produced within tipDelay of current time
// (i.e. block timestamp >= current time - tipDelay)
// (2) the network/status endpoint returns a SyncStatus with Synced = true.
func CheckNetworkTip(ctx context.Context,
	network *types.NetworkIdentifier,
	tipDelay int64,
	f FetcherHelper,
) (bool, *types.BlockIdentifier, error) {
	// todo: refactor CheckNetworkTip and its usages to accept metadata and pass it to
	// NetworkStatusRetry call.
	status, fetchErr := f.NetworkStatusRetry(ctx, network, nil)
	if fetchErr != nil {
		return false, nil, fmt.Errorf(
			"unable to fetch network status of network %s: %w",
			types.PrintStruct(network),
			fetchErr.Err,
		)
	}

	// if the block timestamp is within tip delay of current time,
	// it can be considered to be at tip.
	if AtTip(tipDelay, status.CurrentBlockTimestamp) {
		return true, status.CurrentBlockIdentifier, nil
	}

	// If the sync status returned by network/status is true, we should consider the block to be at
	// tip.
	if status.SyncStatus != nil && status.SyncStatus.Synced != nil && *status.SyncStatus.Synced {
		return true, status.CurrentBlockIdentifier, nil
	}

	return false, status.CurrentBlockIdentifier, nil
}

// CheckStorageTip returns a boolean indicating if the current
// block returned by block storage helper is at tip. It also
// returns the block identifier of the current storage block.
// Note that the tipDelay param takes tip delay in seconds.
// A block in storage is considered to be at tip if one of the
// following two conditions is met:
// (1) the block was produced within tipDelay of current time
// (i.e. block timestamp >= current time - tipDelay)
// (2) CheckNetworkTip returns true and the block it returns
// is same as the current block in storage
func CheckStorageTip(ctx context.Context,
	network *types.NetworkIdentifier,
	tipDelay int64,
	f FetcherHelper,
	b BlockStorageHelper,
) (bool, *types.BlockIdentifier, error) {
	blockResponse, err := b.GetBlockLazy(ctx, nil)
	if errors.Is(err, storageErrors.ErrHeadBlockNotFound) {
		// If no blocks exist in storage yet, we are not at tip
		return false, nil, nil
	}

	currentStorageBlock := blockResponse.Block
	if AtTip(tipDelay, currentStorageBlock.Timestamp) {
		return true, currentStorageBlock.BlockIdentifier, nil
	}

	// if latest block in storage is not at tip,
	// check network status
	networkAtTip, tipBlock, fetchErr := CheckNetworkTip(ctx, network, tipDelay, f)
	if fetchErr != nil {
		return false,
			nil,
			fmt.Errorf("unable to check network tip: %w", fetchErr)
	}

	if networkAtTip && types.Hash(tipBlock) == types.Hash(currentStorageBlock.BlockIdentifier) {
		return true, currentStorageBlock.BlockIdentifier, nil
	}

	return false, currentStorageBlock.BlockIdentifier, nil
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
		return nil, fmt.Errorf("unable to fetch network list: %w", fetchErr.Err)
	}

	networkMatched, supportedNetworks := fetcher.CheckNetworkListForNetwork(
		networks,
		networkIdentifier,
	)
	if !networkMatched {
		color.Yellow("Supported networks: %s", types.PrettyPrintStruct(supportedNetworks))
		return nil, fmt.Errorf(
			"network %s is invalid: %w",
			types.PrintStruct(networkIdentifier),
			ErrNetworkNotSupported,
		)
	}

	status, fetchErr := helper.NetworkStatusRetry(
		ctx,
		networkIdentifier,
		nil,
	)
	if fetchErr != nil {
		return nil, fmt.Errorf(
			"unable to fetch network status of network %s: %w",
			types.PrintStruct(networkIdentifier),
			fetchErr.Err,
		)
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
func RandomNumber(minimum *big.Int, maximum *big.Int) (*big.Int, error) {
	transformed := new(big.Int).Sub(maximum, minimum)
	if transformed.Sign() < 0 {
		return nil, fmt.Errorf(
			"maximum value %s < minimum value %s",
			maximum.String(),
			minimum.String(),
		)
	}

	addition, err := rand.Int(rand.Reader, transformed)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random number: %w", err)
	}

	return new(big.Int).Add(minimum, addition), nil
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
		return nil, nil, fmt.Errorf(
			"unable to fetch account balance for currency %s of account %s: %w",
			types.PrintStruct([]*types.Currency{currency}),
			types.PrintStruct(account),
			fetchErr.Err,
		)
	}

	liveAmount := types.ExtractAmount(liveBalances, currency)
	return liveAmount, liveBlock, nil
}

// AllCurrencyBalance returns the balance batch of an account
// for all currencies at a particular height.
// It is up to the caller to determine if the retrieved
// block has the expected hash for the requested index.
func AllCurrencyBalance(
	ctx context.Context,
	network *types.NetworkIdentifier,
	helper FetcherHelper,
	account *types.AccountIdentifier,
	index int64,
) ([]*types.Amount, *types.BlockIdentifier, error) {
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
		nil,
	)
	if fetchErr != nil {
		return nil, nil, fmt.Errorf(
			"unable to fetch account balance for all currencies of account %s: %w",
			types.PrintStruct(account),
			fetchErr.Err,
		)
	}

	return liveBalances, liveBlock, nil
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
		if balanceRequest.Currency != nil {
			amount, block, err := CurrencyBalance(
				ctx,
				balanceRequest.Network,
				fetcher,
				balanceRequest.Account,
				balanceRequest.Currency,
				-1,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get currency balance: %w", err)
			}

			accountBalance := &AccountBalance{
				Account: balanceRequest.Account,
				Amount:  amount,
				Block:   block,
			}
			accountBalances = append(accountBalances, accountBalance)
		} else {
			amounts, block, err := AllCurrencyBalance(
				ctx,
				balanceRequest.Network,
				fetcher,
				balanceRequest.Account,
				-1,
			)
			if err != nil {
				return nil, fmt.Errorf("failed to get all currencies balance: %w", err)
			}

			for _, amount := range amounts {
				accountBalance := &AccountBalance{
					Account: balanceRequest.Account,
					Amount:  amount,
					Block:   block,
				}
				accountBalances = append(accountBalances, accountBalance)
			}
		}
	}

	return accountBalances, nil
}

// -------------------------------------------------------------------------------
// ----------------- Helper struct for fetching account coins --------------------
// -------------------------------------------------------------------------------

// AccountCoinsRequest defines the required information to get an account's coins.
type AccountCoinsRequest struct {
	Account        *types.AccountIdentifier
	Network        *types.NetworkIdentifier
	Currencies     []*types.Currency
	IncludeMempool bool
}

// AccountCoins defines an account's coins info at tip.
type AccountCoinsResponse struct {
	Coins []*types.Coin
}

// GetAccountCoins calls /account/coins endpoint and returns an array of coins at tip.
func GetAccountCoins(
	ctx context.Context,
	fetcher FetcherHelper,
	acctCoinsReqs []*AccountCoinsRequest,
) ([]*AccountCoinsResponse, error) {
	var acctCoins []*AccountCoinsResponse
	for _, req := range acctCoinsReqs {
		_, coins, _, err := fetcher.AccountCoinsRetry(
			ctx,
			req.Network,
			req.Account,
			req.IncludeMempool,
			req.Currencies,
		)

		if err != nil {
			return nil, fmt.Errorf(
				"unable to fetch account coin for currency %s of account %s: %w",
				types.PrintStruct(req.Currencies),
				types.PrintStruct(req.Account),
				err.Err,
			)
		}

		resp := &AccountCoinsResponse{
			Coins: coins,
		}

		acctCoins = append(acctCoins, resp)
	}

	return acctCoins, nil
}

// -------------------------------------------------------------------------------
// ------------------- End of helper struct for account coins --------------------
// -------------------------------------------------------------------------------

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
