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
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"

	"github.com/coinbase/rosetta-sdk-go/types"

	"github.com/DataDog/zstd"
	msgpack "github.com/vmihailenco/msgpack/v5"
)

// Compressor handles the compression and decompression
// of data using zstd. Optionally, the caller can provide
// a map of dicts on initialization that can be used by zstd.
// You can read more about these "dicts" here:
// https://github.com/facebook/zstd#the-case-for-small-data-compression.
//
// NOTE: If you change these dicts, you will not be able
// to decode previously encoded data. For many users, providing
// no dicts is sufficient!
type Compressor struct {
	dicts map[string][]byte
	pool  *BufferPool
}

// CompressorEntry is used to initialize a Compressor.
// All DictionaryPaths are loaded from disk at initialization.
type CompressorEntry struct {
	Namespace      string
	DictionaryPath string
}

// NewCompressor returns a new *Compressor. The dicts
// provided should contain k:v of namespace:zstd dict.
func NewCompressor(entries []*CompressorEntry, pool *BufferPool) (*Compressor, error) {
	dicts := map[string][]byte{}
	for _, entry := range entries {
		b, err := ioutil.ReadFile(path.Clean(entry.DictionaryPath))
		if err != nil {
			return nil, fmt.Errorf(
				"%w from %s: %v",
				ErrLoadDictFailed,
				entry.DictionaryPath,
				err,
			)
		}

		log.Printf("loaded zstd dictionary for %s\n", entry.Namespace)
		dicts[entry.Namespace] = b
	}

	return &Compressor{
		dicts: dicts,
		pool:  pool,
	}, nil
}

func getEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.UseJSONTag(true)

	return enc
}

// Encode attempts to compress the object and will use a dict if
// one exists for the namespace.
func (c *Compressor) Encode(namespace string, object interface{}) ([]byte, error) {
	buf := c.pool.Get()
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrObjectEncodeFailed, err)
	}

	output, err := c.EncodeRaw(namespace, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRawCompressFailed, err)
	}

	c.pool.Put(buf)
	return output, nil
}

// EncodeRaw only compresses an input, leaving encoding to the caller.
// This is particularly useful for training a compressor.
func (c *Compressor) EncodeRaw(namespace string, input []byte) ([]byte, error) {
	return c.encode(input, c.dicts[namespace])
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.UseJSONTag(true)

	return dec
}

// Decode attempts to decompress the object and will use a dict if
// one exists for the namespace.
func (c *Compressor) Decode(
	namespace string,
	input []byte,
	object interface{},
	reclaimInput bool,
) error {
	decompressed, err := c.DecodeRaw(namespace, input)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrRawDecompressFailed, err)
	}

	if err := getDecoder(bytes.NewReader(decompressed)).Decode(&object); err != nil {
		return fmt.Errorf("%w: %v", ErrRawDecodeFailed, err)
	}

	c.pool.PutByteSlice(decompressed)
	if reclaimInput {
		c.pool.PutByteSlice(input)
	}
	return nil
}

// DecodeRaw only decompresses an input, leaving decoding to the caller.
// This is particularly useful for training a compressor.
func (c *Compressor) DecodeRaw(namespace string, input []byte) ([]byte, error) {
	return c.decode(input, c.dicts[namespace])
}

func (c *Compressor) encode(input []byte, zstdDict []byte) ([]byte, error) {
	buf := c.pool.Get()
	var writer io.WriteCloser
	if len(zstdDict) > 0 {
		writer = zstd.NewWriterLevelDict(buf, zstd.DefaultCompression, zstdDict)
	} else {
		writer = zstd.NewWriter(buf)
	}
	if _, err := writer.Write(input); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrBufferWriteFailed, err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrWriterCloseFailed, err)
	}

	return buf.Bytes(), nil
}

func (c *Compressor) decode(b []byte, zstdDict []byte) ([]byte, error) {
	buf := c.pool.Get()
	var reader io.ReadCloser
	if len(zstdDict) > 0 {
		reader = zstd.NewReaderDict(bytes.NewReader(b), zstdDict)
	} else {
		reader = zstd.NewReader(bytes.NewReader(b))
	}

	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrObjectDecodeFailed, err)
	}

	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrReaderCloseFailed, err)
	}

	return buf.Bytes(), nil
}

func copyStruct(input interface{}, output interface{}) error {
	inputString := types.PrintStruct(input)
	if err := json.Unmarshal([]byte(inputString), &output); err != nil {
		return fmt.Errorf("%w: %v", ErrCopyBlockFailed, err)
	}

	return nil
}

func (c *Compressor) encodeAccountCoin(accountCoin *AccountCoin) []byte {
	output := c.pool.Get().Bytes()
	output = append(
		output,
		[]byte(accountCoin.Account.Address)...,
	)
	output = append(
		output,
		unicodeRecordSeparator,
	)
	output = append(
		output,
		[]byte(accountCoin.Coin.CoinIdentifier.Identifier)...,
	)
	output = append(
		output,
		unicodeRecordSeparator,
	)
	output = append(
		output,
		[]byte(types.PrintStruct(accountCoin.Coin.Amount))...,
	)
	if accountCoin.Account.Metadata == nil &&
		accountCoin.Account.SubAccount == nil &&
		accountCoin.Coin.Amount.Metadata == nil &&
		accountCoin.Coin.Amount.Currency.Metadata == nil {
		return output
	}

	output = append(
		output,
		unicodeRecordSeparator,
	)
	if accountCoin.Account.Metadata != nil {
		output = append(
			output,
			[]byte(types.PrintStruct(accountCoin.Account.Metadata))...,
		)
	}
	output = append(
		output,
		unicodeRecordSeparator,
	)

	if accountCoin.Account.SubAccount != nil {
		output = append(
			output,
			[]byte(accountCoin.Account.SubAccount.Address)...,
		)
	}
	output = append(output, unicodeRecordSeparator)

	if accountCoin.Account.SubAccount != nil && accountCoin.Account.SubAccount.Metadata != nil {
		output = append(
			output,
			[]byte(types.PrintStruct(accountCoin.Account.SubAccount.Metadata))...,
		)
	}
	output = append(output, unicodeRecordSeparator)

	if accountCoin.Coin.Amount.Metadata != nil {
		output = append(
			output,
			[]byte(types.PrintStruct(accountCoin.Coin.Amount.Metadata))...,
		)
	}
	output = append(
		output,
		unicodeRecordSeparator,
	)
	if accountCoin.Coin.Amount.Currency.Metadata != nil {
		output = append(
			output,
			[]byte(types.PrintStruct(accountCoin.Coin.Amount.Currency.Metadata))...,
		)
	}

	return output
}

const (
	unicodeRecordSeparator = '\u001E'
)

// AccountCoin Encoding
const (
	accountAddress = iota
	coinIdentifier
	amount
	accountMetadata // If none exist, we stop after amount.
	subAccountAddress
	subAccountMetadata
	amountMetadata
	currencyMetadata
)

var (
	errDecoding = errors.New("decoding")
)

func (c *Compressor) decodeAccountCoin(
	b []byte,
	accountCoin *AccountCoin,
	reclaimInput bool,
) error {
	count := 0
	currentBytes := b
	for {
		nextRune := bytes.IndexRune(currentBytes, unicodeRecordSeparator)
		if nextRune == -1 {
			if count != amount && count != currencyMetadata {
				fmt.Printf("%s\n", string(currentBytes))
				return fmt.Errorf("%w: next rune is -1 at %d", errDecoding, count)
			}

			nextRune = len(currentBytes)
		}

		val := currentBytes[:nextRune]
		if len(val) == 0 {
			goto handleNext
		}

		switch count {
		case accountAddress:
			accountCoin.Account = &types.AccountIdentifier{
				Address: string(val),
			}
		case coinIdentifier:
			accountCoin.Coin = &types.Coin{
				CoinIdentifier: &types.CoinIdentifier{
					Identifier: string(val),
				},
			}
		case amount:
			var a types.Amount
			if err := json.Unmarshal(val, &a); err != nil {
				return fmt.Errorf("%w: amount %s", errDecoding, err.Error())
			}
			accountCoin.Coin.Amount = &a
		case accountMetadata:
			var m map[string]interface{}
			if err := json.Unmarshal(val, &m); err != nil {
				return fmt.Errorf("%w: account metadata %s", errDecoding, err.Error())
			}
			accountCoin.Account.Metadata = m
		case subAccountAddress:
			accountCoin.Account.SubAccount = &types.SubAccountIdentifier{
				Address: string(val),
			}
		case subAccountMetadata:
			if accountCoin.Account.SubAccount == nil {
				return errDecoding // must have address
			}

			var m map[string]interface{}
			if err := json.Unmarshal(val, &m); err != nil {
				return fmt.Errorf("%w: subaccount metadata %s", errDecoding, err.Error())
			}
			accountCoin.Account.SubAccount.Metadata = m
		case amountMetadata:
			var m map[string]interface{}
			if err := json.Unmarshal(val, &m); err != nil {
				return fmt.Errorf("%w: amount metadata %s", errDecoding, err.Error())
			}
			accountCoin.Coin.Amount.Metadata = m
		case currencyMetadata:
			var m map[string]interface{}
			if err := json.Unmarshal(val, &m); err != nil {
				return fmt.Errorf("%w: currency metadata %s", errDecoding, err.Error())
			}
			accountCoin.Coin.Amount.Currency.Metadata = m
		default:
			return fmt.Errorf("%w: count %d > end", errDecoding, count)
		}

	handleNext:
		if nextRune == len(currentBytes) && (count == amount || count == currencyMetadata) {
			break
		}

		currentBytes = currentBytes[nextRune+1:]
		count++
	}

	if reclaimInput {
		c.pool.PutByteSlice(b)
	}

	return nil
}
