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
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"strconv"

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

const (
	unicodeRecordSeparator = '\u001E'
)

// Indexes of encoded AccountCoin struct
const (
	accountAddress = iota
	coinIdentifier
	amountValue
	amountCurrencySymbol
	amountCurrencyDecimals

	// If none exist below, we stop after amount.
	accountMetadata
	subAccountAddress
	subAccountMetadata
	amountMetadata
	currencyMetadata
)

func (c *Compressor) encodeAndWrite(output *bytes.Buffer, object interface{}) error {
	buf := c.pool.Get()
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return fmt.Errorf("%w: %v", ErrObjectEncodeFailed, err)
	}

	if _, err := output.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("%w: %v", ErrObjectEncodeFailed, err)
	}

	c.pool.Put(buf)
	return nil
}

func (c *Compressor) decodeMap(input []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := getDecoder(bytes.NewReader(input)).Decode(&m); err != nil {
		return nil, fmt.Errorf("%w: %v", ErrRawDecodeFailed, err)
	}

	return m, nil
}

// EncodeAccountCoin is used to encode an *AccountCoin using the scheme (on the happy path):
// accountAddress|coinIdentifier|amountValue|amountCurrencySymbol|
// amountCurrencyDecimals
//
// And the following scheme on the unhappy path:
// accountAddress|coinIdentifier|amountValue|amountCurrencySymbol|
// amountCurrencyDecimals|accountMetadata|subAccountAddress|
// subAccountMetadata|amountMetadata|currencyMetadata
//
// In both cases, the | character is represented by the unicodeRecordSeparator rune.
func (c *Compressor) EncodeAccountCoin( // nolint:gocognit
	accountCoin *AccountCoin,
) ([]byte, error) {
	output := c.pool.Get()
	if _, err := output.WriteString(accountCoin.Account.Address); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteString(accountCoin.Coin.CoinIdentifier.Identifier); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteString(accountCoin.Coin.Amount.Value); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteString(accountCoin.Coin.Amount.Currency.Symbol); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if _, err := output.WriteString(
		strconv.FormatInt(int64(accountCoin.Coin.Amount.Currency.Decimals), 10),
	); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}

	// Exit early if we don't have any complex data to record (this helps
	// us save a lot of space on the happy path).
	if accountCoin.Account.Metadata == nil &&
		accountCoin.Account.SubAccount == nil &&
		accountCoin.Coin.Amount.Metadata == nil &&
		accountCoin.Coin.Amount.Currency.Metadata == nil {
		return output.Bytes(), nil
	}

	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if accountCoin.Account.Metadata != nil {
		if err := c.encodeAndWrite(output, accountCoin.Account.Metadata); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}

	if accountCoin.Account.SubAccount != nil {
		if _, err := output.WriteString(accountCoin.Account.SubAccount.Address); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}

	if accountCoin.Account.SubAccount != nil && accountCoin.Account.SubAccount.Metadata != nil {
		if err := c.encodeAndWrite(output, accountCoin.Account.SubAccount.Metadata); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}

	if accountCoin.Coin.Amount.Metadata != nil {
		if err := c.encodeAndWrite(output, accountCoin.Coin.Amount.Metadata); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
	}
	if accountCoin.Coin.Amount.Currency.Metadata != nil {
		if err := c.encodeAndWrite(output, accountCoin.Coin.Amount.Currency.Metadata); err != nil {
			return nil, fmt.Errorf("%w: %s", ErrObjectEncodeFailed, err.Error())
		}
	}

	return output.Bytes(), nil
}

// DecodeAccountCoin decodes an AccountCoin and optionally
// reclaims the memory associated with the input.
func (c *Compressor) DecodeAccountCoin( // nolint:gocognit
	b []byte,
	accountCoin *AccountCoin,
	reclaimInput bool,
) error {
	count := 0
	currentBytes := b
	for {
		nextRune := bytes.IndexRune(currentBytes, unicodeRecordSeparator)
		if nextRune == -1 {
			if count != amountCurrencyDecimals && count != currencyMetadata {
				return fmt.Errorf("%w: next rune is -1 at %d", ErrRawDecodeFailed, count)
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
		case amountValue:
			accountCoin.Coin.Amount = &types.Amount{
				Value: string(val),
			}
		case amountCurrencySymbol:
			accountCoin.Coin.Amount.Currency = &types.Currency{
				Symbol: string(val),
			}
		case amountCurrencyDecimals:
			i, err := strconv.ParseInt(string(val), 10, 32)
			if err != nil {
				return fmt.Errorf("%w: %s", ErrRawDecodeFailed, err.Error())
			}

			accountCoin.Coin.Amount.Currency.Decimals = int32(i)
		case accountMetadata:
			m, err := c.decodeMap(val)
			if err != nil {
				return fmt.Errorf("%w: account metadata %s", ErrRawDecodeFailed, err.Error())
			}

			accountCoin.Account.Metadata = m
		case subAccountAddress:
			accountCoin.Account.SubAccount = &types.SubAccountIdentifier{
				Address: string(val),
			}
		case subAccountMetadata:
			if accountCoin.Account.SubAccount == nil {
				return ErrRawDecodeFailed // must have address
			}

			m, err := c.decodeMap(val)
			if err != nil {
				return fmt.Errorf("%w: subaccount metadata %s", ErrRawDecodeFailed, err.Error())
			}

			accountCoin.Account.SubAccount.Metadata = m
		case amountMetadata:
			m, err := c.decodeMap(val)
			if err != nil {
				return fmt.Errorf("%w: amount metadata %s", ErrRawDecodeFailed, err.Error())
			}

			accountCoin.Coin.Amount.Metadata = m
		case currencyMetadata:
			m, err := c.decodeMap(val)
			if err != nil {
				return fmt.Errorf("%w: currency metadata %s", ErrRawDecodeFailed, err.Error())
			}

			accountCoin.Coin.Amount.Currency.Metadata = m
		default:
			return fmt.Errorf("%w: count %d > end", ErrRawDecodeFailed, count)
		}

	handleNext:
		if nextRune == len(currentBytes) &&
			(count == amountCurrencyDecimals || count == currencyMetadata) {
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
