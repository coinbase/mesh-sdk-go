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

package encoder

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"path"
	"strconv"

	"github.com/DataDog/zstd"
	msgpack "github.com/vmihailenco/msgpack/v5"

	"github.com/coinbase/rosetta-sdk-go/storage/errors"
	"github.com/coinbase/rosetta-sdk-go/types"
)

const (
	jsonTag = "json"
)

// Encoder handles the encoding/decoding of structs and the
// compression/decompression of data using zstd. Optionally,
// the caller can provide a map of dicts on initialization that
// can be used by zstd. You can read more about these "dicts" here:
// https://github.com/facebook/zstd#the-case-for-small-data-compression.
//
// NOTE: If you change these dicts, you will not be able
// to decode previously encoded data. For many users, providing
// no dicts is sufficient!
type Encoder struct {
	compressionDicts map[string][]byte
	pool             *BufferPool
	compress         bool
}

// CompressorEntry is used to initialize a dictionary compression.
// All DictionaryPaths are loaded from disk at initialization.
type CompressorEntry struct {
	Namespace      string
	DictionaryPath string
}

// NewEncoder returns a new *Encoder. The dicts
// provided should contain k:v of namespace:zstd dict.
func NewEncoder(
	entries []*CompressorEntry,
	pool *BufferPool,
	compress bool,
) (*Encoder, error) {
	dicts := map[string][]byte{}
	for _, entry := range entries {
		b, err := ioutil.ReadFile(path.Clean(entry.DictionaryPath))
		if err != nil {
			return nil, fmt.Errorf(
				"unable to load dictionary %s: %w",
				entry.DictionaryPath,
				err,
			)
		}

		log.Printf("loaded zstd dictionary for %s\n", entry.Namespace)
		dicts[entry.Namespace] = b
	}

	return &Encoder{
		compressionDicts: dicts,
		pool:             pool,
		compress:         compress,
	}, nil
}

func getEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.SetCustomStructTag(jsonTag)

	return enc
}

// Encode attempts to compress the object and will use a dict if
// one exists for the namespace.
func (e *Encoder) Encode(namespace string, object interface{}) ([]byte, error) {
	buf := e.pool.Get()
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return nil, fmt.Errorf("unable to encode object: %w", err)
	}

	if !e.compress {
		return buf.Bytes(), nil
	}

	output, err := e.EncodeRaw(namespace, buf.Bytes())
	if err != nil {
		return nil, fmt.Errorf("unable to compress raw bytes: %w", err)
	}

	e.pool.Put(buf)
	return output, nil
}

// EncodeRaw only compresses an input, leaving encoding to the caller.
// This is particularly useful for training a compressor.
func (e *Encoder) EncodeRaw(namespace string, input []byte) ([]byte, error) {
	return e.encode(input, e.compressionDicts[namespace])
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.SetCustomStructTag(jsonTag)

	return dec
}

// Decode attempts to decompress the object and will use a dict if
// one exists for the namespace.
func (e *Encoder) Decode(
	namespace string,
	input []byte,
	object interface{},
	reclaimInput bool,
) error {
	if e.compress {
		decompressed, err := e.DecodeRaw(namespace, input)
		if err != nil {
			return fmt.Errorf("unable to decompress raw bytes: %w", err)
		}

		if err := getDecoder(bytes.NewReader(decompressed)).Decode(&object); err != nil {
			return fmt.Errorf("unable to decode object: %w", err)
		}

		e.pool.PutByteSlice(decompressed)
	} else { // nolint:gocritic
		if err := getDecoder(bytes.NewReader(input)).Decode(&object); err != nil {
			return fmt.Errorf("unable to decode object: %w", err)
		}
	}

	if reclaimInput {
		e.pool.PutByteSlice(input)
	}

	return nil
}

// DecodeRaw only decompresses an input, leaving decoding to the caller.
// This is particularly useful for training a compressor.
func (e *Encoder) DecodeRaw(namespace string, input []byte) ([]byte, error) {
	return e.decode(input, e.compressionDicts[namespace])
}

func (e *Encoder) encode(input []byte, zstdDict []byte) ([]byte, error) {
	buf := e.pool.Get()
	var writer io.WriteCloser
	if len(zstdDict) > 0 {
		writer = zstd.NewWriterLevelDict(buf, zstd.DefaultCompression, zstdDict)
	} else {
		writer = zstd.NewWriter(buf)
	}
	if _, err := writer.Write(input); err != nil {
		return nil, fmt.Errorf("unable to write to buffer: %w", err)
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("unable to close writer: %w", err)
	}

	return buf.Bytes(), nil
}

func (e *Encoder) decode(b []byte, zstdDict []byte) ([]byte, error) {
	buf := e.pool.Get()
	var reader io.ReadCloser
	if len(zstdDict) > 0 {
		reader = zstd.NewReaderDict(bytes.NewReader(b), zstdDict)
	} else {
		reader = zstd.NewReader(bytes.NewReader(b))
	}

	if _, err := buf.ReadFrom(reader); err != nil {
		return nil, fmt.Errorf("unable to decode object: %w", err)
	}

	if err := reader.Close(); err != nil {
		return nil, fmt.Errorf("unable to close reader: %w", err)
	}

	return buf.Bytes(), nil
}

// CopyStruct performs a deep copy of an entire struct
// using its JSON representation.
func CopyStruct(input interface{}, output interface{}) error {
	inputString := types.PrintStruct(input)
	if err := json.Unmarshal([]byte(inputString), &output); err != nil {
		return fmt.Errorf("unable to copy block: %w", err)
	}

	return nil
}

const (
	unicodeRecordSeparator = '\u001E'
)

func (e *Encoder) encodeAndWrite(output *bytes.Buffer, object interface{}) error {
	buf := e.pool.Get()
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return fmt.Errorf("unable to encode object: %w", err)
	}

	if _, err := output.Write(buf.Bytes()); err != nil {
		return fmt.Errorf("unable to write buffer: %w", err)
	}

	e.pool.Put(buf)
	return nil
}

func (e *Encoder) decodeMap(input []byte) (map[string]interface{}, error) {
	var m map[string]interface{}
	if err := getDecoder(bytes.NewReader(input)).Decode(&m); err != nil {
		return nil, fmt.Errorf("unable to decode object: %w", err)
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
func (e *Encoder) EncodeAccountCoin( // nolint:gocognit
	accountCoin *types.AccountCoin,
) ([]byte, error) {
	output := e.pool.Get()
	if _, err := output.WriteString(accountCoin.Account.Address); err != nil {
		return nil, fmt.Errorf("unable to write account coin account address to buffer: %w", err)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(accountCoin.Coin.CoinIdentifier.Identifier); err != nil {
		return nil, fmt.Errorf("unable to write account oin coin identifier to buffer: %w", err)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(accountCoin.Coin.Amount.Value); err != nil {
		return nil, fmt.Errorf("unable to write account coin amount value to buffer: %w", err)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(accountCoin.Coin.Amount.Currency.Symbol); err != nil {
		return nil, fmt.Errorf(
			"unable to write account coin amount currency symbol to buffer: %w",
			err,
		)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(
		strconv.FormatInt(int64(accountCoin.Coin.Amount.Currency.Decimals), 10),
	); err != nil {
		return nil, fmt.Errorf(
			"unable to write account coin amount currency decimals to buffer: %w",
			err,
		)
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
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if accountCoin.Account.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCoin.Account.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account coin account metadata to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCoin.Account.SubAccount != nil {
		if _, err := output.WriteString(accountCoin.Account.SubAccount.Address); err != nil {
			return nil, fmt.Errorf(
				"unable to write account coin account sub account address: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCoin.Account.SubAccount != nil && accountCoin.Account.SubAccount.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCoin.Account.SubAccount.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account coin account sub account metadata to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCoin.Coin.Amount.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCoin.Coin.Amount.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account coin amount metadata to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if accountCoin.Coin.Amount.Currency.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCoin.Coin.Amount.Currency.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account coin amount currency metadata: %w",
				err,
			)
		}
	}

	return output.Bytes(), nil
}

// DecodeAccountCoin decodes an AccountCoin and optionally
// reclaims the memory associated with the input.
func (e *Encoder) DecodeAccountCoin( // nolint:gocognit
	b []byte,
	accountCoin *types.AccountCoin,
	reclaimInput bool,
) error {
	// Indices of encoded AccountCoin struct
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

	count := 0
	currentBytes := b
	for {
		nextRune := bytes.IndexRune(currentBytes, unicodeRecordSeparator)
		if nextRune == -1 {
			if count != amountCurrencyDecimals && count != currencyMetadata {
				return fmt.Errorf("next rune is -1 at %d: %w", count, errors.ErrRawDecodeFailed)
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
				return fmt.Errorf("unable to parse int: %w", err)
			}

			accountCoin.Coin.Amount.Currency.Decimals = int32(i)
		case accountMetadata:
			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf("unable to decode map: %w", err)
			}

			accountCoin.Account.Metadata = m
		case subAccountAddress:
			accountCoin.Account.SubAccount = &types.SubAccountIdentifier{
				Address: string(val),
			}
		case subAccountMetadata:
			if accountCoin.Account.SubAccount == nil {
				return errors.ErrRawDecodeFailed // must have address
			}

			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf(
					"unable to decode map: %w",
					err,
				)
			}

			accountCoin.Account.SubAccount.Metadata = m
		case amountMetadata:
			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf("unable to decode map: %w", err)
			}

			accountCoin.Coin.Amount.Metadata = m
		case currencyMetadata:
			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf(
					"unable to decode map: %w",
					err,
				)
			}

			accountCoin.Coin.Amount.Currency.Metadata = m
		default:
			return fmt.Errorf("count %d > end: %w", count, errors.ErrRawDecodeFailed)
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
		e.pool.PutByteSlice(b)
	}

	return nil
}

// EncodeAccountCurrency is used to encode an AccountCurrency using the scheme (on the happy path):
// accountAddress|currencySymbol|currencyDecimals
//
// And the following scheme on the unhappy path:
// accountAddress|currencySymbol|currencyDecimals|accountMetadata|
// subAccountAddress|subAccountMetadata|currencyMetadata
//
// In both cases, the | character is represented by the unicodeRecordSeparator rune.
func (e *Encoder) EncodeAccountCurrency( // nolint:gocognit
	accountCurrency *types.AccountCurrency,
) ([]byte, error) {
	output := e.pool.Get()
	if _, err := output.WriteString(accountCurrency.Account.Address); err != nil {
		return nil, fmt.Errorf(
			"unable to write account currency account address to buffer: %w",
			err,
		)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(accountCurrency.Currency.Symbol); err != nil {
		return nil, fmt.Errorf(
			"unable to write account currency currency symbol to buffer: %w",
			err,
		)
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if _, err := output.WriteString(
		strconv.FormatInt(int64(accountCurrency.Currency.Decimals), 10),
	); err != nil {
		return nil, fmt.Errorf(
			"unable to write account currency currency decimals to buffer: %w",
			err,
		)
	}

	// Exit early if we don't have any complex data to record (this helps
	// us save a lot of space on the happy path).
	if accountCurrency.Account.Metadata == nil &&
		accountCurrency.Account.SubAccount == nil &&
		accountCurrency.Currency.Metadata == nil {
		return output.Bytes(), nil
	}

	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}
	if accountCurrency.Account.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCurrency.Account.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account currency account metadata to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCurrency.Account.SubAccount != nil {
		if _, err := output.WriteString(accountCurrency.Account.SubAccount.Address); err != nil {
			return nil, fmt.Errorf(
				"unable to write account currency account sub account to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCurrency.Account.SubAccount != nil &&
		accountCurrency.Account.SubAccount.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCurrency.Account.SubAccount.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to encode and write account currency account sub account metadata to buffer: %w",
				err,
			)
		}
	}
	if _, err := output.WriteRune(unicodeRecordSeparator); err != nil {
		return nil, fmt.Errorf("unable to write unicode record separator to buffer: %w", err)
	}

	if accountCurrency.Currency.Metadata != nil {
		if err := e.encodeAndWrite(output, accountCurrency.Currency.Metadata); err != nil {
			return nil, fmt.Errorf(
				"unable to write account currency currency metadata to buffer: %w",
				err,
			)
		}
	}

	return output.Bytes(), nil
}

// DecodeAccountCurrency decodes an AccountCurrency and optionally
// reclaims the memory associated with the input.
func (e *Encoder) DecodeAccountCurrency( // nolint:gocognit
	b []byte,
	accountCurrency *types.AccountCurrency,
	reclaimInput bool,
) error {
	// Indices of encoded AccountCurrency struct
	const (
		accountAddress = iota
		currencySymbol
		currencyDecimals

		// If none exist below, we stop after amount.
		accountMetadata
		subAccountAddress
		subAccountMetadata
		currencyMetadata
	)

	count := 0
	currentBytes := b
	for {
		nextRune := bytes.IndexRune(currentBytes, unicodeRecordSeparator)
		if nextRune == -1 {
			if count != currencyDecimals && count != currencyMetadata {
				return fmt.Errorf("next rune is -1 at %d: %w", count, errors.ErrRawDecodeFailed)
			}

			nextRune = len(currentBytes)
		}

		val := currentBytes[:nextRune]
		if len(val) == 0 {
			goto handleNext
		}

		switch count {
		case accountAddress:
			accountCurrency.Account = &types.AccountIdentifier{
				Address: string(val),
			}
		case currencySymbol:
			accountCurrency.Currency = &types.Currency{
				Symbol: string(val),
			}
		case currencyDecimals:
			i, err := strconv.ParseInt(string(val), 10, 32)
			if err != nil {
				return fmt.Errorf("unable to parse int for %s: %w", string(val), err)
			}

			accountCurrency.Currency.Decimals = int32(i)
		case accountMetadata:
			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf("unable to decode map: %w", err)
			}

			accountCurrency.Account.Metadata = m
		case subAccountAddress:
			accountCurrency.Account.SubAccount = &types.SubAccountIdentifier{
				Address: string(val),
			}
		case subAccountMetadata:
			if accountCurrency.Account.SubAccount == nil {
				return errors.ErrRawDecodeFailed // must have address
			}

			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf("unable to decode map: %w", err)
			}

			accountCurrency.Account.SubAccount.Metadata = m
		case currencyMetadata:
			m, err := e.decodeMap(val)
			if err != nil {
				return fmt.Errorf("unable to decode map: %w", err)
			}

			accountCurrency.Currency.Metadata = m
		default:
			return fmt.Errorf("count %d > end: %w", count, errors.ErrRawDecodeFailed)
		}

	handleNext:
		if nextRune == len(currentBytes) &&
			(count == currencyDecimals || count == currencyMetadata) {
			break
		}

		currentBytes = currentBytes[nextRune+1:]
		count++
	}

	if reclaimInput {
		e.pool.PutByteSlice(b)
	}

	return nil
}
