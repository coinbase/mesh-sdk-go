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
}

// CompressorEntry is used to initialize a Compressor.
// All DictionaryPaths are loaded from disk at initialization.
type CompressorEntry struct {
	Namespace      string
	DictionaryPath string
}

// NewCompressor returns a new *Compressor. The dicts
// provided should contain k:v of namespace:zstd dict.
func NewCompressor(entries []*CompressorEntry) (*Compressor, error) {
	dicts := map[string][]byte{}
	for _, entry := range entries {
		b, err := ioutil.ReadFile(path.Clean(entry.DictionaryPath))
		if err != nil {
			return nil, fmt.Errorf(
				"%w: unable to load dictionary from %s",
				err,
				entry.DictionaryPath,
			)
		}

		log.Printf("loaded zstd dictionary for %s\n", entry.Namespace)
		dicts[entry.Namespace] = b
	}

	return &Compressor{
		dicts: dicts,
	}, nil
}

// Encode attempts to compress the object and will use a dict if
// one exists for the namespace.
func (c *Compressor) Encode(namespace string, object interface{}) ([]byte, error) {
	return encode(object, c.dicts[namespace])
}

// Decode attempts to decompress the object and will use a dict if
// one exists for the namespace.
func (c *Compressor) Decode(namespace string, input []byte, object interface{}) error {
	decompressed, err := c.DecodeRaw(namespace, input)
	if err != nil {
		return fmt.Errorf("%w: unable to decompress raw bytes", err)
	}

	if err := getDecoder(bytes.NewReader(decompressed)).Decode(&object); err != nil {
		return fmt.Errorf("%w: unable to decode bytes", err)
	}

	return nil
}

// DecodeRaw only decompresses an input, leaving decoding to the caller.
// This is particularly useful for training a compressor.
func (c *Compressor) DecodeRaw(namespace string, input []byte) ([]byte, error) {
	return decode(input, c.dicts[namespace])
}

func getEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.UseJSONTag(true)

	return enc
}

func encode(object interface{}, zstdDict []byte) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return nil, fmt.Errorf("%w: could not encode object", err)
	}

	var compressed []byte
	if len(zstdDict) > 0 {
		compressBuf := new(bytes.Buffer)
		writer := zstd.NewWriterLevelDict(compressBuf, zstd.DefaultCompression, zstdDict)
		_, err = writer.Write(buf.Bytes())
		if err == nil {
			err = writer.Close()
		}

		compressed = compressBuf.Bytes()
	} else {
		compressed, err = zstd.Compress(nil, buf.Bytes())
	}
	if err != nil {
		return nil, fmt.Errorf("%w: could not compress object", err)
	}

	return compressed, nil
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.UseJSONTag(true)

	return dec
}

func decode(b []byte, zstdDict []byte) ([]byte, error) {
	var decompressed []byte
	var err error
	if len(zstdDict) > 0 {
		reader := zstd.NewReaderDict(bytes.NewReader(b), zstdDict)
		defer reader.Close()

		decompressed, err = ioutil.ReadAll(reader)
	} else {
		decompressed, err = zstd.Decompress(nil, b)
	}
	if err != nil {
		return nil, fmt.Errorf("%w: unable to decompress object", err)
	}

	return decompressed, nil
}

func copyStruct(input interface{}, output interface{}) error {
	inputString := types.PrintStruct(input)
	if err := json.Unmarshal([]byte(inputString), &output); err != nil {
		return fmt.Errorf("%w: unable to copy block", err)
	}

	return nil
}
