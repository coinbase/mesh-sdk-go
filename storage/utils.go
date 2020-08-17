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
	"fmt"
	"io"

	msgpack "github.com/vmihailenco/msgpack/v5"
)

func getEncoder(w io.Writer) *msgpack.Encoder {
	enc := msgpack.NewEncoder(w)
	enc.UseJSONTag(true)

	return enc
}

func encode(object interface{}) ([]byte, error) {
	buf := new(bytes.Buffer)
	err := getEncoder(buf).Encode(object)
	if err != nil {
		return nil, fmt.Errorf("%w: could not encode object", err)
	}

	return buf.Bytes(), nil
}

func getDecoder(r io.Reader) *msgpack.Decoder {
	dec := msgpack.NewDecoder(r)
	dec.UseJSONTag(true)

	return dec
}

func decode(b []byte, object interface{}) error {
	err := getDecoder(bytes.NewReader(b)).Decode(&object)
	if err != nil {
		return fmt.Errorf("%w: unable to decode bytes", err)
	}

	return nil
}
