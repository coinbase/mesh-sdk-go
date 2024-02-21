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

// Vendored from: https://github.com/DmitriyVTitov/size

// Package size implements run-time calculation of size of the variable.
// Source code is based on "binary.Size()" function from Go standard library.
// size.Of() omits size of slices, arrays and maps containers itself (24, 24 and 8 bytes).
// When counting maps separate calculations are done for keys and values.
import (
	"reflect"
)

const (
	bytesInKb = float64(1024) // nolint:gomnd
)

// BtoMb converts B to MB.
func BtoMb(b float64) float64 {
	return b / bytesInKb / bytesInKb
}

// SizeOf returns the size of 'v' in bytes.
// If there is an error during calculation, Of returns -1.
func SizeOf(v interface{}) int {
	cache := make(map[uintptr]bool) // cache with every visited Pointer for recursion detection
	return sizeOf(reflect.Indirect(reflect.ValueOf(v)), cache)
}

// sizeOf returns the number of bytes the actual data represented by v occupies in memory.
// If there is an error, sizeOf returns -1.
func sizeOf(v reflect.Value, cache map[uintptr]bool) int { // nolint:gocognit
	switch v.Kind() {
	case reflect.Array:
		fallthrough
	case reflect.Slice:
		// return 0 if this node has been visited already (infinite recursion)
		if v.Kind() != reflect.Array && cache[v.Pointer()] {
			return 0
		}
		if v.Kind() != reflect.Array {
			cache[v.Pointer()] = true
		}
		sum := 0
		for i := 0; i < v.Len(); i++ {
			s := sizeOf(v.Index(i), cache)
			if s < 0 {
				return -1
			}
			sum += s
		}
		return sum + int(v.Type().Size())
	case reflect.Struct:
		sum := 0
		for i, n := 0, v.NumField(); i < n; i++ {
			s := sizeOf(v.Field(i), cache)
			if s < 0 {
				return -1
			}
			sum += s
		}
		return sum
	case reflect.String:
		return len(v.String()) + int(v.Type().Size())
	case reflect.Ptr:
		// return Ptr size if this node has been visited already (infinite recursion)
		if cache[v.Pointer()] {
			return int(v.Type().Size())
		}
		cache[v.Pointer()] = true
		if v.IsNil() {
			return int(reflect.New(v.Type()).Type().Size())
		}
		s := sizeOf(reflect.Indirect(v), cache)
		if s < 0 {
			return -1
		}
		return s + int(v.Type().Size())
	case reflect.Bool,
		reflect.Uint8, reflect.Uint16, reflect.Uint32, reflect.Uint64,
		reflect.Int8, reflect.Int16, reflect.Int32, reflect.Int64,
		reflect.Int,
		reflect.Chan,
		reflect.Float32, reflect.Float64, reflect.Complex64, reflect.Complex128:
		return int(v.Type().Size())
	case reflect.Map:
		// return 0 if this node has been visited already (infinite recursion)
		if cache[v.Pointer()] {
			return 0
		}
		cache[v.Pointer()] = true
		sum := 0
		keys := v.MapKeys()
		for i := range keys {
			val := v.MapIndex(keys[i])
			// calculate size of key and value separately
			sv := sizeOf(val, cache)
			if sv < 0 {
				return -1
			}
			sum += sv
			sk := sizeOf(keys[i], cache)
			if sk < 0 {
				return -1
			}
			sum += sk
		}
		return sum + int(v.Type().Size())
	case reflect.Interface:
		return sizeOf(v.Elem(), cache) + int(v.Type().Size())
	}

	return -1
}
