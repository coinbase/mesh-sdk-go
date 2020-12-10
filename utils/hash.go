package utils

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
)

// hashBytes returns a hex-encoded sha256 hash of the provided
// byte slice.
func hashBytes(data []byte) (string, error) {
	h := sha256.New()
	_, err := h.Write(data)
	if err != nil {
		return "", fmt.Errorf("%w: unable to hash data %s", err, string(data))
	}

	return fmt.Sprintf("%x", h.Sum(nil)), nil
}

//NOTE: types package also contains similar Hash() but they panic
//instead of returning error.

// Hash returns a deterministic hash for any interface.
// This works because Golang's JSON marshaler sorts all map keys, recursively.
// Source: https://golang.org/pkg/encoding/json/#Marshal
// Inspiration:
// https://github.com/onsi/gomega/blob/c0be49994280db30b6b68390f67126d773bc5558/matchers/match_json_matcher.go#L16
//
// It is important to note that any interface that is a slice
// or contains slices will not be equal if the slice ordering is
// different.
func Hash(i interface{}) (string, error) {
	// Convert interface to JSON object (not necessarily ordered if struct
	// contains json.RawMessage)
	a, err := json.Marshal(i)
	if err != nil {
		return "", fmt.Errorf("%w: unable to marshal %+v", err, i)
	}

	// Convert JSON object to interface (all json.RawMessage converted to go types)
	var b interface{}
	if err := json.Unmarshal(a, &b); err != nil {
		return "", fmt.Errorf("%w: unable to unmarshal %+v", err, a)
	}

	// Convert interface to JSON object (all map keys ordered)
	c, err := json.Marshal(b)
	if err != nil {
		return "", fmt.Errorf("%w: unable to marshal %+v", err, b)
	}

	return hashBytes(c)
}
