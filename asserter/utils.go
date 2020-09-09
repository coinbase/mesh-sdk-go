package asserter

import (
	"fmt"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// StringArray ensures all strings in an array
// are non-empty strings and not duplicates.
func StringArray(arrName string, arr []string) error {
	if len(arr) == 0 {
		return fmt.Errorf("no %s found", arrName)
	}

	parsed := map[string]struct{}{}
	for _, s := range arr {
		if s == "" {
			return fmt.Errorf("%s has an empty string", arrName)
		}

		if _, ok := parsed[s]; ok {
			return fmt.Errorf("%s contains a duplicate %s", arrName, s)
		}

		parsed[s] = struct{}{}
	}

	return nil
}

// AccountArray ensures all *types.AccountIdentifier in an array
// are valid and not duplicates.
func AccountArray(arrName string, arr []*types.AccountIdentifier) error {
	if len(arr) == 0 {
		return fmt.Errorf("no %s found", arrName)
	}

	parsed := map[string]struct{}{}
	for _, s := range arr {
		if err := AccountIdentifier(s); err != nil {
			return fmt.Errorf("%s has an invalid account identifier", arrName)
		}

		if _, ok := parsed[types.Hash(s)]; ok {
			return fmt.Errorf("%s contains a duplicate %s", arrName, types.PrintStruct(s))
		}

		parsed[types.Hash(s)] = struct{}{}
	}

	return nil
}
