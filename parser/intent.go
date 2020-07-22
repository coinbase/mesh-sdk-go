package parser

import (
	"context"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ExpectedOperation returns an error if an observed operation
// differs from the intended operation. An operation is considered
// to be different from the intent if the AccountIdentifier,
// Amount, or Type has changed.
func ExpectedOperation(intent *types.Operation, observed *types.Operation) error {
	if types.Hash(intent.Account) != types.Hash(observed.Account) {
		return fmt.Errorf(
			"intended account %s did not match observed account %s",
			types.PrettyPrintStruct(intent.Account),
			types.PrettyPrintStruct(observed.Account),
		)
	}

	if types.Hash(intent.Amount) != types.Hash(observed.Amount) {
		return fmt.Errorf(
			"intended amount %s did not match observed amount %s",
			types.PrettyPrintStruct(intent.Amount),
			types.PrettyPrintStruct(observed.Amount),
		)
	}

	if intent.Type != observed.Type {
		return fmt.Errorf(
			"intended type %s did not match observed type %s",
			intent.Type,
			observed.Type,
		)
	}

	return nil
}

// ExpectedOperations returns an error if a slice of intended
// operations differ from observed operations. Optionally,
// it is possible to error if any extra observed opertions
// are found.
func ExpectedOperations(
	ctx context.Context,
	intent []*types.Operation,
	observed []*types.Operation,
	errExtra bool,
) error {
	matches := map[int]int{} // mapping from intent -> observed
	unmatched := []int{}     // observed

	for o, obs := range observed {
		foundMatch := false
		for i, in := range intent {
			if _, exists := matches[i]; exists {
				continue
			}

			if ExpectedOperation(in, obs) == nil {
				matches[i] = o
				foundMatch = true
				break
			}
		}

		if !foundMatch {
			if errExtra {
				return fmt.Errorf(
					"found extra operation %s",
					types.PrettyPrintStruct(o),
				)
			}
			unmatched = append(unmatched, o)
		}
	}

	if len(matches) != len(intent) {
		for i := 0; i < len(intent); i++ {
			if _, exists := matches[i]; !exists {
				return fmt.Errorf(
					"could not find operation with intent %s in observed",
					types.PrettyPrintStruct(intent[i]),
				)
			}
		}
	}

	for _, extra := range unmatched {
		log.Printf("found extra operation %s\n", types.PrettyPrintStruct(observed[extra]))
	}

	return nil
}

// ExpectedSigners returns an error if a slice of SigningPayload
// has different signers than what was observed (typically populated
// using the signers returned from parsing a transaction).
func ExpectedSigners(intent []*types.SigningPayload, observed []string) error {
	matches := map[int]int{} // requested -> observed
	unmatched := []int{}     // observed
	for o, observed := range observed {
		foundMatch := false
		for r, req := range intent {
			if _, exists := matches[r]; exists {
				continue
			}

			if req.Address == observed {
				matches[r] = o
				foundMatch = true
				break
			}
		}

		if !foundMatch {
			unmatched = append(unmatched, o)
		}
	}

	if len(matches) != len(intent) {
		for i := 0; i < len(intent); i++ {
			if _, exists := matches[i]; !exists {
				return fmt.Errorf(
					"could not find signer with address %s in observed",
					types.PrettyPrintStruct(intent[i]),
				)
			}
		}
	}

	if len(unmatched) != 0 {
		unexpected := []string{}
		for _, m := range unmatched {
			unexpected = append(unexpected, observed[m])
		}

		return fmt.Errorf(
			"found unexpected signers: %s",
			types.PrettyPrintStruct(unexpected),
		)
	}

	return nil
}
