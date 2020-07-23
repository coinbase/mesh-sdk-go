package parser

import (
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/asserter"
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
	intent []*types.Operation,
	observed []*types.Operation,
	errExtra bool,
) error {
	matches := make(map[int]struct{})
	unmatched := []*types.Operation{}

	for _, obs := range observed {
		foundMatch := false
		for i, in := range intent {
			if _, exists := matches[i]; exists {
				continue
			}

			if ExpectedOperation(in, obs) == nil {
				matches[i] = struct{}{}
				foundMatch = true
				break
			}
		}

		if !foundMatch {
			if errExtra {
				return fmt.Errorf(
					"found extra operation %s",
					types.PrettyPrintStruct(obs),
				)
			}
			unmatched = append(unmatched, obs)
		}
	}

	for i := 0; i < len(intent); i++ {
		if _, exists := matches[i]; !exists {
			return fmt.Errorf(
				"could not find operation with intent %s in observed",
				types.PrettyPrintStruct(intent[i]),
			)
		}
	}

	if len(unmatched) > 0 {
		log.Printf("found extra operations: %s\n", types.PrettyPrintStruct(unmatched))
	}

	return nil
}

// ExpectedSigners returns an error if a slice of SigningPayload
// has different signers than what was observed (typically populated
// using the signers returned from parsing a transaction).
func ExpectedSigners(intent []*types.SigningPayload, observed []string) error {
	// De-duplicate required signers (ex: multiple UTXOs from same address)
	intendedSigners := make(map[string]struct{})
	for _, payload := range intent {
		intendedSigners[payload.Address] = struct{}{}
	}

	if err := asserter.StringArray("observed signers", observed); err != nil {
		return fmt.Errorf("%w: found duplicate signer", err)
	}

	// Could exist here if len(intent) != len(observed) but
	// more useful to print out a detailed error message.
	seenSigners := make(map[string]struct{})
	unmatched := []string{} // observed
	for _, signer := range observed {
		_, exists := intendedSigners[signer]
		if !exists {
			unmatched = append(unmatched, signer)
		} else {
			seenSigners[signer] = struct{}{}
		}
	}

	for k := range intendedSigners {
		if _, exists := seenSigners[k]; !exists {
			return fmt.Errorf(
				"could not find match for intended signer %s",
				k,
			)
		}
	}

	if len(unmatched) != 0 {
		return fmt.Errorf(
			"found unexpected signers: %s",
			types.PrettyPrintStruct(unmatched),
		)
	}

	return nil
}
