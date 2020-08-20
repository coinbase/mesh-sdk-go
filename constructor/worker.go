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

package constructor

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/lucasjones/reggen"
	"github.com/tidwall/sjson"
)

// NewWorker returns a new Worker.
func NewWorker(helper WorkerHelper) *Worker {
	return &Worker{helper: helper}
}

func unmarshalInput(input []byte, output interface{}) error {
	// To prevent silent erroring, we explicitly
	// reject any unknown fields.
	dec := json.NewDecoder(bytes.NewReader(input))
	dec.DisallowUnknownFields()

	if err := dec.Decode(&output); err != nil {
		return fmt.Errorf("%w: unable to unmarshal", err)
	}

	return nil
}

func marshalString(value string) string {
	return fmt.Sprintf(`"%s"`, value)
}

func (w *Worker) invokeWorker(
	ctx context.Context,
	action ActionType,
	input string,
) (string, error) {
	switch action {
	case SetVariable:
		return input, nil
	case GenerateKey:
		return GenerateKeyWorker(input)
	case Derive:
		return w.DeriveWorker(ctx, input)
	case SaveAddress:
		return "", w.SaveAddressWorker(ctx, input)
	case PrintMessage:
		PrintMessageWorker(input)
		return "", nil
	case RandomString:
		return RandomStringWorker(input)
	case Math:
		return MathWorker(input)
	case FindBalance:
		return w.FindBalanceWorker(ctx, input)
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidActionType, action)
	}
}

func (w *Worker) actions(ctx context.Context, state string, actions []*Action) (string, error) {
	for _, action := range actions {
		processedInput, err := PopulateInput(state, action.Input)
		if err != nil {
			return "", fmt.Errorf("%w: unable to populate variables", err)
		}

		output, err := w.invokeWorker(ctx, action.Type, processedInput)
		if err != nil {
			return "", fmt.Errorf("%w: unable to process action", err)
		}

		if len(output) == 0 {
			continue
		}

		// Update state at the specified output path if there is an output.
		state, err = sjson.SetRaw(state, action.OutputPath, output)
		if err != nil {
			return "", fmt.Errorf("%w: unable to update state", err)
		}
	}

	return state, nil
}

// ProcessNextScenario performs the actions in the next available
// scenario.
func (w *Worker) ProcessNextScenario(
	ctx context.Context,
	j *Job,
) error {
	scenario := j.Scenarios[j.Index]
	newState, err := w.actions(ctx, j.State, scenario.Actions)
	if err != nil {
		return fmt.Errorf("%w: unable to process %s actions", err, scenario.Name)
	}

	j.State = newState
	j.Index++
	return nil
}

// DeriveWorker attempts to derive an address given a
// *types.ConstructionDeriveRequest input.
func (w *Worker) DeriveWorker(
	ctx context.Context,
	rawInput string,
) (string, error) {
	var input types.ConstructionDeriveRequest
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := asserter.PublicKey(input.PublicKey); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	address, metadata, err := w.helper.Derive(
		ctx,
		input.NetworkIdentifier,
		input.PublicKey,
		input.Metadata,
	)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return types.PrintStruct(types.ConstructionDeriveResponse{
		Address:  address,
		Metadata: metadata,
	}), nil
}

// GenerateKeyWorker attempts to generate a key given a
// *GenerateKeyInput input.
func GenerateKeyWorker(rawInput string) (string, error) {
	var input GenerateKeyInput
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	kp, err := keys.GenerateKeypair(input.CurveType)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return types.PrintStruct(kp), nil
}

// SaveAddressWorker saves an address and associated KeyPair
// in KeyStorage.
func (w *Worker) SaveAddressWorker(ctx context.Context, rawInput string) error {
	var input SaveAddressInput
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if len(input.Address) == 0 {
		return fmt.Errorf("%w: %s", ErrInvalidInput, "empty address")
	}

	if err := w.helper.StoreKey(ctx, input.Address, input.KeyPair); err != nil {
		return fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return nil
}

// PrintMessageWorker logs some message to stdout.
func PrintMessageWorker(message string) {
	log.Printf("Message: %s\n", message)
}

// RandomStringWorker generates a string that complies
// with the provided regex input.
func RandomStringWorker(rawInput string) (string, error) {
	var input RandomStringInput
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	output, err := reggen.Generate(input.Regex, input.Limit)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return marshalString(output), nil
}

// MathWorker performs some MathOperation on 2 numbers.
func MathWorker(rawInput string) (string, error) {
	var input MathInput
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	var result string
	switch input.Operation {
	case Addition:
		result, err = types.AddValues(input.LeftValue, input.RightValue)
	case Subtraction:
		result, err = types.SubtractValues(input.LeftValue, input.RightValue)
	default:
		return "", fmt.Errorf("%s is not a supported math operation", input.Operation)
	}
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return marshalString(result), nil
}

func (w *Worker) FindBalanceWorker(ctx context.Context, rawInput string) (string, error) {
	var input FindBalanceInput
	err := unmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	addresses, err := w.helper.AllAddresses(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get all addresses %s", ErrActionFailed, err.Error())
	}

	// If there are no addresses, we should create one.
	if len(addresses) == 0 {
		return "", ErrCreateAccount
	}

	// get available addresses
	unlockedAddresses := []string{}
	lockedAddresses, err := w.helper.LockedAddresses(ctx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get locked addresses %s", ErrActionFailed, err)
	}

	// Convert to a map so can do fast lookups
	lockedSet := map[string]struct{}{}
	for _, address := range lockedAddresses {
		lockedSet[address] = struct{}{}
	}

	for _, address := range addresses {
		if _, exists := lockedSet[address]; !exists {
			unlockedAddresses = append(unlockedAddresses, address)
		}
	}

	// get balances of each unlocked address
	// -> exit as soon as conditions met
	for _, address := range unlockedAddresses {
		if len(input.Address) != 0 && address != input.Address {
			continue
		}

		// skip if in NotAccount
		if utils.ContainsString(input.NotAddress, address) {
			continue
		}

		acct := &types.AccountIdentifier{
			Address:    address,
			SubAccount: input.SubAccount,
		}
		if input.RequireCoin {
			coins, err := w.helper.Coins(ctx, acct)
			if err != nil {
				return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
			}

			disallowedCoins := []string{}
			for _, coinIdentifier := range input.NotCoins {
				disallowedCoins = append(disallowedCoins, types.Hash(coinIdentifier))
			}

			for _, coin := range coins {
				if utils.ContainsString(disallowedCoins, types.Hash(coin.CoinIdentifier)) {
					continue
				}

				if types.Hash(coin.Amount.Currency) != types.Hash(input.MinimumBalance.Currency) {
					continue
				}

				diff, err := types.SubtractValues(coin.Amount.Value, input.MinimumBalance.Value)
				if err != nil {
					return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
				}

				bigIntDiff, err := types.BigInt(diff)
				if err != nil {
					return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
				}

				if bigIntDiff.Sign() < 0 {
					continue
				}

				return types.PrintStruct(FindBalanceOutput{
					Account: acct,
					Balance: coin.Amount,
					Coin:    coin.CoinIdentifier,
				}), nil
			}
		} else {
			amounts, err := w.helper.Balance(ctx, acct)
			if err != nil {
				return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
			}

			// TODO: look for amounts > min
			for _, amount := range amounts {
				diff, err := types.SubtractValues(amount.Value, input.MinimumBalance.Value)
				if err != nil {
					return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
				}

				bigIntDiff, err := types.BigInt(diff)
				if err != nil {
					return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
				}

				if bigIntDiff.Sign() < 0 {
					continue
				}

				return types.PrintStruct(FindBalanceOutput{
					Account: acct,
					Balance: amount,
				}), nil
			}
		}
	}

	// if wait == true, loop
	if input.Wait {
		// TODO: better log message
		log.Printf("waiting for balance...\n")
		time.Sleep(5 * time.Second)
		return w.FindBalanceWorker(ctx, rawInput)
	}

	// if create -1, exit ErrUnsatisfiable
	if input.Create == -1 {
		return "", ErrUnsatisfiable
	}

	// if totalaccount < create, return ErrCreateAddress
	return "", ErrCreateAccount
}
