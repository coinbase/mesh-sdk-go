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

package worker

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"

	"github.com/lucasjones/reggen"
	"github.com/tidwall/sjson"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage/database"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"
)

// New returns a new *Worker.
func New(helper Helper) *Worker {
	return &Worker{helper: helper}
}

func marshalString(value string) string {
	return fmt.Sprintf(`"%s"`, value)
}

func (w *Worker) invokeWorker(
	ctx context.Context,
	dbTx database.Transaction,
	action job.ActionType,
	input string,
) (string, error) {
	switch action {
	case job.SetVariable:
		return input, nil
	case job.GenerateKey:
		return GenerateKeyWorker(input)
	case job.Derive:
		return w.DeriveWorker(ctx, input)
	case job.SaveAccount:
		return "", w.SaveAccountWorker(ctx, dbTx, input)
	case job.PrintMessage:
		PrintMessageWorker(input)
		return "", nil
	case job.RandomString:
		return RandomStringWorker(input)
	case job.Math:
		return MathWorker(input)
	case job.FindBalance:
		return w.FindBalanceWorker(ctx, dbTx, input)
	case job.RandomNumber:
		return RandomNumberWorker(input)
	case job.Assert:
		return "", AssertWorker(input)
	case job.FindCurrencyAmount:
		return w.FindCurrencyAmountWorker(ctx, dbTx, input)
	case job.Broadcast:
		return w.BroadcastWorker(ctx, dbTx, input)
	default:
		return "", fmt.Errorf("unknown action type: %s", action)
	}
}

// Process processes a job and returns the broadcast if the job is ready to be broadcast.
func (w *Worker) Process(
	ctx context.Context,
	dbTx database.Transaction,
	j *job.Job,
) (*job.Broadcast, error) {
	if j.CheckComplete() {
		return nil, fmt.Errorf("cannot process complete job")
	}

	for j.Index < len(j.Scenarios[j.ScenarioIndex].Actions) {
		action := j.Scenarios[j.ScenarioIndex].Actions[j.Index]

		// Process input template
		processedInput, err := job.ProcessInput(j.State, action.Input)
		if err != nil {
			return nil, fmt.Errorf("failed to process input: %w", err)
		}

		output, err := w.invokeWorker(ctx, dbTx, action.Type, processedInput)
		if err != nil {
			return nil, fmt.Errorf("action %s failed: %w", action.Type, err)
		}

		// Update state with output if OutputPath is specified
		if action.OutputPath != "" {
			j.State, err = sjson.SetRaw(j.State, action.OutputPath, output)
			if err != nil {
				return nil, fmt.Errorf("failed to set output: %w", err)
			}
		}

		j.Index++

		// Check if we need to move to next scenario
		if j.Index >= len(j.Scenarios[j.ScenarioIndex].Actions) {
			j.ScenarioIndex++
			j.Index = 0

			if j.ScenarioIndex >= len(j.Scenarios) {
				return nil, nil
			}
		}
	}

	return nil, nil
}

// GenerateKeyWorker generates a new key pair.
func GenerateKeyWorker(rawInput string) (string, error) {
	var input job.GenerateKeyInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	kp, err := keys.GenerateKeyPair(input.CurveType)
	if err != nil {
		return "", fmt.Errorf("failed to generate key pair: %w", err)
	}

	return types.PrettyPrintStruct(kp)
}

// DeriveWorker derives an account identifier from a public key.
func (w *Worker) DeriveWorker(
	ctx context.Context,
	rawInput string,
) (string, error) {
	var input job.DeriveInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	if err := asserter.PublicKey(input.PublicKey); err != nil {
		return "", fmt.Errorf("public key is invalid: %w", err)
	}

	account, metadata, err := w.helper.Derive(
		ctx,
		input.NetworkIdentifier,
		input.PublicKey,
		input.Metadata,
	)
	if err != nil {
		return "", fmt.Errorf("failed to derive account: %w", err)
	}

	if err := asserter.AccountIdentifier(account); err != nil {
		return "", fmt.Errorf("derived account identifier is invalid: %w", err)
	}

	result := &job.DeriveOutput{
		AccountIdentifier: account,
		Metadata:          metadata,
	}

	return types.PrettyPrintStruct(result)
}

// SaveAccountWorker saves a *types.AccountIdentifier and associated KeyPair
// in KeyStorage.
func (w *Worker) SaveAccountWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) error {
	var input job.SaveAccountInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("failed to unmarshal input %s: %w", rawInput, err)
	}

	if err := asserter.AccountIdentifier(input.AccountIdentifier); err != nil {
		return fmt.Errorf(
			"account identifier %s is invalid: %w",
			types.PrintStruct(input.AccountIdentifier),
			err,
		)
	}

	// Validate KeyPair is not nil
	if input.KeyPair == nil {
		return fmt.Errorf("keypair is nil")
	}

	// Validate PublicKey is not nil
	if input.KeyPair.PublicKey == nil {
		return fmt.Errorf("keypair public key is nil")
	}

	// Validate PrivateKey is not empty
	if len(input.KeyPair.PrivateKey) == 0 {
		return fmt.Errorf("keypair private key is empty")
	}

	if err := w.helper.StoreKey(ctx, dbTx, input.AccountIdentifier, input.KeyPair); err != nil {
		return fmt.Errorf("failed to store key: %w", err)
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
	var input job.RandomStringInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input %s: %w", rawInput, err)
	}

	output, err := reggen.Generate(input.Regex, input.Limit)
	if err != nil {
		return "", fmt.Errorf("failed to generate a string with the provide regex input: %w", err)
	}

	return marshalString(output), nil
}

// MathWorker performs some MathOperation on 2 numbers.
func MathWorker(rawInput string) (string, error) {
	var input job.MathInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	var result string
	switch input.Operation {
	case job.Addition:
		result, err = types.AddValues(input.LeftValue, input.RightValue)
	case job.Subtraction:
		result, err = types.SubtractValues(input.LeftValue, input.RightValue)
	case job.Multiplication:
		result, err = types.MultiplyValues(input.LeftValue, input.RightValue)
	case job.Division:
		result, err = types.DivideValues(input.LeftValue, input.RightValue)
	default:
		return "", fmt.Errorf("unknown math operation: %s", input.Operation)
	}

	if err != nil {
		return "", fmt.Errorf("math operation failed: %w", err)
	}

	return marshalString(result), nil
}

// FindBalanceWorker finds the balance for a given account and currency.
func (w *Worker) FindBalanceWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) (string, error) {
	var input job.FindBalanceInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	if err := asserter.AccountIdentifier(input.AccountIdentifier); err != nil {
		return "", fmt.Errorf("account identifier is invalid: %w", err)
	}

	if err := asserter.Currency(input.Currency); err != nil {
		return "", fmt.Errorf("currency is invalid: %w", err)
	}

	amount, err := w.helper.Balance(ctx, dbTx, input.AccountIdentifier, input.Currency)
	if err != nil {
		return "", fmt.Errorf("failed to get balance: %w", err)
	}

	return types.PrettyPrintStruct(amount)
}

// RandomNumberWorker generates a random number between minimum and maximum.
func RandomNumberWorker(rawInput string) (string, error) {
	var input job.RandomNumberInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	result, err := utils.RandomNumber(input.Minimum, input.Maximum)
	if err != nil {
		return "", fmt.Errorf("failed to generate random number: %w", err)
	}

	return marshalString(result), nil
}

// AssertWorker asserts that a condition is true.
func AssertWorker(rawInput string) error {
	var input job.AssertInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("failed to unmarshal input: %w", err)
	}

	if !input.Condition {
		return errors.New(input.Message)
	}

	return nil
}

// FindCurrencyAmountWorker finds the amount for a specific currency.
func (w *Worker) FindCurrencyAmountWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) (string, error) {
	var input job.FindCurrencyAmountInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	if err := asserter.Currency(input.Currency); err != nil {
		return "", fmt.Errorf("currency is invalid: %w", err)
	}

	amount, err := w.helper.Balance(ctx, dbTx, input.AccountIdentifier, input.Currency)
	if err != nil {
		return "", fmt.Errorf("failed to get balance: %w", err)
	}

	return types.PrettyPrintStruct(amount)
}

// BroadcastWorker broadcasts a transaction.
func (w *Worker) BroadcastWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) (string, error) {
	var input job.BroadcastInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("failed to unmarshal input: %w", err)
	}

	if err := asserter.NetworkIdentifier(input.NetworkIdentifier); err != nil {
		return "", fmt.Errorf("network identifier is invalid: %w", err)
	}

	txID, metadata, err := w.helper.Broadcast(ctx, input.NetworkIdentifier, input.SignedTransaction)
	if err != nil {
		return "", fmt.Errorf("failed to broadcast transaction: %w", err)
	}

	result := &job.BroadcastOutput{
		TransactionIdentifier: txID,
		Metadata:              metadata,
	}

	return types.PrettyPrintStruct(result), nil
}
