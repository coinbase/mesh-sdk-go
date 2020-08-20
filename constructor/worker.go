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

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/types"

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

func (w *Worker) invokeWorker(ctx context.Context, action ActionType, processedInput string) (string, error) {
	var err error

	switch action {
	case SetVariable:
		return processedInput, nil
	case GenerateKey:
		var unmarshaledInput GenerateKeyInput
		err = unmarshalInput([]byte(processedInput), &unmarshaledInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		var output *keys.KeyPair
		output, err = GenerateKeyWorker(&unmarshaledInput)
		if err == nil {
			return types.PrintStruct(output), nil
		}
	case Derive:
		var unmarshaledInput types.ConstructionDeriveRequest
		err = unmarshalInput([]byte(processedInput), &unmarshaledInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		if err := asserter.PublicKey(unmarshaledInput.PublicKey); err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		var output *types.ConstructionDeriveResponse
		output, err = w.DeriveWorker(ctx, &unmarshaledInput)
		if err == nil {
			return types.PrintStruct(output), nil
		}
	case SaveAddress:
		var unmarshaledInput SaveAddressInput
		err = unmarshalInput([]byte(processedInput), &unmarshaledInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		if len(unmarshaledInput.Address) == 0 {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, "empty address")
		}

		err = w.SaveAddressWorker(ctx, &unmarshaledInput)
	case PrintMessage:
		PrintMessageWorker(processedInput)
	case RandomString:
		var unmarshaledInput RandomStringInput
		err = unmarshalInput([]byte(processedInput), &unmarshaledInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		var output string
		output, err = RandomStringWorker(&unmarshaledInput)
		if err == nil {
			return marshalString(output), nil
		}
	case Math:
		var unmarshaledInput MathInput
		err = unmarshalInput([]byte(processedInput), &unmarshaledInput)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
		}

		var output string
		output, err = MathWorker(&unmarshaledInput)
		if err == nil {
			return marshalString(output), nil
		}
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidActionType, action)
	}

	if err != nil {
		return "", fmt.Errorf("%w: %s %s", ErrActionFailed, action, err.Error())
	}

	return "", nil
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
	input *types.ConstructionDeriveRequest,
) (*types.ConstructionDeriveResponse, error) {
	address, metadata, err := w.helper.Derive(
		ctx,
		input.NetworkIdentifier,
		input.PublicKey,
		input.Metadata,
	)
	if err != nil {
		return nil, err
	}

	return &types.ConstructionDeriveResponse{
		Address:  address,
		Metadata: metadata,
	}, nil
}

// GenerateKeyWorker attempts to generate a key given a
// *GenerateKeyInput input.
func GenerateKeyWorker(input *GenerateKeyInput) (*keys.KeyPair, error) {
	kp, err := keys.GenerateKeypair(input.CurveType)
	if err != nil {
		return nil, err
	}

	return kp, nil
}

// SaveAddressWorker saves an address and associated KeyPair
// in KeyStorage.
func (w *Worker) SaveAddressWorker(ctx context.Context, input *SaveAddressInput) error {
	return w.helper.StoreKey(ctx, input.Address, input.KeyPair)
}

// PrintMessageWorker logs some message to stdout.
func PrintMessageWorker(message string) {
	log.Printf("Message: %s\n", message)
}

// RandomStringWorker generates a string that complies
// with the provided regex input.
func RandomStringWorker(input *RandomStringInput) (string, error) {
	return reggen.Generate(input.Regex, input.Limit)
}

// MathWorker performs some MathOperation on 2 numbers.
func MathWorker(input *MathInput) (string, error) {
	switch input.Operation {
	case Addition:
		return types.AddValues(input.LeftValue, input.RightValue)
	case Subtraction:
		return types.SubtractValues(input.LeftValue, input.RightValue)
	default:
		return "", fmt.Errorf("%s is not a supported math operation", input.Operation)
	}
}
