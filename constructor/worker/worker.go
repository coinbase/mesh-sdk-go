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

package worker

import (
	"context"
	"fmt"
	"log"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/constructor/job"
	"github.com/coinbase/rosetta-sdk-go/keys"
	"github.com/coinbase/rosetta-sdk-go/storage"
	"github.com/coinbase/rosetta-sdk-go/types"
	"github.com/coinbase/rosetta-sdk-go/utils"

	"github.com/lucasjones/reggen"
	"github.com/tidwall/sjson"
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
	dbTx storage.DatabaseTransaction,
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
	case job.SaveAddress:
		return "", w.SaveAddressWorker(ctx, dbTx, input)
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
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidActionType, action)
	}
}

func (w *Worker) actions(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	state string,
	actions []*job.Action,
) (string, error) {
	for _, action := range actions {
		processedInput, err := PopulateInput(state, action.Input)
		if err != nil {
			return "", fmt.Errorf("%w: unable to populate variables", err)
		}

		output, err := w.invokeWorker(ctx, dbTx, action.Type, processedInput)
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
	dbTx storage.DatabaseTransaction,
	j *job.Job,
) error {
	scenario := j.Scenarios[j.Index]
	newState, err := w.actions(ctx, dbTx, j.State, scenario.Actions)
	if err != nil {
		return fmt.Errorf("%w: unable to process %s actions", err, scenario.Name)
	}

	j.State = newState
	j.Index++
	return nil
}

// Process is called on a Job to execute
// the next available scenario. If no scenarios
// are remaining, this will return an error.
func (w *Worker) Process(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	j *job.Job,
) (*job.Broadcast, error) {
	if j.CheckComplete() {
		return nil, ErrJobComplete
	}

	if err := w.ProcessNextScenario(ctx, dbTx, j); err != nil {
		return nil, fmt.Errorf("%w: could not process next scenario", err)
	}

	return j.CreateBroadcast()
}

// DeriveWorker attempts to derive an address given a
// *types.ConstructionDeriveRequest input.
func (w *Worker) DeriveWorker(
	ctx context.Context,
	rawInput string,
) (string, error) {
	var input types.ConstructionDeriveRequest
	err := job.UnmarshalInput([]byte(rawInput), &input)
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
	var input job.GenerateKeyInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
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
func (w *Worker) SaveAddressWorker(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	rawInput string,
) error {
	var input job.SaveAddressInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if len(input.Address) == 0 {
		return fmt.Errorf("%w: %s", ErrInvalidInput, "empty address")
	}

	if err := w.helper.StoreKey(ctx, dbTx, input.Address, input.KeyPair); err != nil {
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
	var input job.RandomStringInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
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
	var input job.MathInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	var result string
	switch input.Operation {
	case job.Addition:
		result, err = types.AddValues(input.LeftValue, input.RightValue)
	case job.Subtraction:
		result, err = types.SubtractValues(input.LeftValue, input.RightValue)
	default:
		return "", fmt.Errorf("%s is not a supported math operation", input.Operation)
	}
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return marshalString(result), nil
}

// RandomNumberWorker generates a random number in the range
// [minimum,maximum).
func RandomNumberWorker(rawInput string) (string, error) {
	var input job.RandomNumberInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	min, err := types.BigInt(input.Minimum)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	max, err := types.BigInt(input.Maximum)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	randNum := utils.RandomNumber(min, max)
	return marshalString(randNum.String()), nil
}

// balanceMessage prints out a log message while waiting
// that reflects the *FindBalanceInput.
func balanceMessage(input *job.FindBalanceInput) string {
	waitObject := "balance"
	if input.RequireCoin {
		waitObject = "coin"
	}

	message := fmt.Sprintf(
		"looking for %s %s",
		waitObject,
		types.PrintStruct(input.MinimumBalance),
	)

	if len(input.Address) > 0 {
		message = fmt.Sprintf(
			"%s on address %s",
			message,
			input.Address,
		)
	}

	if input.SubAccount != nil {
		message = fmt.Sprintf(
			"%s with sub_account %s",
			message,
			types.PrintStruct(input.SubAccount),
		)
	}

	if len(input.NotAddress) > 0 {
		message = fmt.Sprintf(
			"%s != to addresses %s",
			message,
			types.PrintStruct(input.NotAddress),
		)
	}

	if len(input.NotCoins) > 0 {
		message = fmt.Sprintf(
			"%s != to coins %s",
			message,
			types.PrintStruct(input.NotCoins),
		)
	}

	return message
}

func (w *Worker) checkAccountCoins(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	input *job.FindBalanceInput,
	account *types.AccountIdentifier,
) (string, error) {
	coins, err := w.helper.Coins(ctx, dbTx, account, input.MinimumBalance.Currency)
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

		return types.PrintStruct(job.FindBalanceOutput{
			Account: account,
			Balance: coin.Amount,
			Coin:    coin.CoinIdentifier,
		}), nil
	}

	return "", nil
}

func (w *Worker) checkAccountBalance(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	input *job.FindBalanceInput,
	account *types.AccountIdentifier,
) (string, error) {
	fmt.Printf("invoking account balance %s for %s\n", types.PrintStruct(account), types.PrintStruct(input.MinimumBalance))
	amount, err := w.helper.Balance(ctx, dbTx, account, input.MinimumBalance.Currency)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	fmt.Printf("found account balance %s of %s\n", types.PrintStruct(account), types.PrintStruct(amount))

	// look for amounts > min
	diff, err := types.SubtractValues(amount.Value, input.MinimumBalance.Value)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	bigIntDiff, err := types.BigInt(diff)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	if bigIntDiff.Sign() < 0 {
		return "", nil
	}

	return types.PrintStruct(job.FindBalanceOutput{
		Account: account,
		Balance: amount,
	}), nil
}

func (w *Worker) availableAddresses(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
) ([]string, []string, error) {
	addresses, err := w.helper.AllAddresses(ctx, dbTx)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to get all addresses %s",
			ErrActionFailed,
			err.Error(),
		)
	}

	// If there are no addresses, we should create one.
	if len(addresses) == 0 {
		return nil, nil, ErrCreateAccount
	}

	// We fetch all locked addresses to subtract them from AllAddresses.
	// We consider an address "locked" if it is actively involved in a broadcast.
	unlockedAddresses := []string{}
	lockedAddresses, err := w.helper.LockedAddresses(ctx, dbTx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get locked addresses %s", ErrActionFailed, err)
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

	return addresses, unlockedAddresses, nil
}

// FindBalanceWorker attempts to find an account (and coin) with some minimum
// balance in a particular currency.
func (w *Worker) FindBalanceWorker(
	ctx context.Context,
	dbTx storage.DatabaseTransaction,
	rawInput string,
) (string, error) {
	var input job.FindBalanceInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := asserter.Amount(input.MinimumBalance); err != nil {
		return "", fmt.Errorf("%w: minimum balance invalid %s", ErrInvalidInput, err.Error())
	}

	log.Println(balanceMessage(&input))

	addresses, availableAddresses, err := w.availableAddresses(ctx, dbTx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get available addresses", err)
	}

	fmt.Printf("Addresses: %v\n", addresses)
	fmt.Printf("Available addresses: %v\n", availableAddresses)

	// Consider each availableAddress as a potential account.
	for _, address := range availableAddresses {
		// If we require an address and that address
		// is not equal to the address we are considering,
		// we should continue.
		if len(input.Address) != 0 && address != input.Address {
			continue
		}

		// If we specify that we do not use certain addresses
		// and the address we are considering is one of them,
		// we should continue.
		if utils.ContainsString(input.NotAddress, address) {
			continue
		}

		account := &types.AccountIdentifier{
			Address:    address,
			SubAccount: input.SubAccount,
		}

		var output string
		var err error
		if input.RequireCoin {
			output, err = w.checkAccountCoins(ctx, dbTx, &input, account)
		} else {
			output, err = w.checkAccountBalance(ctx, dbTx, &input, account)
		}
		if err != nil {
			return "", err
		}

		// If we did not fund a match, we should continue.
		if len(output) == 0 {
			continue
		}

		log.Printf(
			"Found balance %s\n",
			output,
		)
		return output, nil
	}

	fmt.Println("found no balances")

	// If we should create an account and the number of addresses
	// we have is less than the limit, we return ErrCreateAccount.
	// Note, we must also be looking for an account with a 0 balance.
	if input.Create > 0 && len(addresses) <= input.Create && input.MinimumBalance.Value == "0" {
		return "", ErrCreateAccount
	}

	// If we can't do anything and we aren't supposed to wait, we should
	// return with ErrUnsatisfiable.
	fmt.Println("returning unsatisfiable")
	return "", ErrUnsatisfiable
}
