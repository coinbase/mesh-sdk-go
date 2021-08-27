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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io/ioutil"
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
		return FindCurrencyAmountWorker(input)
	case job.LoadEnv:
		return LoadEnvWorker(input)
	case job.HTTPRequest:
		return HTTPRequestWorker(input)
	case job.SetBlob:
		return "", w.SetBlobWorker(ctx, dbTx, input)
	case job.GetBlob:
		return w.GetBlobWorker(ctx, dbTx, input)
	default:
		return "", fmt.Errorf("%w: %s", ErrInvalidActionType, action)
	}
}

func (w *Worker) actions(
	ctx context.Context,
	dbTx database.Transaction,
	state string,
	actions []*job.Action,
) (string, *Error) {
	for i, action := range actions {
		processedInput, err := PopulateInput(state, action.Input)
		if err != nil {
			return "", &Error{
				ActionIndex: i,
				Action:      action,
				State:       state,
				Err:         fmt.Errorf("%w: unable to populate variables", err),
			}
		}

		output, err := w.invokeWorker(ctx, dbTx, action.Type, processedInput)
		if err != nil {
			return "", &Error{
				ActionIndex:    i,
				Action:         action,
				ProcessedInput: processedInput,
				State:          state,
				Err:            fmt.Errorf("%w: unable to process action", err),
			}
		}

		if len(output) == 0 {
			continue
		}

		// Update state at the specified output path if there is an output.
		oldState := state
		state, err = sjson.SetRaw(state, action.OutputPath, output)
		if err != nil {
			return "", &Error{
				ActionIndex:    i,
				Action:         action,
				ProcessedInput: processedInput,
				Output:         output,
				State:          oldState,
				Err:            fmt.Errorf("%w: unable to update state", err),
			}
		}
	}

	return state, nil
}

// ProcessNextScenario performs the actions in the next available
// scenario.
func (w *Worker) ProcessNextScenario(
	ctx context.Context,
	dbTx database.Transaction,
	j *job.Job,
) *Error {
	scenario := j.Scenarios[j.Index]
	newState, err := w.actions(ctx, dbTx, j.State, scenario.Actions)
	if err != nil {
		// Set additional context not available within actions.
		err.Workflow = j.Workflow
		err.Job = j.Identifier
		err.Scenario = scenario.Name
		err.ScenarioIndex = j.Index

		return err
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
	dbTx database.Transaction,
	j *job.Job,
) (*job.Broadcast, *Error) {
	if j.CheckComplete() {
		return nil, &Error{Err: ErrJobComplete}
	}

	if err := w.ProcessNextScenario(ctx, dbTx, j); err != nil {
		return nil, err
	}

	broadcast, err := j.CreateBroadcast()
	if err != nil {
		scenarioIndex := j.Index - 1 // ProcessNextScenario increments by 1
		return nil, &Error{
			Workflow:      j.Workflow,
			Job:           j.Identifier,
			ScenarioIndex: scenarioIndex,
			Scenario:      j.Scenarios[scenarioIndex].Name,
			State:         j.State,
			Err:           fmt.Errorf("%w: unable to create broadcast", err),
		}
	}

	return broadcast, nil
}

// DeriveWorker attempts to derive an account given a
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

	accountIdentifier, metadata, err := w.helper.Derive(
		ctx,
		input.NetworkIdentifier,
		input.PublicKey,
		input.Metadata,
	)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return types.PrintStruct(&types.ConstructionDeriveResponse{
		AccountIdentifier: accountIdentifier,
		Metadata:          metadata,
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
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := asserter.AccountIdentifier(input.AccountIdentifier); err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := w.helper.StoreKey(ctx, dbTx, input.AccountIdentifier, input.KeyPair); err != nil {
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
	case job.Multiplication:
		result, err = types.MultiplyValues(input.LeftValue, input.RightValue)
	case job.Division:
		result, err = types.DivideValues(input.LeftValue, input.RightValue)
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

	randNum, err := utils.RandomNumber(min, max)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

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

	if input.AccountIdentifier != nil {
		message = fmt.Sprintf(
			"%s on account %s",
			message,
			types.PrintStruct(input.AccountIdentifier),
		)
	}

	if input.SubAccountIdentifier != nil {
		message = fmt.Sprintf(
			"%s with sub_account %s",
			message,
			types.PrintStruct(input.SubAccountIdentifier),
		)
	}

	if len(input.NotAddress) > 0 {
		message = fmt.Sprintf(
			"%s != to addresses %s",
			message,
			types.PrintStruct(input.NotAddress),
		)
	}

	if len(input.NotAccountIdentifier) > 0 {
		message = fmt.Sprintf(
			"%s != to accounts %s",
			message,
			types.PrintStruct(input.NotAccountIdentifier),
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
	dbTx database.Transaction,
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

		return types.PrintStruct(&job.FindBalanceOutput{
			AccountIdentifier: account,
			Balance:           coin.Amount,
			Coin:              coin.CoinIdentifier,
		}), nil
	}

	return "", nil
}

func (w *Worker) checkAccountBalance(
	ctx context.Context,
	dbTx database.Transaction,
	input *job.FindBalanceInput,
	account *types.AccountIdentifier,
) (string, error) {
	amount, err := w.helper.Balance(ctx, dbTx, account, input.MinimumBalance.Currency)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

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
		log.Printf(
			"checkAccountBalance: Account (%s) has balance (%s), less than the minimum balance (%s)",
			account.Address,
			amount.Value,
			input.MinimumBalance.Value,
		)
		return "", nil
	}

	return types.PrintStruct(&job.FindBalanceOutput{
		AccountIdentifier: account,
		Balance:           amount,
	}), nil
}

func (w *Worker) availableAccounts(
	ctx context.Context,
	dbTx database.Transaction,
) ([]*types.AccountIdentifier, []*types.AccountIdentifier, error) {
	accounts, err := w.helper.AllAccounts(ctx, dbTx)
	if err != nil {
		return nil, nil, fmt.Errorf(
			"%w: unable to get all accounts %s",
			ErrActionFailed,
			err.Error(),
		)
	}

	// If there are no accounts, we should create one.
	if len(accounts) == 0 {
		return nil, nil, ErrCreateAccount
	}

	// We fetch all locked accounts to subtract them from AllAccounts.
	// We consider an account "locked" if it is actively involved in a broadcast.
	unlockedAccounts := []*types.AccountIdentifier{}
	lockedAccounts, err := w.helper.LockedAccounts(ctx, dbTx)
	if err != nil {
		return nil, nil, fmt.Errorf("%w: unable to get locked accounts %s", ErrActionFailed, err)
	}

	// Convert to a map so can do fast lookups
	lockedSet := map[string]struct{}{}
	for _, account := range lockedAccounts {
		lockedSet[types.Hash(account)] = struct{}{}
	}

	for _, account := range accounts {
		if _, exists := lockedSet[types.Hash(account)]; !exists {
			unlockedAccounts = append(unlockedAccounts, account)
		}
	}

	return accounts, unlockedAccounts, nil
}

func shouldCreateRandomAccount(
	input *job.FindBalanceInput,
	accountCount int,
) (bool, error) {
	if input.MinimumBalance.Value != "0" {
		return false, nil
	}

	if input.CreateLimit <= 0 || accountCount >= input.CreateLimit {
		return false, nil
	}

	rand, err := utils.RandomNumber(
		utils.ZeroInt,
		utils.OneHundredInt,
	)
	if err != nil {
		return false, err
	}

	if rand.Int64() >= int64(input.CreateProbability) {
		return false, nil
	}

	return true, nil
}

// findBalanceWorkerInputValidation ensures the input to FindBalanceWorker
// is valid.
func findBalanceWorkerInputValidation(input *job.FindBalanceInput) error {
	if err := asserter.Amount(input.MinimumBalance); err != nil {
		return fmt.Errorf("%w: minimum balance invalid", err)
	}

	if input.AccountIdentifier != nil {
		if err := asserter.AccountIdentifier(input.AccountIdentifier); err != nil {
			return err
		}

		if input.SubAccountIdentifier != nil {
			return errors.New("cannot populate both account and sub account")
		}

		if len(input.NotAccountIdentifier) > 0 {
			return errors.New("cannot populate both account and not accounts")
		}

		if len(input.NotAddress) > 0 {
			return errors.New("cannot populate both account and not address")
		}
	}

	if len(input.NotAccountIdentifier) > 0 {
		if err := asserter.AccountArray("not account identifier", input.NotAccountIdentifier); err != nil {
			return err
		}
	}

	return nil
}

func skipAccount(input job.FindBalanceInput, account *types.AccountIdentifier) bool {
	// If we require an account and that account
	// is not equal to the account we are considering,
	// we should continue.
	if input.AccountIdentifier != nil &&
		types.Hash(account) != types.Hash(input.AccountIdentifier) {
		return true
	}

	// If we specify not to use certain addresses and we are considering
	// one of them, we should continue.
	if utils.ContainsString(input.NotAddress, account.Address) {
		return true
	}

	// If we specify that we do not use certain accounts
	// and the account we are considering is one of them,
	// we should continue.
	if utils.ContainsAccountIdentifier(input.NotAccountIdentifier, account) {
		return true
	}

	// If we require a particular SubAccountIdentifier, we skip
	// if the account we are examining does not have it.
	if input.SubAccountIdentifier != nil &&
		(account.SubAccount == nil ||
			types.Hash(account.SubAccount) != types.Hash(input.SubAccountIdentifier)) {
		return true
	}

	return false
}

// FindBalanceWorker attempts to find an account (and coin) with some minimum
// balance in a particular currency.
func (w *Worker) FindBalanceWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) (string, error) {
	var input job.FindBalanceInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	// Validate that input is properly formatted
	if err := findBalanceWorkerInputValidation(&input); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	log.Println(balanceMessage(&input))

	accounts, availableAccounts, err := w.availableAccounts(ctx, dbTx)
	if err != nil {
		return "", fmt.Errorf("%w: unable to get available accounts", err)
	}

	// Randomly, we choose to generate a new account. If we didn't do this,
	// we would never grow past 2 accounts for mocking transfers.
	shouldCreate, err := shouldCreateRandomAccount(&input, len(accounts))
	if err != nil {
		return "", fmt.Errorf("%w: unable to determine if should create", err)
	}

	if shouldCreate {
		return "", ErrCreateAccount
	}

	var unmatchedAccounts []string
	// Consider each available account as a potential account.
	for _, account := range availableAccounts {
		if skipAccount(input, account) {
			continue
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
			unmatchedAccounts = append(unmatchedAccounts, account.Address)
			continue
		}

		return output, nil
	}

	if len(unmatchedAccounts) > 0 {
		log.Printf("%d account(s) insufficiently funded. Did you forget to fund? %+v",
			len(unmatchedAccounts),
			unmatchedAccounts,
		)
	}

	// If we can't do anything, we should return with ErrUnsatisfiable.
	if input.MinimumBalance.Value != "0" {
		return "", ErrUnsatisfiable
	}

	// If we should create an account and the number of accounts
	// we have is less than the limit, we return ErrCreateAccount.
	if input.CreateLimit > 0 && len(accounts) < input.CreateLimit {
		return "", ErrCreateAccount
	}

	// If we reach here, it means we shouldn't create another account
	// and should just return unsatisfiable.
	return "", ErrUnsatisfiable
}

// AssertWorker checks if an input is < 0.
func AssertWorker(rawInput string) error {
	// We unmarshal the input here to handle string
	// unwrapping automatically.
	var input string
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	val, err := types.BigInt(input)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if val.Sign() < 0 {
		return fmt.Errorf("%w: %s < 0", ErrActionFailed, val.String())
	}

	return nil
}

// FindCurrencyAmountWorker finds a *types.Amount with a specific
// *types.Currency in a []*types.Amount.
func FindCurrencyAmountWorker(rawInput string) (string, error) {
	var input job.FindCurrencyAmountInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := asserter.Currency(input.Currency); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if err := asserter.AssertUniqueAmounts(input.Amounts); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	for _, amount := range input.Amounts {
		if types.Hash(amount.Currency) != types.Hash(input.Currency) {
			continue
		}

		return types.PrintStruct(amount), nil
	}

	return "", fmt.Errorf(
		"%w: unable to find currency %s",
		ErrActionFailed,
		types.PrintStruct(input.Currency),
	)
}

// LoadEnvWorker loads an environment variable and stores
// it in state. This is useful for algorithmic fauceting.
func LoadEnvWorker(rawInput string) (string, error) {
	// We unmarshal the input here to handle string
	// unwrapping automatically.
	var input string
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	return os.Getenv(input), nil
}

// HTTPRequestWorker makes an HTTP request and returns the response to
// store in a variable. This is useful for algorithmic fauceting.
func HTTPRequestWorker(rawInput string) (string, error) {
	var input job.HTTPRequestInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	if input.Timeout <= 0 {
		return "", fmt.Errorf("%w: %d is not a valid timeout", ErrInvalidInput, input.Timeout)
	}

	if _, err := url.ParseRequestURI(input.URL); err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	client := &http.Client{Timeout: time.Duration(input.Timeout) * time.Second}
	var request *http.Request
	switch input.Method {
	case job.MethodGet:
		request, err = http.NewRequest(http.MethodGet, input.URL, nil)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
		}
		request.Header.Set("Accept", "application/json")
	case job.MethodPost:
		request, err = http.NewRequest(
			http.MethodPost,
			input.URL,
			bytes.NewBufferString(input.Body),
		)
		if err != nil {
			return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
		}
		request.Header.Set("Content-Type", "application/json")
		request.Header.Set("Accept", "application/json")
	default:
		return "", fmt.Errorf(
			"%w: %s is not a supported HTTP method",
			ErrInvalidInput,
			input.Method,
		)
	}

	resp, err := client.Do(request)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}
	defer resp.Body.Close()

	body, err := ioutil.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf(
			"%w: status code %d with body %s",
			ErrActionFailed,
			resp.StatusCode,
			body,
		)
	}

	return string(body), nil
}

// SetBlobWorker transactionally saves a key and value for use
// across workflows.
func (w *Worker) SetBlobWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) error {
	var input job.SetBlobInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	// By using interface{} for key, we can ensure that JSON
	// objects with the same keys but in a different order are
	// treated as equal.
	if err := w.helper.SetBlob(ctx, dbTx, types.Hash(input.Key), input.Value); err != nil {
		return fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	return nil
}

// GetBlobWorker transactionally retrieves a value associated with
// a key, if it exists.
func (w *Worker) GetBlobWorker(
	ctx context.Context,
	dbTx database.Transaction,
	rawInput string,
) (string, error) {
	var input job.GetBlobInput
	err := job.UnmarshalInput([]byte(rawInput), &input)
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrInvalidInput, err.Error())
	}

	// By using interface{} for key, we can ensure that JSON
	// objects with the same keys but in a different order are
	// treated as equal.
	exists, val, err := w.helper.GetBlob(ctx, dbTx, types.Hash(input.Key))
	if err != nil {
		return "", fmt.Errorf("%w: %s", ErrActionFailed, err.Error())
	}

	if !exists {
		return "", fmt.Errorf(
			"%w: key %s does not exist",
			ErrActionFailed,
			types.PrintStruct(input.Key),
		)
	}

	return string(val), nil
}
