package types

// AccountCurrency is a simple struct combining
// a *types.Account and *types.Currency. This can
// be useful for looking up balances.
type AccountCurrency struct {
	Account  *AccountIdentifier `json:"account_identifier,omitempty"`
	Currency *Currency          `json:"currency,omitempty"`
}
