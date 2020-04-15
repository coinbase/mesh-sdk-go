package fetcher

import (
	"time"

	"github.com/coinbase/rosetta-sdk-go/asserter"
	"github.com/coinbase/rosetta-sdk-go/client"
)

// Option is used to overwrite default values in
// Fetcher construction. Any Option not provided
// falls back to the default value.
type Option func(f *Fetcher)

// WithClient overrides the default client.APIClient.
func WithClient(client *client.APIClient) Option {
	return func(f *Fetcher) {
		f.rosettaClient = client
	}
}

// WithBlockConcurrency overrides the default block concurrency.
func WithBlockConcurrency(concurrency uint64) Option {
	return func(f *Fetcher) {
		f.blockConcurrency = concurrency
	}
}

// WithTransactionConcurrency overrides the default transaction
// concurrency.
func WithTransactionConcurrency(concurrency uint64) Option {
	return func(f *Fetcher) {
		f.transactionConcurrency = concurrency
	}
}

// WithMaxRetries overrides the default number of retries on
// a request.
func WithMaxRetries(maxRetries uint64) Option {
	return func(f *Fetcher) {
		f.maxRetries = maxRetries
	}
}

// WithRetryElapsedTime overrides the default max elapsed time
// to retry a request.
func WithRetryElapsedTime(retryElapsedTime time.Duration) Option {
	return func(f *Fetcher) {
		f.retryElapsedTime = retryElapsedTime
	}
}

// WithAsserter sets the asserter.Asserter on construction
// so it does not need to be initialized.
func WithAsserter(asserter *asserter.Asserter) Option {
	return func(f *Fetcher) {
		f.Asserter = asserter
	}
}
