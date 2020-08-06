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

package fetcher

import (
	"crypto/tls"
	"net/http"
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

// WithInsecureTLS overrides the default TLS
// security settings to allow insecure certificates
// on an HTTPS connection.
//
// This should ONLY be used when debugging a Rosetta API
// implementation. Using this option can lead to a man-in-the-middle
// attack!!
func WithInsecureTLS() Option {
	return func(f *Fetcher) {
		// See this conversation around why `.Clone()` is used here:
		// https://github.com/golang/go/issues/26013
		customTransport := http.DefaultTransport.(*http.Transport).Clone()
		customTransport.TLSClientConfig = &tls.Config{InsecureSkipVerify: true} // #nosec G402

		f.rosettaClient.GetConfig().HTTPClient.Transport = customTransport
	}
}

// WithTimeout overrides the default HTTP timeout.
func WithTimeout(timeout time.Duration) Option {
	return func(f *Fetcher) {
		f.rosettaClient.GetConfig().HTTPClient.Timeout = timeout
	}
}
