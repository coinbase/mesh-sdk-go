// Copyright 2025 Coinbase, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

// Package fallback provides utilities for fallback mechanisms when the main
// Rosetta SDK functionality doesn't support certain operations.
package fallback

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/coinbase/rosetta-sdk-go/types"
)

// ConstructionPreprocessOperationsRequest matches the request format for the fallback API
type ConstructionPreprocessOperationsRequest struct {
	NetworkIdentifier *types.NetworkIdentifier     `json:"network_identifier"`
	FromAddress       string                       `json:"from_address,omitempty"`
	ConstructOp       string                       `json:"construct_op"`
	Options           map[string]interface{}       `json:"options,omitempty"`
}

// ConstructionPreprocessOperationsResponse matches the response format for the fallback API
type ConstructionPreprocessOperationsResponse struct {
	Operations []*types.Operation `json:"operations"`
	MaxFee     *types.Amount      `json:"max_fee,omitempty"`
	Metadata   []byte             `json:"metadata,omitempty"`
}

// FallbackClient provides fallback functionality for construction operations
type FallbackClient struct {
	BaseURL    string
	HTTPClient *http.Client
}

// NewFallbackClient creates a new fallback client
func NewFallbackClient(baseURL string, httpClient *http.Client) *FallbackClient {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &FallbackClient{
		BaseURL:    baseURL,
		HTTPClient: httpClient,
	}
}

// ConstructionPreprocessOperations calls the fallback API for operation preprocessing
func (c *FallbackClient) ConstructionPreprocessOperations(
	ctx context.Context,
	networkIdentifier *types.NetworkIdentifier,
	fromAddress string,
	constructOp string,
	options map[string]interface{},
) ([]*types.Operation, *types.Amount, []byte, error) {
	request := &ConstructionPreprocessOperationsRequest{
		NetworkIdentifier: networkIdentifier,
		FromAddress:       fromAddress,
		ConstructOp:       constructOp,
		Options:           options,
	}

	reqBody, err := json.Marshal(request)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to marshal request: %w", err)
	}

	url := c.BaseURL + "/construction/preprocess_operations"
	httpReq, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewBuffer(reqBody))
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to create request: %w", err)
	}

	httpReq.Header.Set("Content-Type", "application/json")
	httpReq.Header.Set("Accept", "application/json")

	resp, err := c.HTTPClient.Do(httpReq)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to make request: %w", err)
	}
	defer func() {
		_ = resp.Body.Close()
	}()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	if resp.StatusCode == 501 {
		return nil, nil, nil, fmt.Errorf("construction preprocess operations not supported by this Rosetta implementation")
	}

	if resp.StatusCode != 200 {
		// Try to parse as Rosetta error
		var rosettaErr types.Error
		if json.Unmarshal(body, &rosettaErr) == nil {
			return nil, nil, nil, fmt.Errorf("rosetta error %d: %s", rosettaErr.Code, rosettaErr.Message)
		}
		return nil, nil, nil, fmt.Errorf("unexpected status code %d: %s", resp.StatusCode, string(body))
	}

	var response ConstructionPreprocessOperationsResponse
	if err := json.Unmarshal(body, &response); err != nil {
		return nil, nil, nil, fmt.Errorf("failed to unmarshal response: %w", err)
	}

	return response.Operations, response.MaxFee, response.Metadata, nil
}