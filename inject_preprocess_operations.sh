#!/bin/bash
# Script to inject ConstructionPreprocessOperations API after code generation

echo "Injecting ConstructionPreprocessOperations API..."

# 1. Add to server API interface (server/api.go)
if ! grep -q "ConstructionPreprocessOperations" server/api.go; then
    # Find the line with ConstructionPreprocess and add our method after it
    sed -i.bak '/ConstructionPreprocess(/a\
	// ConstructionPreprocessOperations is an OPTIONAL API that provides fallback parsing\
	// for high-level transaction construction operations when local OperationSelector\
	// does not support certain construct operations. Implementations can return an error\
	// with code 501 (Not Implemented) if they do not support this functionality.\
	ConstructionPreprocessOperations(\
		context.Context,\
		*types.ConstructionPreprocessOperationsRequest,\
	) (*types.ConstructionPreprocessOperationsResponse, *types.Error)' server/api.go
fi

# 2. Add route to server router (server/api_construction.go)
if ! grep -q "ConstructionPreprocessOperations" server/api_construction.go; then
    # Add route to Routes() function
    sed -i.bak '/\/construction\/preprocess/a\
		{\
			"ConstructionPreprocessOperations",\
			strings.ToUpper("Post"),\
			"/construction/preprocess_operations",\
			c.ConstructionPreprocessOperations,\
		},' server/api_construction.go

    # Add handler method
    cat >> server/api_construction.go << 'EOF'

// ConstructionPreprocessOperations - Parse and Validate Operations via Rosetta API
func (c *ConstructionAPIController) ConstructionPreprocessOperations(w http.ResponseWriter, r *http.Request) {
	constructionPreprocessOperationsRequest := &types.ConstructionPreprocessOperationsRequest{}
	if err := json.NewDecoder(r.Body).Decode(&constructionPreprocessOperationsRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)
		return
	}

	// Assert that ConstructionPreprocessOperationsRequest is correct
	if err := c.asserter.ConstructionPreprocessOperationsRequest(constructionPreprocessOperationsRequest); err != nil {
		EncodeJSONResponse(&types.Error{
			Message: err.Error(),
		}, http.StatusInternalServerError, w)
		return
	}

	result, serviceErr := c.service.ConstructionPreprocessOperations(
		r.Context(),
		constructionPreprocessOperationsRequest,
	)
	if serviceErr != nil {
		EncodeJSONResponse(serviceErr, http.StatusInternalServerError, w)
		return
	}

	EncodeJSONResponse(result, http.StatusOK, w)
}
EOF
fi

# 3. Add client method (client/api_construction.go)
if ! grep -q "ConstructionPreprocessOperations" client/api_construction.go; then
    cat >> client/api_construction.go << 'EOF'

// ConstructionPreprocessOperations PreprocessOperations is called to parse and validate
// transaction construction operations using the Rosetta API. This endpoint allows the
// Rosetta implementation to handle operation parsing and validation that might not be
// supported by the local OperationSelector.
func (a *ConstructionAPIService) ConstructionPreprocessOperations(
	ctx _context.Context,
	constructionPreprocessOperationsRequest *types.ConstructionPreprocessOperationsRequest,
) (*types.ConstructionPreprocessOperationsResponse, *types.Error, error) {
	var (
		localVarPostBody interface{}
	)

	// create path and map variables
	localVarPath := a.client.cfg.BasePath + "/construction/preprocess_operations"
	localVarHeaderParams := make(map[string]string)

	// to determine the Content-Type header
	localVarHTTPContentTypes := []string{"application/json"}

	// set Content-Type header
	localVarHTTPContentType := selectHeaderContentType(localVarHTTPContentTypes)
	if localVarHTTPContentType != "" {
		localVarHeaderParams["Content-Type"] = localVarHTTPContentType
	}

	// to determine the Accept header
	localVarHTTPHeaderAccepts := []string{"application/json"}

	// set Accept header
	localVarHTTPHeaderAccept := selectHeaderAccept(localVarHTTPHeaderAccepts)
	if localVarHTTPHeaderAccept != "" {
		localVarHeaderParams["Accept"] = localVarHTTPHeaderAccept
	}
	// body params
	localVarPostBody = constructionPreprocessOperationsRequest

	r, err := a.client.prepareRequest(ctx, localVarPath, localVarPostBody, localVarHeaderParams)
	if err != nil {
		return nil, nil, fmt.Errorf("failed to prepare request: %w", err)
	}

	localVarHTTPResponse, err := a.client.callAPI(ctx, r)
	if err != nil || localVarHTTPResponse == nil {
		return nil, nil, fmt.Errorf("failed to call API: %w", err)
	}

	localVarBody, err := io.ReadAll(localVarHTTPResponse.Body)
	defer func() {
		_, _ = io.Copy(io.Discard, localVarHTTPResponse.Body)
		_ = localVarHTTPResponse.Body.Close()
	}()
	if err != nil {
		return nil, nil, fmt.Errorf("failed to read response: %w", err)
	}

	switch localVarHTTPResponse.StatusCode {
	case _nethttp.StatusOK:
		var v types.ConstructionPreprocessOperationsResponse
		err = a.client.decode(&v, localVarBody, localVarHTTPResponse.Header.Get("Content-Type"))
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to decode when hit status code 200, response body %s: %w",
				string(localVarBody),
				err,
			)
		}

		return &v, nil, nil
	case _nethttp.StatusInternalServerError:
		var v types.Error
		err = a.client.decode(&v, localVarBody, localVarHTTPResponse.Header.Get("Content-Type"))
		if err != nil {
			return nil, nil, fmt.Errorf(
				"failed to decode when hit status code 500, response body %s: %w",
				string(localVarBody),
				err,
			)
		}

		return nil, &v, fmt.Errorf("error %+v", v)
	case _nethttp.StatusBadGateway,
		_nethttp.StatusServiceUnavailable,
		_nethttp.StatusGatewayTimeout,
		_nethttp.StatusRequestTimeout:
		return nil, nil, fmt.Errorf(
			"status code %d, response body %s: %w",
			localVarHTTPResponse.StatusCode,
			string(localVarBody),
			ErrRetriable,
		)
	default:
		return nil, nil, fmt.Errorf(
			"invalid status code %d, response body %s",
			localVarHTTPResponse.StatusCode,
			string(localVarBody),
		)
	}
}
EOF
fi

echo "ConstructionPreprocessOperations API injection completed!"