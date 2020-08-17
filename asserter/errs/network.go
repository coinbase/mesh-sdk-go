package errs

import "errors"

var (
	ErrSubNetworkIdentifierInvalid = errors.New(
		"NetworkIdentifier.SubNetworkIdentifier.Network is missing",
	)

	ErrNetworkIdentifierIsNil = errors.New("NetworkIdentifier is nil")

	ErrNetworkIdentifierBlockchainMissing = errors.New("NetworkIdentifier.Blockchain is missing")

	ErrNetworkIdentifierNetworkMissing = errors.New("NetworkIdentifier.Network is missing")

	ErrPeerIDMissing = errors.New("Peer.PeerID is missing")

	ErrVersionIsNil = errors.New("version is nil")

	ErrVersionNodeVersionMissing = errors.New("Version.NodeVersion is missing")

	ErrVersionMiddlewareVersionMissing = errors.New("Version.MiddlewareVersion is missing")

	ErrNetworkStatusResponseIsNil = errors.New("network status response is nil")

	ErrNoAllowedOperationStatuses = errors.New("no Allow.OperationStatuses found")

	ErrOperationStatusMissing = errors.New("Operation.Status is missing")

	ErrNoSuccessfulAllowedOperationStatuses = errors.New(
		"no successful Allow.OperationStatuses found",
	)

	ErrErrorIsNil = errors.New("Error is nil")

	ErrErrorCodeIsNeg = errors.New("Error.Code is negative")

	ErrErrorMessageMissing = errors.New("Error.Message is missing")

	ErrErrorCodeUsedMultipleTimes = errors.New("error code used multiple times")

	ErrAllowIsNil = errors.New("Allow is nil")

	ErrNetworkOptionsResponseIsNil = errors.New("options is nil")

	ErrNetworkListResponseIsNil = errors.New("NetworkListResponse is nil")

	ErrNetworkListResponseNetworksContinsDuplicates = errors.New(
		"NetworkListResponse.Networks contains duplicates",
	)
)
