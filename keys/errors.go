package keys

import "errors"

// Named error types for Keys errors
var (
	ErrPrivKeyUndecodable   = errors.New("could not decode privkey")
	ErrPrivKeyLengthInvalid = errors.New("invalid privkey length")

	ErrSignUnsupportedPayloadSignatureType = errors.New(
		"sign: unexpected payload.SignatureType while signing",
	)
	ErrSignUnsupportedSigType = errors.New(
		"sign: unexpected Signature type while signing",
	)
	ErrSignFailed = errors.New("sign: unable to sign. %w")

	ErrVerifyUnsupportedPayloadSignatureType = errors.New(
		"verify: unexpected payload.SignatureType while verifying",
	)
	ErrVerifyUnsupportedSignatureType = errors.New(
		"verify: unexpected Signature type while verifying",
	)
	ErrVerifyFailed = errors.New("verify: verify returned false")
)
