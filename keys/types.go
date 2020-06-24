package keys

// CurveType is the type of curve associated with a PublicKey.
type CurveType string

// SignatureType is the type of a cryptographic signature.
type SignatureType string

type PublicKey struct {
	HexEncodedBytes string
	Curve           CurveType
}

type PrivateKey struct {
	HexEncodedBytes string // in hex
	Curve           CurveType
}

type KeyPair struct {
	PublicKey  *PublicKey
	PrivateKey *PrivateKey
}

// SigningPayload is signed by the client with the keypair associated
// with an address using the specified SignatureType.
type SigningPayload struct {
	Address         string // in network-specific format (often times it is not possible to determine the public key from the address a priori)
	HexEncodedBytes string
	SignatureType   SignatureType // signature for payload must be SignatureType
}

// Signature contains the payload that was signed, the public keys of the
// keypairs used to produce the signature, the signature (encoded in hex),
// and the SignatureType.
type Signature struct {
	Payload *SigningPayload

	// PublicKey is often times not known during construction of the signing
	// payloads by the Wallet API Implementation but may be needed for
	// CombineSignatures to work correctly.
	PublicKey     *PublicKey
	SignatureType SignatureType
	Signature     string // in hex
}

const (
	// Curves
	SECP256K1_CURVE    = "secp256k1"    // public key is SEC compressed 33-bytes
	EDWARDS25519_CURVE = "edwards25519" // public key is y (255-bits) || x-sign-bit (32-bytes)

	// Signatures
	ECDSA_SIGNATURE                 = "ecdsa"                 // r (32-bytes) || s (32-bytes)
	ECDSA_PUBKEY_RECOVERY_SIGNATURE = "ecdsa_pubkey_recovery" // r (32-bytes) || s (32-bytes) || v (1-byte)
	ED25519_SIGNATURE               = "ed25519"               // R (32-byte) || s (32-bytes)
)

func (c CurveType) IsEdwards25519() bool {
	return c == EDWARDS25519_CURVE
}

func (c CurveType) IsSecp256k1() bool {
	return c == SECP256K1_CURVE
}

func (s SignatureType) IsEcdsa() bool {
	return s == ECDSA_SIGNATURE
}

func (s SignatureType) IsEcdsaPubkeyRecovery() bool {
	return s == ECDSA_PUBKEY_RECOVERY_SIGNATURE
}

func (s SignatureType) IsEd25519() bool {
	return s == ED25519_SIGNATURE
}
