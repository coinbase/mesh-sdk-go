package keys

// CurveType is the type of curve associated with a PublicKey.
type CurveType string

// SignatureType is the type of a cryptographic signature.
type SignatureType string

// PublicKey contains the hex-encoded pubkey bytes as well as the CurveType
type PublicKey struct {
	PubKeyHex string
	Curve     CurveType
}

// PrivateKey contains the hex-encoded privkey bytes as well as the CurveType
type PrivateKey struct {
	PrivKeyHex string
	Curve      CurveType
}

// KeyPair contains a PrivateKey and its' associated PublicKey
type KeyPair struct {
	PublicKey  *PublicKey
	PrivateKey *PrivateKey
}

// SigningPayload is signed by the client with the keypair associated
// with an address using the specified SignatureType.
type SigningPayload struct {
	Address       string // in network-specific format
	PayloadHex    string
	SignatureType SignatureType
}

// Signature contains the payload that was signed, the public keys of the
// keypairs used to produce the signature, the hex-encoded signature,
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
	Secp256k1Curve    = "secp256k1"    // public key is SEC compressed 33-bytes
	Edwards25519Curve = "edwards25519" // public key is y (255-bits) || x-sign-bit (32-bytes)

	// Signatures
	EcdsaSignature               = "ecdsa"                 // r (32-bytes) || s (32-bytes)
	EcdsaPubkeyRecoverySignature = "ecdsa_pubkey_recovery" // r (32-bytes) || s (32-bytes) || v (1-byte)
	Ed25519Signature             = "ed25519"               // R (32-byte) || s (32-bytes)
)

func (c CurveType) IsEdwards25519() bool {
	return c == Edwards25519Curve
}

func (c CurveType) IsSecp256k1() bool {
	return c == Secp256k1Curve
}

func (s SignatureType) IsEcdsa() bool {
	return s == EcdsaSignature
}

func (s SignatureType) IsEcdsaPubkeyRecovery() bool {
	return s == EcdsaPubkeyRecoverySignature
}

func (s SignatureType) IsEd25519() bool {
	return s == Ed25519Signature
}
