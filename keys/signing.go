package keys

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"

	"github.com/ethereum/go-ethereum/crypto/secp256k1"
)

func signSecp256k1(privKeyBytes, payload []byte) ([]byte, error) {
	sig, err := secp256k1.Sign(payload, privKeyBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to sign. %w", err)
	}

	return sig, nil
}

func signEd25519(privKeyBytes, payload []byte) []byte {
	privKey := ed25519.PrivateKey(privKeyBytes)
	sig := ed25519.Sign(privKey, payload)

	return sig
}

func SignPayload(payload *SigningPayload, keypair *KeyPair) (*Signature, error) {
	privKeyBytes, err := hex.DecodeString(keypair.PrivateKey.HexEncodedBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode private key. %w", err)
	}

	decodedMessage, err := hex.DecodeString(payload.HexEncodedBytes)
	if err != nil {
		return nil, fmt.Errorf("sign: unable to decode message. %w", err)
	}

	curve := keypair.PrivateKey.Curve
	switch {
	case curve.IsSecp256k1():
		sig, err := signSecp256k1(privKeyBytes, decodedMessage)
		if err != nil {
			return nil, err
		}

		return &Signature{
			Payload:       payload,
			Signature:     hex.EncodeToString(sig), // in form R || S || V (https://docs.google.com/spreadsheets/d/1Jyxmi6FdZhcbZxJkkm5HTvUSfCsEtKo-JjB6N2liVMU/edit#gid=1014148769)
			PublicKey:     keypair.PublicKey,
			SignatureType: payload.SignatureType,
		}, nil
	case curve.IsEdwards25519():
		sig := ed25519.Sign(privKeyBytes, decodedMessage)

		return &Signature{
			Payload:       payload,
			Signature:     hex.EncodeToString(sig), // in form R || S
			PublicKey:     keypair.PublicKey,
			SignatureType: payload.SignatureType,
		}, nil
	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}
}

func verifyEd25519(pubKey, decodedMessage, decodedSignature []byte) bool {
	return ed25519.Verify(pubKey, decodedMessage, decodedSignature)
}

func verifySecp256k1(pubKey, decodedMessage, decodedSignature []byte) (bool, error) {
	var normalizedSig []byte
	switch len(decodedSignature) {
	case 65:
		normalizedSig = decodedSignature[:64]
	case 64:
		normalizedSig = decodedSignature
	default:
		return false, fmt.Errorf("signature length %d is invalid", len(decodedSignature))
	}

	return secp256k1.VerifySignature(pubKey, decodedMessage, normalizedSig), nil
}

func Verify(signature *Signature) (bool, error) {
	curve := signature.PublicKey.Curve
	pubKey, err := hex.DecodeString(signature.PublicKey.HexEncodedBytes)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode pubkey. %w", err)
	}
	decodedMessage, err := hex.DecodeString(signature.Payload.HexEncodedBytes)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode message. %w", err)
	}
	decodedSignature, err := hex.DecodeString(signature.Signature)
	if err != nil {
		return false, fmt.Errorf("verify: unable to decode signature. %w", err)
	}

	switch {
	case curve.IsSecp256k1():
		verify, _ := verifySecp256k1(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	case curve.IsEdwards25519():
		verify := verifyEd25519(pubKey, decodedMessage, decodedSignature)
		return verify, nil
	default:
		return false, fmt.Errorf("%s is not supported", curve)
	}
}
