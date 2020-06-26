package keys

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"

	"github.com/btcsuite/btcd/btcec"
)

func GenerateKeypair(curve CurveType) (*KeyPair, error) {
	var keyPair *KeyPair

	switch {
	case curve.IsSecp256k1():
		rawPrivKey, err := btcec.NewPrivateKey(btcec.S256())
		if err != nil {
			return nil, fmt.Errorf("keygen: new private key. %w", err)
		}
		rawPubKey := rawPrivKey.PubKey()

		pubKey := &PublicKey{
			PubKeyHex: hex.EncodeToString(rawPubKey.SerializeCompressed()),
			Curve:     curve,
		}
		privKey := &PrivateKey{
			PrivKeyHex: hex.EncodeToString(rawPrivKey.Serialize()),
			Curve:      curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	case curve.IsEdwards25519():
		rawPubKey, rawPrivKey, err := ed25519.GenerateKey(nil)
		if err != nil {
			return nil, fmt.Errorf("keygen: new keypair. %w", err)
		}

		pubKey := &PublicKey{
			PubKeyHex: hex.EncodeToString(rawPubKey),
			Curve:     curve,
		}
		privKey := &PrivateKey{
			PrivKeyHex: hex.EncodeToString(rawPrivKey.Seed()),
			Curve:      curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}

	return keyPair, nil
}

func (k KeyPair) IsValid() (bool, error) {
	pk, _ := hex.DecodeString(k.PublicKey.PubKeyHex)
	sk, _ := hex.DecodeString(k.PrivateKey.PrivKeyHex)
	pkCurve := k.PublicKey.Curve
	skCurve := k.PrivateKey.Curve

	if pkCurve != skCurve {
		return false, fmt.Errorf("private key curve %s and public key curve %s do not match", skCurve, pkCurve)
	}

	// Secp256k1 Pubkeys are 33-bytes
	if pkCurve.IsSecp256k1() && len(pk) != 33 {
		return false, fmt.Errorf("invalid pubkey length %v", len(pk))
	}

	// Ed25519 pubkeys are 32-bytes
	if pkCurve.IsEdwards25519() && len(pk) != 32 {
		return false, fmt.Errorf("invalid pubkey length %v", len(pk))
	}

	// All privkeys are 32-bytes
	if len(sk) != 32 {
		return false, fmt.Errorf("invalid privkey length %v", len(sk))
	}

	return true, nil
}
