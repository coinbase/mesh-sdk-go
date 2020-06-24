package keys

import (
	"crypto/ed25519"
	"encoding/hex"
	"fmt"
	"log"

	"github.com/btcsuite/btcd/btcec"
	"github.com/coinbase/rosetta-sdk-go/types"
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
			HexEncodedBytes: hex.EncodeToString(rawPubKey.SerializeCompressed()),
			Curve:           curve,
		}
		privKey := &PrivateKey{
			HexEncodedBytes: hex.EncodeToString(rawPrivKey.Serialize()),
			Curve:           curve,
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
			HexEncodedBytes: hex.EncodeToString(rawPubKey),
			Curve:           curve,
		}
		privKey := &PrivateKey{
			HexEncodedBytes: hex.EncodeToString(rawPrivKey),
			Curve:           curve,
		}

		keyPair = &KeyPair{
			PublicKey:  pubKey,
			PrivateKey: privKey,
		}

	default:
		return nil, fmt.Errorf("%s is not supported", curve)
	}

	log.Printf("Generated keypair %s\n", types.PrettyPrintStruct(keyPair))

	return keyPair, nil
}
