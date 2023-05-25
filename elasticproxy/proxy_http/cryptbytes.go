// Copyright (C) 2023 Sneller, Inc.
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as published by
// the Free Software Foundation, either version 3 of the License, or
// (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program.  If not, see <http://www.gnu.org/licenses/>.

package proxy_http

import (
	"crypto/cipher"
	"crypto/rand"
	"fmt"
	"io"

	"golang.org/x/crypto/chacha20poly1305"
)

type aeadBox struct {
	Nonce   []byte `json:"nonce"`
	Payload []byte `json:"payload"`
}

func encrypt(src []byte, keysrc io.Reader) (*aeadBox, error) {
	aead := createAEAD(keysrc)

	// payload is just nonce plus encrypted data
	nonce := make([]byte, aead.NonceSize())
	_, err := io.ReadFull(rand.Reader, nonce)
	if err != nil {
		return nil, err
	}

	return &aeadBox{
		Nonce:   nonce,
		Payload: aead.Seal(nil, nonce, src, nil),
	}, nil
}

func (b *aeadBox) decrypt(keysrc io.Reader) ([]byte, error) {
	aead := createAEAD(keysrc)

	if len(b.Nonce) != aead.NonceSize() {
		return nil, fmt.Errorf("decrypt: invalid nonce size %d", len(b.Nonce))
	}

	return aead.Open(b.Payload[:0], b.Nonce, b.Payload, nil)
}

func createAEAD(keysrc io.Reader) cipher.AEAD {
	key := make([]byte, chacha20poly1305.KeySize)
	_, err := io.ReadFull(keysrc, key)
	if err != nil {
		panic(err) // should never happen
	}

	aead, err := chacha20poly1305.NewX(key)
	if err != nil {
		panic(err)
	}

	return aead
}
