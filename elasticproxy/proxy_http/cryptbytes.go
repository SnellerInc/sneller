// Copyright 2023 Sneller, Inc.
//
//  Licensed under the Apache License, Version 2.0 (the "License");
//  you may not use this file except in compliance with the License.
//  You may obtain a copy of the License at
//
//    http://www.apache.org/licenses/LICENSE-2.0
//
//  Unless required by applicable law or agreed to in writing, software
//  distributed under the License is distributed on an "AS IS" BASIS,
//  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
//  See the License for the specific language governing permissions and
//  limitations under the License.

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
