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
	"crypto/sha512"
	"encoding/json"
	"errors"
	"fmt"
	"io"

	"github.com/bradfitz/gomemcache/memcache"
	"golang.org/x/crypto/hkdf"
)

// MappingCache is an interaface to cache ElasticMapping data
//
// Key is constructed from database and table names.
type MappingCache interface {
	// Store saves ElasticMapping entry for given database and table.
	Store(idxName string, mapping *ElasticMapping) error

	// Fetch loads ElasticMapping entry for given database and table.
	// It returns nil, false if no mapping was found.
	Fetch(idxName string) (*ElasticMapping, error)
}

// DummyCache is a MappingCache that does not support storing
// and always fetches nothing.
type DummyCache struct{}

func (d DummyCache) Store(idxName string, mapping *ElasticMapping) error {
	return nil
}

func (d DummyCache) Fetch(idxName string) (*ElasticMapping, error) {
	return nil, nil
}

// MemcacheMappingCache is a MappingCache backed by memcached
type MemcacheMappingCache struct {
	client            *memcache.Client
	tenantID          string
	secret            []byte // input entropy for key creation
	defaultExpiration int32  // default expiration time; see memcache.Item.Expiration
}

// NewMemcacheMappingCache creates new MemcacheMappingCache instance.
func NewMemcacheMappingCache(client *memcache.Client, tenantID string, secret string, defaultExpiration int) *MemcacheMappingCache {
	return &MemcacheMappingCache{
		client:            client,
		tenantID:          tenantID,
		secret:            []byte(secret),
		defaultExpiration: int32(defaultExpiration),
	}
}

// key calculates value for use as memcache.Item.Key
func (m *MemcacheMappingCache) key(idxName string) string {
	strid := fmt.Sprintf("%s:%s", m.tenantID, idxName)
	hash := sha512.Sum512([]byte(strid))
	return fmt.Sprintf("ep:mapping:%x", hash)
}

func (m *MemcacheMappingCache) keysrc() io.Reader {
	return hkdf.New(sha512.New, m.secret, nil, nil)
}

func (m *MemcacheMappingCache) Store(idxName string, mapping *ElasticMapping) error {
	v, err := json.Marshal(mapping)
	if err != nil {
		return err
	}

	box, err := encrypt(v, m.keysrc())
	if err != nil {
		return err
	}

	serialized, err := json.Marshal(box)
	if err != nil {
		return err
	}

	item := &memcache.Item{
		Key:        m.key(idxName),
		Value:      serialized,
		Expiration: m.defaultExpiration,
	}

	return m.client.Set(item)
}

func (m *MemcacheMappingCache) Fetch(idxName string) (*ElasticMapping, error) {
	v, err := m.client.Get(m.key(idxName))
	if err != nil {
		if errors.Is(err, memcache.ErrCacheMiss) {
			return nil, nil
		}

		return nil, err
	}

	box := new(aeadBox)
	err = json.Unmarshal(v.Value, box)
	if err != nil {
		return nil, err
	}

	jsondata, err := box.decrypt(m.keysrc())
	if err != nil {
		return nil, err
	}

	mapping := new(ElasticMapping)
	err = json.Unmarshal(jsondata, mapping)
	if err != nil {
		return nil, err
	}

	return mapping, nil
}
