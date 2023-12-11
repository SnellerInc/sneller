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
	"fmt"
	"log"
	"net/http"

	"github.com/bradfitz/gomemcache/memcache"
)

// HandlerContext is all data required to process an HTTP request.
type HandlerContext struct {
	Request *http.Request
	Writer  http.ResponseWriter
	Config  *Config
	Logging *Logging
	Client  *http.Client
	Mapping *mappingEntry // currently selected mapping for an index
	Cache   MappingCache

	// function performing verbose logging
	VerboseLog func(string, ...any)

	// Flag indicating we're using verbose logging;
	// provided in case some logging activities might
	// cost more than a single VerboseLog call.
	Verbose bool

	Memcache struct {
		// Optional memcache client
		Client *memcache.Client
		// The ID used to distinguish ElasticProxy instances
		TenantID string
		// A string used to create a crypto key
		Secret string
		// Item expiration timeout
		ExpirationTime int
	}
}

var dummyCache DummyCache

// NewHandlerContext creates a new context based on the ElasticSearch configuration and the handled request.
func NewHandlerContext(config *Config, client *http.Client, w http.ResponseWriter, r *http.Request, verbose bool, verboseLog func(format string, v ...any)) *HandlerContext {
	return &HandlerContext{
		Config:     config,
		Logging:    newLogging(r),
		Client:     client,
		Request:    r,
		Writer:     w,
		Cache:      dummyCache,
		Verbose:    verbose,
		VerboseLog: verboseLog,
	}
}

func (c *HandlerContext) SelectIndex(index string) bool {
	m, ok := c.Config.Mapping[index]
	if !ok {
		return false
	}

	c.Mapping = m
	c.Logging.Index = index

	if c.Memcache.Client != nil && c.Cache == dummyCache {
		c.Cache = NewMemcacheMappingCache(
			c.Memcache.Client,
			c.Memcache.TenantID,
			c.Memcache.Secret,
			c.Memcache.ExpirationTime)
	}

	return true
}

func (c *HandlerContext) AddHeader(k, v string) {
	c.Writer.Header().Add(k, v)
}

func (c *HandlerContext) Error(status int, s string, args ...any) {
	msg := fmt.Sprintf(s, args...)
	http.Error(c.Writer, msg, status)
	r := c.Request
	log.Printf("%s %v[%s]: %s", r.Method, r.URL, r.RemoteAddr, msg)
}

func (c *HandlerContext) NotFound(s string, args ...any) {
	c.Error(http.StatusNotFound, s, args...)
}

func (c *HandlerContext) InternalServerError(s string, args ...any) {
	c.Error(http.StatusInternalServerError, s, args...)
}

func (c *HandlerContext) BadRequest(s string, args ...any) {
	c.Error(http.StatusBadRequest, s, args...)
}

func (c *HandlerContext) NeedsAuthentication() bool {
	return c.Config.Elastic.User != "" || c.Config.Elastic.Password != ""
}

func (c *HandlerContext) Authenticate(username, password string) bool {
	return username == c.Config.Elastic.User && password == c.Config.Elastic.Password
}

func (c *HandlerContext) HasSnellerEndpoint() bool {
	return c.Config.Sneller.EndPoint != nil
}
