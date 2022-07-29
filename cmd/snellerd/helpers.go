// Copyright (C) 2022 Sneller, Inc.
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

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"regexp"
	"strconv"
	"strings"

	"github.com/SnellerInc/sneller/db"
)

func (s *server) handle(handler func(http.ResponseWriter, *http.Request), methods ...string) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		defer r.Body.Close()
		// obtain the real address
		remoteAddress := r.RemoteAddr
		forwarded := false
		if forwardedFor := r.Header.Get("X-Forwarded-For"); forwardedFor != "" {
			parts := strings.Split(forwardedFor, ",")
			remoteAddress = strings.TrimSpace(parts[len(parts)-1])
			forwarded = true
		}
		// unforwarded requests to "/"
		// are just ELB heartbeats;
		// don't log these, as they spam the logs
		if r.URL.Path != "/" || forwarded {
			s.logger.Printf("Request %s %s from %s", r.Method, r.URL.Path, remoteAddress)
		}
		if version != "" {
			w.Header().Set("X-Sneller-Version", version)
		}
		w.Header().Set("Access-Control-Allow-Credentials", "true")
		w.Header().Set("Access-Control-Allow-Headers", "Accept, Authorization")
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		for _, httpMethod := range methods {
			if r.Method == httpMethod {
				handler(w, r)
				return
			}
		}
		w.WriteHeader(http.StatusMethodNotAllowed)
	}
}

func (s *server) getTenant(ctx context.Context, w http.ResponseWriter, r *http.Request) (db.Tenant, error) {
	authHeader := r.Header.Get("Authorization")
	if authHeader == "" {
		w.WriteHeader(http.StatusUnauthorized)
		return nil, errors.New("not authorized")
	}

	// Check if it's a bearer token
	parts := strings.SplitN(authHeader, " ", 2)
	if parts[0] != "Bearer" || len(parts) != 2 {
		err := errors.New("invalid authorization header format")
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err.Error())) // TODO: we might want to remove this in production
		return nil, err
	}

	creds, err := s.auth.Authorize(ctx, parts[1])
	if err != nil {
		w.WriteHeader(http.StatusForbidden)
		w.Write([]byte(err.Error())) // TODO: we might want to remove this in production
		return nil, err
	}
	return creds, nil
}

func writeResultResponse(w http.ResponseWriter, statusCode int, v interface{}) {
	result, err := json.Marshal(v)
	if err != nil {
		panic("unable to serialize HTTP response")
	}
	w.Header().Add("Content-Type", "application/json")
	w.Header().Set("Content-Length", strconv.Itoa(len(result)))
	w.WriteHeader(statusCode)
	w.Write(result)
}

func matchPattern(text, pattern string) bool {
	if pattern == "" || pattern == "%" || pattern == text {
		return true
	}

	// Create a regex based on the pattern
	start := 0
	var regex strings.Builder
	regex.WriteString("^")
	for i := 0; i < len(pattern); i++ {
		switch pattern[i] {
		case '_':
			regex.WriteString(regexp.QuoteMeta(pattern[start:i]))
			regex.WriteString(".")
			start = i + 1
		case '%':
			regex.WriteString(regexp.QuoteMeta(pattern[start:i]))
			regex.WriteString(".*")
			start = i + 1
		}
	}

	// No regex, so fast-path should have already detected matches
	if start == 0 {
		return false
	}

	regex.WriteString(regexp.QuoteMeta(pattern[start:]))
	regex.WriteString("$")

	r := regex.String()
	match, err := regexp.Match(r, []byte(text))
	if err != nil {
		panic(fmt.Sprintf("Invalid regex generated: %v", r))
	}
	return match
}

func writeInternalServerResponse(w http.ResponseWriter, err error) {
	// TODO: Remove the error when in production
	http.Error(w, err.Error(), http.StatusInternalServerError)
}
