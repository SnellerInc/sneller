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
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"os"
	"regexp"
	"sync"
	"testing"

	"github.com/SnellerInc/sneller/db"
)

// tsbuf is a threadsafe buffer;
// we use this for logging test failures
// in order to make 'go test -race' happy
type tsbuf struct {
	sync.Mutex
	bytes.Buffer
}

func (t *tsbuf) Write(p []byte) (int, error) {
	t.Lock()
	defer t.Unlock()
	return t.Buffer.Write(p)
}

// return a log.Logger that only dumps
// output if the test actually fails
func testlogger(t *testing.T) *log.Logger {
	var buf tsbuf
	l := log.New(&buf, t.Name(), log.LstdFlags)
	t.Cleanup(func() {
		if t.Failed() {
			buf.Lock()
			defer buf.Unlock()
			buf.WriteTo(os.Stderr)
		}
	})
	return l
}

type requester struct {
	t    *testing.T
	host string
}

func (r *requester) get(uri string) *http.Request {
	req, err := http.NewRequest(http.MethodGet, r.host+uri, nil)
	if err != nil {
		r.t.Fatal(err)
	}
	return req
}

func (r *requester) getQuery(db, query string) *http.Request {
	var uri string
	if db == "" {
		uri = fmt.Sprintf("/executeQuery?query=%s", url.QueryEscape(query))
	} else {
		uri = fmt.Sprintf("/executeQuery?database=%s&query=%s",
			url.QueryEscape(db), url.QueryEscape(query))
	}
	req := r.get(uri)
	req.Header.Set("TE", "trailers")
	req.Header.Set("Authorization", "Bearer snellerd-test")
	return req
}

func (r *requester) getQueryJSON(db, query string) *http.Request {
	req := r.getQuery(db, query)
	req.Header.Set("Accept", "application/json")
	return req
}

func (r *requester) getDBs() *http.Request {
	req := r.get("/databases")
	req.Header.Set("Authorization", "Bearer snellerd-test")
	return req
}

func (r *requester) getTables(db string) *http.Request {
	req := r.get(fmt.Sprintf("/tables?database=%s", url.QueryEscape(db)))
	req.Header.Set("Authorization", "Bearer snellerd-test")
	return req
}

func (r *requester) getInputs(db, table string) *http.Request {
	req := r.get(fmt.Sprintf("/inputs?database=%s&table=%s", url.QueryEscape(db), url.QueryEscape(table)))
	req.Header.Set("Authorization", "Bearer snellerd-test")
	return req
}

type testAuth struct {
	self db.Tenant
}

func (a testAuth) Authorize(_ context.Context, token string) (db.Tenant, error) {
	if token == "snellerd-test" {
		return a.self, nil
	}
	return nil, errors.New("no such tenant: " + token)
}

func empty(t *testing.T) *server {
	tt := testdirEnviron(t)
	s := &server{
		logger:    testlogger(t),
		sandbox:   false,
		cachedir:  t.TempDir(),
		tenantcmd: []string{"./stub"},
		peers:     noPeers{},
		auth:      testAuth{tt},
	}
	t.Cleanup(func() {
		s.Close()
	})
	return s
}

func TestBadRequest(t *testing.T) {
	testFiles(t)
	s := empty(t)

	httpsock := listen(t)
	go s.Serve(httpsock, nil)

	rqe := &requester{
		t:    t,
		host: "http://" + httpsock.Addr().String(),
	}

	// test that the responses for this
	// particular set of queries is 400
	// plus some particular error text
	queries := []struct {
		text, match string
	}{
		{"SELECT 3||x FROM parking", "ill-typed"},
		{"SELECT LEAST(TRIM(x)) FROM parking WHERE x = 3", "ill-typed"},
	}

	cl := http.DefaultClient
	for i := range queries {
		r := rqe.getQuery("default", queries[i].text)
		res, err := cl.Do(r)
		if err != nil {
			t.Fatal(err)
		}
		if res.StatusCode != http.StatusBadRequest {
			t.Errorf("got status code %d", res.StatusCode)
		}
		bodytext, err := io.ReadAll(res.Body)
		res.Body.Close()
		if err != nil {
			t.Fatal(err)
		}
		if ok, _ := regexp.Match(queries[i].match, bodytext); !ok {
			t.Errorf("text %q didn't match %s", bodytext, queries[i].match)
		}
		t.Logf("error text: %q", bodytext)
	}
}
