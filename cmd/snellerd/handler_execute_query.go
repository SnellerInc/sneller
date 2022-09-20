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
	"crypto/sha256"
	"encoding/base64"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/SnellerInc/sneller"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/plan/pir"
	"github.com/SnellerInc/sneller/tenant"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/google/uuid"
)

func itoa(i int64) string {
	return strconv.FormatInt(i, 10)
}

func flush(w http.ResponseWriter) {
	if f, ok := w.(http.Flusher); ok {
		f.Flush()
	}
}

func contains(list []string, item string) bool {
	for i := range list {
		if list[i] == item {
			return true
		}
	}
	return false
}

func setError(w http.ResponseWriter) {
	w.Header().Set("Server-Timing", "error;desc=\"Query Execution Error\"")
}

func setTiming(w http.ResponseWriter, elapsed time.Duration, stats *plan.ExecStats) {
	w.Header().Add("Server-Timing", fmt.Sprintf("exec;dur=%g, miss;desc=\"Cache Misses\";count=%d, hit;desc=\"Cache Hits\";count=%d, scanned;desc=\"Bytes Scanned\";count=%d",
		float64(elapsed)/float64(time.Millisecond), stats.CacheMisses, stats.CacheHits, stats.BytesScanned))
}

// after 15 minutes, stop waiting for a result
// and SIGQUIT the child process
const queryKillTimeout = 15 * time.Minute

// example invocation:
// curl -v -H 'Authorization: sneller' -H 'Accept: application/ion' 'http://localhost:8080/executeQuery?database=sf1-new&query=SELECT%20%2A%20FROM%20nation%20LIMIT%2010'
// curl -v -X POST -H 'Authorization: sneller' -H 'Accept: application/ion' --data-raw 'SELECT * FROM nation LIMIT 10' 'http://localhost:8080/executeQuery?database=sf1-new'
func (s *server) executeQueryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	creds, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}
	authElapsed := time.Since(start)

	var query []byte
	switch r.Method {
	case http.MethodGet, http.MethodHead:
		str := r.URL.Query().Get("query")
		if str == "" {
			http.Error(w, "no query parameter", http.StatusBadRequest)
			return
		}
		query = []byte(str)

	case http.MethodPost:
		// restrict the size of the query text to something reasonable
		body := http.MaxBytesReader(w, r.Body, 128*1024*1024)
		query, err = io.ReadAll(body)
		if err != nil {
			http.Error(w, "cannot read query", http.StatusBadRequest)
			return
		}
	}

	// Determine the output format
	explicitJSON := r.URL.Query().Has("json")
	var encodingFormat tnproto.OutputFormat
	acceptHeader := r.Header.Get("Accept")
	switch acceptHeader {
	case "application/x-ndjson", "application/x-jsonlines":
		encodingFormat = tnproto.OutputChunkedJSON
	case "application/ion":
		if explicitJSON {
			http.Error(w, fmt.Sprintf("can't request JSON and explicitly accept %q", acceptHeader), http.StatusBadRequest)
			return
		}
		encodingFormat = tnproto.OutputChunkedIon
	case "application/json":
		encodingFormat = tnproto.OutputChunkedJSONArray
	case "", "*/*":
		if explicitJSON {
			encodingFormat = tnproto.OutputChunkedJSON
			acceptHeader = "application/x-ndjson"
		} else {
			encodingFormat = tnproto.OutputChunkedIon
			acceptHeader = "application/ion"
		}
	default:
		s.logger.Printf("invalid accept header value %q", acceptHeader)
		http.Error(w, "invalid 'Accept' header", http.StatusBadRequest)
		return
	}

	defaultDatabase := r.URL.Query().Get("database")
	parsedQuery, err := partiql.Parse(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	normalized := parsedQuery.Text()
	redacted := parsedQuery.Text()

	var id tnproto.ID
	var key tnproto.Key
	hash := sha256.Sum256([]byte(creds.ID()))
	copy(id[:], hash[:])
	hash = sha256.Sum256([]byte(creds.ID() + string(creds.Key()[:])))
	copy(key[:], hash[:])

	planEnv, err := sneller.Environ(creds, defaultDatabase)
	if err != nil {
		http.Error(w, "tenant ID disallowed", http.StatusForbidden)
		s.logger.Printf("refusing query: %s", err)
		return
	}
	endPoints := s.peers.Get()

	queryID := uuid.New()
	w.Header().Add("X-Sneller-Query-ID", queryID.String())

	var tree *plan.Tree
	start = time.Now()
	if len(endPoints) == 0 {
		tree, err = plan.New(parsedQuery, planEnv)
	} else {
		planSplitter := s.newSplitter(id, key, endPoints)
		tree, err = plan.NewSplit(parsedQuery, planEnv, planSplitter)
		if err == nil {
			w.Header().Set("X-Sneller-Max-Scanned-Bytes", itoa(planSplitter.MaxScan))
		}
	}
	if err != nil {
		s.logger.Printf("query id %s planning failed: %s", queryID, err)
		planError(w, err)
		return
	}
	s.logger.Printf("query id %s auth %s planning %s", queryID, authElapsed, time.Since(start))

	planHash, newestBlobTime := planEnv.CacheValues()

	// hash the tenant/query/plan/format to an eTag
	hasher := sha256.New()
	hasher.Write([]byte(creds.ID()))
	io.WriteString(hasher, normalized)
	hasher.Write(planHash)
	hasher.Write([]byte{byte(encodingFormat)})
	eTag := `"` + base64.RawStdEncoding.EncodeToString(hasher.Sum(nil)) + `"`

	// Add the ETag to the response
	w.Header().Add("ETag", eTag)
	w.Header().Add("Last-Modified", newestBlobTime.UTC().Format(http.TimeFormat))
	w.Header().Add("Cache-Control", "private, must-revalidate")
	w.Header().Add("Vary", "Accept, Authentication")

	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		// Check the 'If-None-Match' request header
		ifNoneMatch := r.Header.Get("If-None-Match")
		if ifNoneMatch != "" {
			for _, matchEtag := range strings.Split(ifNoneMatch, ",") {
				matchEtag = strings.TrimSpace(matchEtag)
				if eTag == matchEtag {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		} else {
			// Check the 'If-Modified-Since' request header
			ifModifiedSince := r.Header.Get("If-Modified-Since")
			if ifModifiedSince != "" {
				ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
				if err != nil {
					w.WriteHeader(http.StatusBadRequest)
					return
				}

				if !newestBlobTime.After(ifModifiedSinceTime) {
					w.WriteHeader(http.StatusNotModified)
					return
				}
			}
		}
	}

	w.Header().Add("Content-Type", acceptHeader)
	if r.Method == http.MethodHead {
		w.WriteHeader(http.StatusOK)
		return
	}
	sendTrailer := contains(r.Header.Values("TE"), "trailers")
	if sendTrailer {
		w.Header().Add("Trailer", "Server-Timing")
	}

	conn := &delayedHijack{
		laddr: s.bound,
		req:   r,
		res:   w,
	}
	startrun := time.Now()
	rc, err := s.manager.Do(id, key, tree, encodingFormat, conn)
	if err != nil {
		if !conn.hijacked {
			// didn't call w.WriteHeader() yet;
			// we can write a plaintext error
			w.Header().Del("Trailer")
			w.Header().Set("Content-Type", "text/plain")
			if errors.Is(err, tenant.ErrOverloaded) {
				w.WriteHeader(http.StatusTooManyRequests)
			} else {
				w.WriteHeader(http.StatusInternalServerError)
			}
		} else {
			if sendTrailer {
				setError(w)
			}
			if encodingFormat == tnproto.OutputChunkedIon {
				writeError(w, "error dispatching query")
			}
		}
		s.logger.Printf("query ID %s %q execution failed (do): %v", queryID, redacted, err)
		return
	}
	go func() {
		<-r.Context().Done()
		rc.Close()
	}()
	s.logger.Printf("query ID %s plan transfer took %s", queryID, time.Since(startrun))
	var stats plan.ExecStats
	deadlined := setDeadline(rc, queryKillTimeout)
	err = tenant.Check(rc, &stats)
	if err != nil {
		canceled := false
		if ctxerr := r.Context().Err(); ctxerr != nil {
			// see if we got an error due to cancellation
			err = ctxerr
			canceled = true
		}
		if sendTrailer {
			setError(w)
		}
		if canceled {
			s.logger.Printf("query ID %s canceled after %s", queryID, time.Since(startrun))
			return
		}
		s.logger.Printf("query ID %s %q execution failed (check): %v", queryID, redacted, err)
		if deadlined && isTimeout(err) {
			s.logger.Printf("query ID %s killing tenant ID %s due to timeout", queryID, id)
			s.manager.Quit(id)
		}
		return
	}
	elapsed := time.Since(startrun)
	if sendTrailer {
		setTiming(w, elapsed, &stats)
	}
	if encodingFormat == tnproto.OutputChunkedIon {
		writeStatus(w, &stats)
	}
	s.logger.Printf("query id %s duration %s bytes %d hits %d misses %d",
		queryID, elapsed, stats.BytesScanned, stats.CacheHits, stats.CacheMisses)
}

// satisfied by net.Conn and friends
type readDeadliner interface {
	SetReadDeadline(time.Time) error
}

func setDeadline(rc io.Reader, timeout time.Duration) bool {
	if rd, ok := rc.(readDeadliner); ok {
		return rd.SetReadDeadline(time.Now().Add(timeout)) == nil
	}
	return false
}

func isTimeout(err error) bool {
	for e := err; e != nil; e = errors.Unwrap(e) {
		ne, ok := err.(net.Error)
		if ok && ne.Timeout() {
			return true
		}
	}
	return false
}

func isBadQuery(err error, w http.ResponseWriter) bool {
	var emptySyntax *expr.SyntaxError
	var emptyType *expr.TypeError
	var emptyCompile *pir.CompileError
	if errors.As(err, &emptySyntax) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, emptySyntax.Error())
		io.WriteString(w, "\n")
		return true
	}
	if errors.As(err, &emptyType) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, emptyType.Error())
		io.WriteString(w, "\n")
		return true
	}
	if errors.As(err, &emptyCompile) {
		w.WriteHeader(http.StatusBadRequest)
		emptyCompile.WriteTo(w)
		return true
	}
	return false
}

// when handling an error from plan.New, determine
// if the error is a user error (a bad query, for example),
// in which case the error is safe to display directly
// to the user (and the status code ought to be 4xx)
//
// type and syntax errors are returned as 400,
// fs.ErrNotExist errors are returned as 404,
// and others are returned as 500
func planError(w http.ResponseWriter, err error) {
	w.Header().Set("Content-Type", "text/plain")
	if errors.Is(err, fs.ErrNotExist) {
		w.WriteHeader(http.StatusNotFound)
		io.WriteString(w, "table does not exist\n")
		return
	}
	if isBadQuery(err, w) {
		return
	}
	w.WriteHeader(http.StatusInternalServerError)
	io.WriteString(w, "couldn't create query plan\n")
}

func writeError(w http.ResponseWriter, errtext string) {
	var tmp ion.Buffer
	var st ion.Symtab
	resultsym := st.Intern("final_status")
	errsym := st.Intern("error")
	st.Marshal(&tmp, true)
	tmp.BeginAnnotation(1)
	tmp.BeginField(resultsym)
	tmp.BeginStruct(-1)
	tmp.BeginField(errsym)
	tmp.WriteString(errtext)
	tmp.EndStruct()
	tmp.EndAnnotation()
	w.Write(tmp.Bytes())
}

func writeStatus(w http.ResponseWriter, stats *plan.ExecStats) {
	var tmp ion.Buffer
	var st ion.Symtab
	resultsym := st.Intern("final_status")
	tmp.BeginAnnotation(1)
	tmp.BeginField(resultsym)
	stats.Encode(&tmp, &st)
	tmp.EndAnnotation()
	split := tmp.Size()
	st.Marshal(&tmp, true)
	w.Write(tmp.Bytes()[split:])
	w.Write(tmp.Bytes()[:split])
}
