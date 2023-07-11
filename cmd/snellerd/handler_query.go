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
	"encoding/json"
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
	"github.com/SnellerInc/sneller/db"
	"github.com/SnellerInc/sneller/expr"
	"github.com/SnellerInc/sneller/expr/partiql"
	"github.com/SnellerInc/sneller/ion"
	"github.com/SnellerInc/sneller/plan"
	"github.com/SnellerInc/sneller/plan/pir"
	"github.com/SnellerInc/sneller/tenant"
	"github.com/SnellerInc/sneller/tenant/tnproto"
	"github.com/google/uuid"
)

// DefaultMaxScan is the default number of bytes
// allowed to be scanned in a query. If a query
// might exceed this limit, it will be rejected
// without doing any scanning.
const DefaultMaxScan = 0

type errPlanLimit struct {
	scan, max uint64
}

func (e *errPlanLimit) Error() string {
	return fmt.Sprintf("scan limit exceeded (%d > %d)", e.scan, e.max)
}

func utoa(i uint64) string {
	return strconv.FormatUint(i, 10)
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
// curl -v -H 'Authorization: sneller' -H 'Accept: application/ion' 'http://localhost:8080/query?database=sf1-new&query=SELECT%20%2A%20FROM%20nation%20LIMIT%2010'
// curl -v -X POST -H 'Authorization: sneller' -H 'Accept: application/ion' --data-raw 'SELECT * FROM nation LIMIT 10' 'http://localhost:8080/query?database=sf1-new'
func (s *server) queryHandler(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	start := time.Now()
	creds, err := s.getTenant(ctx, w, r)
	if err != nil {
		return
	}
	authElapsed := time.Since(start)
	tenantID := creds.ID()

	isHeadRequest := r.Method == http.MethodHead

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
		isHeadRequest = r.URL.Query().Has("dry")
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
		http.Error(w, "invalid 'Accept' header", http.StatusBadRequest)
		return
	}

	statsOptIn := r.URL.Query().Has("stats")
	if encodingFormat == tnproto.OutputChunkedJSONArray && statsOptIn {
		http.Error(w, "cannot return stats with normal JSON output (try NDJSON)", http.StatusBadRequest)
		return
	}

	defaultDatabase := r.URL.Query().Get("database")
	parsedQuery, err := partiql.Parse(query)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	err = parsedQuery.Check()
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	normalized := parsedQuery.Text()
	redacted := parsedQuery.Text()

	var id tnproto.ID
	var key tnproto.Key
	hash := sha256.Sum256([]byte(tenantID))
	copy(id[:], hash[:])
	hash = sha256.Sum256([]byte(tenantID + string(creds.Key()[:])))
	copy(key[:], hash[:])

	// determine scan limit
	maxScan := uint64(DefaultMaxScan)
	if ct, ok := creds.(db.TenantConfigurable); ok {
		cfg := ct.Config()
		if cfg != nil && cfg.MaxScanBytes > 0 {
			maxScan = cfg.MaxScanBytes
		}
	}

	planEnv, err := sneller.Environ(creds, defaultDatabase)
	if err != nil {
		http.Error(w, "tenant ID disallowed", http.StatusForbidden)
		s.logger.Printf("refusing query: %s", err)
		return
	}
	endPoints := s.peers.Get()

	queryID := uuid.New().String()
	w.Header().Add("X-Sneller-Query-ID", queryID)

	var tree *plan.Tree
	start = time.Now()
	if len(endPoints) == 0 {
		tree, err = plan.New(parsedQuery, planEnv)
	} else {
		splitter := s.newSplitter(id, key, endPoints)
		tree, err = plan.NewSplit(parsedQuery, struct {
			*sneller.FSEnv
			*sneller.Splitter
		}{planEnv, splitter})
	}
	if err != nil {
		s.logger.Printf("tenant %s query ID %s planning failed: %s", tenantID, queryID, err)
		planError(w, err)
		return
	}
	tree.ID = queryID
	// TODO: clean this up
	if enc, ok := planEnv.Root.(interface {
		Encode(*ion.Buffer, *ion.Symtab) error
	}); ok {
		var buf ion.Buffer
		var st ion.Symtab
		if err := enc.Encode(&buf, &st); err != nil {
			s.logger.Printf("tenant %s query ID %s encoding file system: %s", tenantID, queryID, err)
			planError(w, err)
			return
		}
		tree.Data, _, _ = ion.ReadDatum(&st, buf.Bytes())
	}
	willScan := uint64(tree.MaxScanned())
	w.Header().Set("X-Sneller-Max-Scanned-Bytes", utoa(willScan))
	if maxScan > 0 && willScan > maxScan {
		planError(w, &errPlanLimit{scan: willScan, max: maxScan})
		return
	}
	s.logger.Printf("tenant %s query ID %s auth %s planning %s", tenantID, queryID, authElapsed, time.Since(start))

	planHash, newestBlobTime := planEnv.CacheValues()

	// hash the tenant/query/plan/format to an eTag
	hasher := sha256.New()
	hasher.Write([]byte(tenantID))
	io.WriteString(hasher, normalized)
	hasher.Write(planHash)
	hasher.Write([]byte{byte(encodingFormat)})
	eTag := `"` + base64.RawStdEncoding.EncodeToString(hasher.Sum(nil)) + `"`

	// Add the ETag to the response
	w.Header().Add("ETag", eTag)
	w.Header().Add("Last-Modified", newestBlobTime.UTC().Format(http.TimeFormat))
	w.Header().Add("Cache-Control", "private, must-revalidate")
	w.Header().Add("Vary", "Accept, Authentication")

	skipped := http.StatusNotModified
	if r.Method == http.MethodPost {
		skipped = http.StatusPreconditionFailed
	}
	// Check the 'If-None-Match' request header
	if ifNoneMatch := r.Header.Get("If-None-Match"); ifNoneMatch != "" {
		for _, matchEtag := range strings.Split(ifNoneMatch, ",") {
			matchEtag = strings.TrimSpace(matchEtag)
			if eTag == matchEtag {
				w.WriteHeader(skipped)
				return
			}
		}
	} else if ifModifiedSince := r.Header.Get("If-Modified-Since"); ifModifiedSince != "" {
		ifModifiedSinceTime, err := time.Parse(http.TimeFormat, ifModifiedSince)
		if err != nil {
			w.Header().Add("Content-Type", "text/plain")
			w.WriteHeader(http.StatusBadRequest)
			if r.Method != http.MethodHead {
				fmt.Fprintf(w, "bad timestamp in If-Modified-Since: %s\n", err)
			}
			return
		}
		if !newestBlobTime.After(ifModifiedSinceTime) {
			w.WriteHeader(skipped)
			return
		}
	}

	w.Header().Add("Content-Type", acceptHeader)
	if isHeadRequest {
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
		s.logger.Printf("tenant %s query ID %s %q execution failed (do): %v", tenantID, queryID, redacted, err)
		return
	}
	go func() {
		<-r.Context().Done()
		rc.Close()
	}()
	s.logger.Printf("tenant %s query ID %s plan transfer took %s", tenantID, queryID, time.Since(startrun))
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
			s.logger.Printf("tenant %s query ID %s canceled after %s", tenantID, queryID, time.Since(startrun))
			return
		}
		s.logger.Printf("tenant %s query ID %s %q execution failed (check): %v", tenantID, queryID, redacted, err)
		if deadlined && isTimeout(err) {
			s.logger.Printf("tenant %s query ID %s killing tenant worker %s due to timeout", tenantID, queryID, id)
			s.manager.Quit(id, key)
		}
		return
	}
	elapsed := time.Since(startrun)
	if sendTrailer {
		setTiming(w, elapsed, &stats)
	}
	switch encodingFormat {
	case tnproto.OutputChunkedIon:
		writeStatusIon(w, &stats, tree.Results, tree.ResultTypes)
	case tnproto.OutputChunkedJSON:
		if statsOptIn {
			writeStatusJSON(w, &stats, tree.Results, tree.ResultTypes)
		}
	}
	s.logger.Printf("tenant %s query ID %s duration %s bytes %d hits %d misses %d",
		tenantID, queryID, elapsed, stats.BytesScanned, stats.CacheHits, stats.CacheMisses)
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
	var emptyLimit *errPlanLimit
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
	if errors.As(err, &emptyLimit) {
		w.WriteHeader(http.StatusBadRequest)
		io.WriteString(w, emptyLimit.Error())
		io.WriteString(w, "\n")
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

func writeStatusIon(w http.ResponseWriter, stats *plan.ExecStats, results []expr.Binding, types []expr.TypeSet) {
	var tmp ion.Buffer
	var st ion.Symtab
	resultsym := st.Intern("final_status")
	tmp.BeginAnnotation(1)
	tmp.BeginField(resultsym)

	tmp.BeginStruct(-1)

	// stats fields
	tmp.BeginField(st.Intern("hits"))
	tmp.WriteInt(stats.CacheHits)
	tmp.BeginField(st.Intern("misses"))
	tmp.WriteInt(stats.CacheMisses)
	tmp.BeginField(st.Intern("scanned"))
	tmp.WriteInt(stats.BytesScanned)

	// result set fields
	tmp.BeginField(st.Intern("result_set"))
	tmp.BeginStruct(-1)
	for i, bound := range results {
		tmp.BeginField(st.Intern(bound.Result()))
		tmp.WriteUint(uint64(types[i]))
	}
	tmp.EndStruct()

	tmp.EndStruct()
	tmp.EndAnnotation()

	split := tmp.Size()
	st.Marshal(&tmp, true)
	w.Write(tmp.Bytes()[split:])
	w.Write(tmp.Bytes()[:split])
}

func writeStatusJSON(w http.ResponseWriter, stats *plan.ExecStats, results []expr.Binding, types []expr.TypeSet) {
	result := map[string]any{
		"$sneller_final_status$": map[string]any{
			"hits":    stats.CacheHits,
			"misses":  stats.CacheMisses,
			"scanned": stats.BytesScanned,
		},
	}

	if len(results) > 0 && len(types) > 0 {
		resultSet := make(map[string]string)
		for i, bound := range results {
			resultSet[bound.Result()] = types[i].String()
		}
		result["result_set"] = resultSet
	}

	json.NewEncoder(w).Encode(&result)
}
