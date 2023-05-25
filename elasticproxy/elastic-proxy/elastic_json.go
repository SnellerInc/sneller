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

package elastic_proxy

import (
	"crypto/sha256"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"hash"
	"path"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"time"
)

const (
	TotalCountBucket  = "$total_count"
	HitsBucket        = "$hits"
	KeyPrefix         = "$key"
	BucketPrefix      = "$bucket"
	DocCount          = "$doc_count"
	DefaultSource     = "$source"
	SourceAliasPrefix = "$source:"
)

type ElasticJSON struct {
	From           *int                   `json:"from"`
	Size           *int                   `json:"size"`
	Aggregations   map[string]aggregation `json:"aggs"`
	Sort           []SortField            `json:"sort"`
	Query          *Query                 `json:"query"`
	Version        *bool                  `json:"version"` // indicates it the version should be included in the hit
	Source         *source                `json:"_source"` // indicates if source record should be included in the hit
	Fields         []projectedField       `json:"fields"`
	TrackTotalHits *TrackTotalHits        `json:"track_total_hits"`
}

type source struct {
	All      *bool
	Includes []string
	Excludes []string
}

func (es *source) UnmarshalJSON(data []byte) error {
	var all bool
	if err := json.Unmarshal(data, &all); err == nil {
		es.All = &all
		return nil
	}

	var fields string
	if err := json.Unmarshal(data, &fields); err == nil {
		es.Includes = strings.Split(fields, ",")
		es.Excludes = []string{}
		return nil
	}

	var _source struct {
		Includes []string `json:"includes,omitempty"`
		Excludes []string `json:"excludes,omitempty"`
	}
	if err := json.Unmarshal(data, &_source); err == nil {
		es.Includes = _source.Includes
		es.Excludes = _source.Excludes
		return nil
	}
	return fmt.Errorf("unknown value %q for _source", string(data))
}

type SortField struct {
	Field  string
	Format string
	Order  Ordering
}

type sortFieldInner struct {
	Format string   `json:"format,omitempty"`
	Order  Ordering `json:"order"`
}

func (sf *SortField) UnmarshalJSON(data []byte) error {
	var vv map[string]sortFieldInner
	if err := json.Unmarshal(data, &vv); err != nil {
		return err
	}
	if len(vv) == 0 {
		return errors.New("sort-field without field-name")
	}
	if len(vv) > 1 {
		return errors.New("sort-field should only contain a single value")
	}
	for k, v := range vv {
		sf.Field = k
		sf.Format = v.Format
		sf.Order = v.Order
	}
	if sf.Order == "" {
		sf.Order = OrderDescending
	}
	return nil
}

func (sf *SortField) MarshalJSON() ([]byte, error) {
	vv := make(map[string]sortFieldInner, 1)
	vv[sf.Field] = sortFieldInner{
		Format: sf.Format,
		Order:  sf.Order,
	}
	return json.Marshal(vv)
}

type projectedField struct {
	Field  string
	Format string
}

func (sf *projectedField) UnmarshalJSON(data []byte) error {
	type _projectedField projectedField
	if err := json.Unmarshal(data, (*_projectedField)(sf)); err != nil {
		var fieldName string
		if err := json.Unmarshal(data, &fieldName); err != nil {
			return err
		}
		sf.Field = fieldName
	}
	return nil
}

type TrackTotalHits struct {
	Enabled bool  // set if hits need to be returned
	Limit   int64 // max nr. of items (-1: exact count)
}

var DefaultTrackTotalHits = TrackTotalHits{
	Enabled: true,
	Limit:   10000,
}

func (sf *TrackTotalHits) UnmarshalJSON(data []byte) error {
	if err := json.Unmarshal(data, &sf.Limit); err == nil {
		if sf.Limit <= 0 {
			return fmt.Errorf("invalid track_total_limits value %d", sf.Limit)
		}
		return nil
	}

	if err := json.Unmarshal(data, &sf.Enabled); err == nil {
		if sf.Enabled {
			sf.Limit = -1 // no limit, so get exact value
		}
		return nil
	}

	return fmt.Errorf("invalid track_total_hits %q", string(data))
}

type ElasticResult struct {
	TimedOut     bool                `json:"timed_out"`
	Hits         *elasticResultHits  `json:"hits"`
	Shards       elasticResultShards `json:"_shards"`
	Count        *int64              `json:"count,omitempty"` // only for Counting API
	Took         int                 `json:"took"`
	Aggregations *map[string]any     `json:"aggregations,omitempty"`
}

type elasticResultHits struct {
	Hits     []elasticResultHitRecord `json:"hits"`
	Total    *elasticResultHitsTotal  `json:"total,omitempty"`
	MaxScore *float64                 `json:"max_score"`
}

type elasticResultHitRecord struct {
	Score   *float64         `json:"_score"`
	Type    string           `json:"_type"`
	Id      string           `json:"_id"`
	Source  any              `json:"_source,omitempty"`
	Fields  map[string][]any `json:"fields,omitempty"`
	Version *int             `json:"_version,omitempty"`
	Index   string           `json:"_index,omitempty"`
	Sort    []any            `json:"sort,omitempty"`
}

type elasticResultHitsTotal struct {
	Relation string `json:"relation"`
	Value    int64  `json:"value"`
}

type elasticResultShards struct {
	Successful int `json:"successful"`
	Failed     int `json:"failed"`
	Skipped    int `json:"skipped"`
	Total      int `json:"total"`
}

// metricResult holds the result of a
// metric aggregation.
type metricResult struct {
	Value any `json:"value"`
}

// bucketSingleResult holds the result of
// a bucket aggregation that returns always
// a single buckt.
type bucketSingleResult struct {
	SubAggregations map[string]any
	DocCount        int64
}

func (r *bucketSingleResult) MarshalJSON() ([]byte, error) {
	jsonMap := make(map[string]any, len(r.SubAggregations)+1)
	for k, v := range r.SubAggregations {
		jsonMap[k] = v
	}
	jsonMap["doc_count"] = r.DocCount
	return json.Marshal(jsonMap)
}

// bucketMultiResult holds the result of
// a bucket aggregation that can return
// zero or more buckets. Each bucket is
// identified by a unique key.
type bucketMultiResult struct {
	Buckets                 []bucketSingleResultWithKey `json:"buckets"`
	SumOtherDocCount        *int64                      `json:"sum_other_doc_count,omitempty"`
	DocCountErrorUpperBound *int64                      `json:"doc_count_error_upper_bound,omitempty"`
}

// bucketMappedResult holds the result of
// a bucket aggregation where the buckets
// are returned as a hash-map.
type bucketMappedResult struct {
	Buckets map[string]*bucketSingleResult `json:"buckets"`
}

// bucketSingleResultWithKey is just like
// a normal `bucketSingleResult`, but is
// annotated with a key.
type bucketSingleResultWithKey struct {
	bucketSingleResult
	Key       any
	KeyFormat string
	KeyField  string
	Context   *QueryContext
}

func (r *bucketSingleResultWithKey) MarshalJSON() ([]byte, error) {
	jsonMap := make(map[string]any, len(r.SubAggregations)+3)
	for k, v := range r.SubAggregations {
		jsonMap[k] = v
	}
	jsonMap["doc_count"] = r.DocCount

	switch key := r.Key.(type) {
	case bool:
		// booleans are emitted as 0/1
		if key {
			jsonMap["key"] = 1
		} else {
			jsonMap["key"] = 0
		}
	case time.Time:
		// timestamps are always emitted as numerical values
		// when emitted as a key
		jsonMap["key"] = key.UnixMilli()
	default:
		jsonMap["key"] = key
	}

	f := r.KeyFormat
	if f == "" {
		f, _ = format(r.KeyField, r.Context.TypeMapping)
	}

	text := ""
	if f != "" {
		formatValue, err := formatOutRaw(r.Key, f)
		if err != nil {
			return nil, err
		}
		if t, ok := formatValue.(string); ok {
			text = t
		}
	} else {
		// Perform the default mapping
		switch key := r.Key.(type) {
		case bool:
			if key {
				text = "true"
			} else {
				text = "false"
			}
		case time.Time:
			// time is emitted using RFC format
			text = key.Format(time.RFC3339Nano)
		}
	}

	if text != "" {
		jsonMap["key_as_string"] = text
	}

	return json.Marshal(jsonMap)
}

func (ej *ElasticJSON) SQL(qc *QueryContext) (*exprSelect, error) {
	projectExprs := []projectAliasExpr{}
	effectiveSize := 10
	if ej.Size != nil {
		effectiveSize = *ej.Size
	}

	var qExpr expression
	if ej.Query != nil {
		var err error
		qExpr, err = ej.Query.Expression(qc)
		if err != nil {
			return nil, err
		}
	}

	from := projectAliasExpr{
		Context: qc,
		expression: &exprSources{
			Context: qc,
			Sources: qc.TableSources,
		},
		Alias: DefaultSource,
	}

	source := projectAliasExpr{
		Context: qc,
		Alias:   DefaultSource,
		expression: &exprSelect{
			Context: qc,
			Projection: []projectAliasExpr{
				{
					Context:    qc,
					expression: &exprFieldName{Context: qc},
				},
			},
			From:  append([]expression{&from}, qc.Sources...),
			Where: qExpr,
		},
	}

	fromSources := []expression{ParseExprSourceName(qc, source.Alias)}

	var exprTotalHits expression
	tth := ej.TrackTotalHits
	if tth == nil {
		tth = &DefaultTrackTotalHits
	}
	if !qc.IgnoreTotalHits {
		exprTotalHitsSelect := &exprSelect{
			Projection: []projectAliasExpr{
				{
					Context: qc,
					expression: &exprFunction{
						Context: qc,
						Name:    "COUNT",
						Exprs:   []expression{&exprFieldName{Context: qc}},
					},
				},
			},
		}
		if tth.Limit >= 0 {
			if qc.IgnoreSumOtherDocCount {
				exprTotalHitsSelect.From = []expression{
					&exprSelect{
						Projection: []projectAliasExpr{
							{
								Context:    qc,
								expression: &exprFieldName{Context: qc},
							},
						},
						From:  fromSources,
						Limit: int(tth.Limit + 1),
					},
				}
			} else {
				exprTotalHitsSelect.From = fromSources
			}
		} else {
			exprTotalHitsSelect.From = fromSources
		}
		exprTotalHits = exprTotalHitsSelect
	} else {
		exprTotalHits = &exprJSONLiteral{Value: JSONLiteral{Value: float64(-1)}}
	}

	projectExprs = append(projectExprs, projectAliasExpr{
		Context:    qc,
		Alias:      TotalCountBucket,
		expression: exprTotalHits,
	})

	if effectiveSize > 0 {
		effectiveOffset := 0
		if ej.From != nil {
			effectiveOffset = *ej.From
		}

		var orderBy []orderByExpr
		if ej.Sort != nil {
			for _, proj := range ej.Sort {
				orderBy = append(orderBy, orderByExpr{
					Context:    qc,
					expression: ParseExprFieldName(qc, proj.Field),
					Order:      proj.Order,
				})
			}
		}
		projectExprs = append(projectExprs, projectAliasExpr{
			Alias: HitsBucket,
			expression: &exprSelect{
				Context:    qc,
				Projection: []projectAliasExpr{{Context: qc, expression: &exprFieldName{Context: qc}}},
				From:       fromSources,
				Offset:     effectiveOffset,
				Limit:      effectiveSize,
				OrderBy:    orderBy,
			},
		})
	}

	qc.Sources = fromSources
	c := aggsGenerateContext{
		context:             qc,
		currentAggregations: ej.Aggregations,
		groupExprs:          []projectAliasExpr{},
	}

	aggProjectExprs, err := c.transform()
	if err != nil {
		return nil, err
	}

	withExprs := []projectAliasExpr{source}
	for _, aggProjectExpr := range aggProjectExprs {
		withExprs = append(withExprs, aggProjectExpr)

		projectExprs = append(projectExprs, projectAliasExpr{
			Context: qc,
			Alias:   aggProjectExpr.Alias,
			expression: &exprSelect{
				Context:    qc,
				Projection: []projectAliasExpr{{Context: qc, expression: &exprFieldName{Context: qc}}},
				From:       []expression{ParseExprSourceName(qc, aggProjectExpr.Alias)},
			},
		})
	}

	return &exprSelect{
		Context:    qc,
		With:       withExprs,
		Projection: projectExprs,
	}, nil
}

func (ej *ElasticJSON) ConvertResult(qc *QueryContext, snellerResult map[string]any) (*ElasticResult, map[string]any, error) {
	totalCountBucket := snellerResult[TotalCountBucket]
	totalCount := int64(totalCountBucket.(int))

	er := ElasticResult{
		TimedOut: false,
		Hits: &elasticResultHits{
			Hits: []elasticResultHitRecord{},
		},
		Shards: elasticResultShards{
			Successful: 1,
			Total:      1,
		},
	}

	tth := ej.TrackTotalHits
	if tth == nil {
		tth = &DefaultTrackTotalHits
	}
	if tth.Enabled {
		var total elasticResultHitsTotal
		if tth.Limit >= 0 && totalCount > tth.Limit {
			total.Relation = "gte"
			total.Value = tth.Limit
		} else if tth.Limit < 0 || totalCount >= 0 {
			total.Relation = "eq"
			total.Value = totalCount
		}
		er.Hits.Total = &total
	}

	// process normal hits
	if hits, ok := snellerResult[HitsBucket]; ok {
		var hitsRecords []any
		if singleHitsRecord, ok := hits.(map[string]any); ok {
			if len(singleHitsRecord) > 0 {
				hits = []any{singleHitsRecord}
			} else {
				hits = []any{}
			}
		}
		hitsRecords, ok := hits.([]any)
		if !ok {
			return nil, nil, fmt.Errorf("%q should contain an array of records", HitsBucket)
		}

		defaultScore := float64(1)
		score := &defaultScore

		var version *int
		if ej.Version != nil && *ej.Version {
			defaultVersion := int(1)
			version = &defaultVersion
		}

		for _, hitRecord := range hitsRecords {
			hit, ok := hitRecord.(map[string]any)
			if !ok {
				return nil, nil, fmt.Errorf("%q should contain an array of records", HitsBucket)
			}

			// Strip out excessive columns due to
			// https://github.com/SnellerInc/sneller-core/issues/2358#issuecomment-1406379279
			for k := range hit {
				if strings.HasPrefix(k, SourceAliasPrefix) {
					delete(hit, k)
				}
			}

			sortValues := make([]any, 0, len(ej.Sort))
			for _, k := range ej.Sort {
				value := hit[k.Field]
				// timestamp are written as unix-milli in sort orders
				if t, ok := value.(time.Time); ok {
					value = t.UnixMilli()
				}
				sortValues = append(sortValues, value)
			}

			for k := range hit {
				var err error
				hit[k], err = formatOut(k, hit[k], qc.TypeMapping)
				if err != nil {
					return nil, nil, err
				}
			}

			rec := elasticResultHitRecord{
				Score:   score,
				Type:    "_doc",
				Id:      hashItem(hit), // should generate unique and reproducible ids
				Version: version,
				Index:   qc.Index,
				Sort:    sortValues,
			}
			if len(ej.Fields) > 0 {
				rec.Fields = make(map[string][]any)
				for _, f := range ej.Fields {
					keys, values := findValues(hit, f.Field)
					for i := range keys {
						value, err := formatOut(keys[i], values[i], qc.TypeMapping)
						if err != nil {
							value = values[i]
						}
						rec.Fields[keys[i]] = []any{value}
					}
				}
			}
			if ej.Source == nil || (ej.Source.All != nil && *ej.Source.All) || (ej.Source.All == nil && len(ej.Source.Includes) == 0 && len(ej.Source.Excludes) == 0) {
				rec.Source = hit
			} else if len(ej.Source.Includes) > 0 || len(ej.Source.Excludes) > 0 {
				source := make(map[string]any)
				for f, v := range hit {
					match := false
					if len(ej.Source.Includes) == 0 {
						match = true
					} else {
						for _, ff := range ej.Source.Includes {
							if matchWildcard(f, ff) {
								match = true
								break
							}
						}
					}
					if match {
						for _, ff := range ej.Source.Excludes {
							if matchWildcard(f, ff) {
								match = false
								break
							}
						}
						if match {
							source[f] = v
						}
					}
				}
				rec.Source = source
			}

			er.Hits.Hits = append(er.Hits.Hits, rec)
			if er.Hits.MaxScore == nil || *er.Hits.MaxScore < *score {
				er.Hits.MaxScore = score
			}
		}
	}

	// process aggregations
	var preProcessedData map[string]any
	if len(ej.Aggregations) > 0 {
		var err error
		preProcessedData, err = preProcess(snellerResult)
		if err != nil {
			return nil, nil, err
		}

		aggregations := make(map[string]any)
		er.Aggregations = &aggregations

		for name, agg := range ej.Aggregations {
			c := aggsProcessContext{
				context:  qc,
				bucket:   name,
				agg:      agg,
				data:     preProcessedData[name],
				docCount: totalCount,
			}

			switch a := agg.Aggregation.(type) {
			case metricAggregation:
				aggregations[name], err = a.process(&c)
				if err != nil {
					return nil, preProcessedData, err
				}

			case bucketAggregation:
				aggregations[name], err = a.process(&c)
				if err != nil {
					return nil, preProcessedData, err
				}

			default:
				return nil, preProcessedData, errors.New("unknown aggregation")
			}

			if agg.Meta != nil {
				aggregations["meta"] = agg.Meta
			}

		}

		// process pipeline aggregations
		err = processPipelineAggregations(ej.Aggregations, aggregations)
		if err != nil {
			return nil, preProcessedData, err
		}
	}

	return &er, preProcessedData, nil
}

func processPipelineAggregations(aggs map[string]aggregation, data any) error {
	orderedAggs := orderedAggs(aggs)
	for _, subAggName := range orderedAggs {
		subAgg := aggs[subAggName]

		switch sa := subAgg.Aggregation.(type) {
		case pipelineAggregation:
			err := sa.process(subAggName, data)
			if err != nil {
				return err
			}

			err = processPipelineAggregations(subAgg.SubAggregations, data)
			if err != nil {
				return err
			}

		case bucketAggregation:
			var subData any
			switch dt := data.(type) {
			case map[string]any:
				subData = dt[subAggName]
			case *bucketSingleResult:
				subData = dt
			case *bucketMultiResult:
				subData = dt
			case *bucketMappedResult:
				subData = dt
			default:
				panic("unknown data-type")
			}

			err := processPipelineAggregations(subAgg.SubAggregations, subData)
			if err != nil {
				return err
			}

		case metricAggregation:
			// skip

		default:
			panic("unknown aggregation")
		}
	}

	return nil
}

func orderedAggs(aggs map[string]aggregation) []string {
	// we should determine the proper ordering of the aggregations
	// see https://github.com/SnellerInc/elastic-proxy/issues/28
	//
	// 1. return all bucket aggregations (sorted order)
	// 2. return all pipeline aggregations in following order
	//    - bucket_script
	//    - others
	//    - bucket_order
	subAggNames := sortedKeys(aggs)
	var aggNames []string
	addAgg := func(action func(any) bool) {
		for _, subAggName := range subAggNames {
			if action(aggs[subAggName].Aggregation) {
				aggNames = append(aggNames, subAggName)
			}
		}
	}

	// all bucket aggregations
	addAgg(func(agg any) bool {
		_, ok := agg.(bucketAggregation)
		return ok
	})

	// all bucket_script aggregations
	addAgg(func(agg any) bool {
		_, ok := agg.(*aggsBucketScript)
		return ok
	})

	// all non-bucket_script, non-bucket_order pipeline aggregations
	addAgg(func(agg any) bool {
		switch agg.(type) {
		case *aggsBucketScript, *aggsBucketSort:
			return false
		case pipelineAggregation:
			return true
		default:
			return false
		}
	})

	// all bucket_script aggregations
	addAgg(func(agg any) bool {
		_, ok := agg.(*aggsBucketSort)
		return ok
	})

	return aggNames
}

func hashAny(h hash.Hash, v any) {
	if v == nil {
		return
	}
	switch v := v.(type) {
	case map[string]any:
		// map sort-order is not defined, so use
		// a sorted order to guarantee consistent
		// results
		for _, k := range sortedKeys(v) {
			h.Write([]byte(k))
			hashAny(h, v[k])
		}

	default:
		json, err := json.Marshal(v)
		if err != nil {
			panic(fmt.Sprintf("unable to hash value: %v", v))
		}
		h.Write(json)
	}
}

func hashItem(v any) string {
	h := sha256.New()
	hashAny(h, v)
	return base64.RawURLEncoding.EncodeToString(h.Sum(nil))
}

type groupResultMap struct {
	keyColumns    []string                 // column names of the key columns
	OrderedGroups []*groupResults          `json:"$groups$,omitempty"` // results per group (for proper order)
	groups        map[string]*groupResults // grouped columns mapped by hash (for fast access)
}

type groupResults struct {
	KeyValues []any          `json:"$keys$,omitempty"`    // key values of this group (same length as parent keyColumns)
	Results   map[string]any `json:"$results$,omitempty"` // results
	Nested    map[string]any `json:"$nested$,omitempty"`  // nested aggregations
}

func (c *groupResults) docCount() (int64, error) {
	v, ok := c.Results[DocCount]
	if !ok {
		return 0, nil
	}
	switch v := v.(type) {
	case float64:
		return int64(v), nil
	case int:
		return int64(v), nil
	case int64:
		return v, nil
	}
	return 0, fmt.Errorf("%s has invalid type", DocCount)
}

// preProcess obtains all the aggregated query
// results and combines the "flag" SQL output to
// a hierarchical structure that matches the
// group-by.
//
// it also combines the split SQL results that
// are caused, because some aggragations cannot
// be combined.
func preProcess(snellerResult map[string]any) (map[string]any, error) {
	preProcessed := make(map[string]any)

	preProcessed[DocCount] = snellerResult[TotalCountBucket]

	// make sure the buckets are processed in fixed order
	// sorting the buckets makes sure that the outer aggregations
	// are processed before the inner aggregations
	for _, combinedBucketName := range sortedKeys(snellerResult) {
		bucket := snellerResult[combinedBucketName]

		// only process buckets
		bucketName, bucketIndex := splitWithPrefix(BucketPrefix, combinedBucketName)
		if bucketIndex < 0 {
			continue
		}

		bucketNameParts := strings.Split(bucketName, ":")
		rootBucketName := bucketNameParts[0]

		// process all rows in the bucket
		var rows []any
		switch b := bucket.(type) {
		case []any:
			// the bucket contains rows of data, so it is the
			// result of a group-by aggregation
			rows = b

			// skip processing if there is no data
			if len(rows) == 0 {
				continue
			}

			// determine the list of key-groups based on the
			// columns in the first record
			firstRow, ok := rows[0].(map[string]any)
			if !ok {
				return nil, fmt.Errorf("bucket %q should hold a record in the first element", combinedBucketName)
			}

			keyGroups, err := keyGroups(firstRow)
			if err != nil {
				return nil, err
			}

			rootGrm, ok := preProcessed[rootBucketName].(*groupResultMap)
			if !ok {
				rootGrm = &groupResultMap{}
				preProcessed[rootBucketName] = rootGrm
			}

			// create a hierarchical structure based on the key-groups
			for _, item := range rows {
				row := item.(map[string]any)
				grm := rootGrm

				var group *groupResults
				bucketPartIndex := 0
				for _, kg := range keyGroups {
					if bucketPartIndex > 0 {
						if group.Nested == nil {
							group.Nested = make(map[string]any)
						}
						grm, ok = group.Nested[bucketNameParts[bucketPartIndex]].(*groupResultMap)
						if !ok {
							grm = &groupResultMap{}
							group.Nested[bucketNameParts[bucketPartIndex]] = grm
						}
					}

					if len(grm.keyColumns) == 0 {
						grm.groups = make(map[string]*groupResults)
						grm.keyColumns = kg
					}

					// hash the values of the key groups
					h := sha256.New()
					for _, col := range grm.keyColumns {
						hashAny(h, row[col])
					}
					keyHash := base64.RawURLEncoding.EncodeToString(h.Sum(nil))

					// check if this key is already in the
					// list and if not, then add it to the list
					group, ok = grm.groups[keyHash]
					if !ok {
						group = &groupResults{
							KeyValues: make([]any, 0, len(grm.keyColumns)),
						}
						for _, col := range grm.keyColumns {
							group.KeyValues = append(group.KeyValues, row[col])
						}

						grm.groups[keyHash] = group
						grm.OrderedGroups = append(grm.OrderedGroups, group)
					}

					bucketPartIndex++
				}

				for bucketPartIndex < len(bucketNameParts) {
					if group.Nested == nil {
						group.Nested = make(map[string]any)
					}
					nestedGroup, ok := group.Nested[bucketNameParts[bucketPartIndex]].(*groupResults)
					if !ok {
						nestedGroup = &groupResults{}
						group.Nested[bucketNameParts[bucketPartIndex]] = nestedGroup
					}
					group = nestedGroup
					bucketPartIndex++
				}

				if len(row) > 0 {
					if group.Results == nil {
						group.Results = make(map[string]any, len(row))
					}
					for col, v := range row {
						if col != DummyAlias {
							_, index := splitWithPrefix(KeyPrefix, col)
							if index < 0 {
								group.Results[col] = v
							}
						}
					}
				}
			}
		case map[string]any:
			// a single-object response is the result of an
			// aggregation without group-by, so it will be
			// only metric aggregation results
			var results map[string]any

			if len(bucketNameParts) > 1 {
				grm, ok := preProcessed[rootBucketName].(*groupResultMap)
				if !ok {
					grm = &groupResultMap{
						groups: make(map[string]*groupResults),
					}
					preProcessed[rootBucketName] = grm
				}
				group := &groupResults{
					KeyValues: []any{bucketNameParts[1]},
					Results:   make(map[string]any, 0),
				}
				grm.groups[bucketNameParts[1]] = group
				grm.OrderedGroups = append(grm.OrderedGroups, group)

				results = group.Results
			} else {
				var group *groupResults
				var ok bool
				if rootBucketName != "" {
					if group, ok = preProcessed[rootBucketName].(*groupResults); !ok {
						group = &groupResults{
							KeyValues: make([]any, 0),
							Results:   make(map[string]any, 0),
						}
						preProcessed[rootBucketName] = group
					}
					results = group.Results
				} else {
					// top-level metric aggregations
					results = preProcessed
				}
			}

			for col, v := range b {
				if col != DummyAlias {
					results[col] = v
				}
			}
		default:
			return nil, fmt.Errorf("bucket %q has unsupported result data", combinedBucketName)
		}
	}

	return preProcessed, nil
}

func splitWithPrefix(prefix string, text string) (string, int) {
	if !strings.HasPrefix(text, prefix+":") {
		return "", -1
	}

	indexOffset := strings.LastIndex(text, "%")
	name := text[len(prefix)+1 : indexOffset]
	index, err := strconv.Atoi(text[indexOffset+1:])
	if err != nil {
		return "", -1
	}

	return name, index
}

func keyGroups(record map[string]any) ([][]string, error) {
	keyColumnsGroups := make(map[string]([]string))

	// assign all key columns into the map
	for col := range record {
		// determine key/index from column
		keyName, keyIndex := splitWithPrefix(KeyPrefix, col)
		if keyIndex < 0 {
			continue
		}

		// check if the column-group is already in the map
		keyColumnsGroup, ok := keyColumnsGroups[keyName]
		if !ok {
			keyColumnsGroup = make([]string, 0)
		}

		// add the column to the group
		keyColumnsGroup = append(keyColumnsGroup, col)
		keyColumnsGroups[keyName] = keyColumnsGroup
	}

	// now sort all the columns for each group
	for group, columns := range keyColumnsGroups {
		sort.Strings(columns)
		for index, col := range columns {
			_, keyIndex := splitWithPrefix(KeyPrefix, col)
			if index != keyIndex {
				return nil, fmt.Errorf("invalid key-name %q", col)
			}
		}
		keyColumnsGroups[group] = columns
	}

	// return key groups in the proper order
	keyGroups := make([][]string, 0, len(keyColumnsGroups))
	for _, group := range sortedKeys(keyColumnsGroups) {
		keyGroups = append(keyGroups, keyColumnsGroups[group])
	}

	return keyGroups, nil
}

func matchWildcard(s, wildcard string) bool {
	if s == wildcard {
		return true
	}

	parts := strings.Split(wildcard, "*")
	if len(parts) == 1 {
		return false
	}

	var pat strings.Builder
	pat.WriteRune('^')
	for i, literal := range parts {
		if i > 0 {
			pat.WriteString(".*")
		}
		pat.WriteString(regexp.QuoteMeta(literal))
	}
	pat.WriteRune('$')
	result, _ := regexp.MatchString(pat.String(), s)
	return result
}

func findValues(m map[string]any, field string) (keys []string, values []any) {
	fieldParts := strings.Split(field, ".")
	depth := 0
	var keyParts []string
	var action func(map[string]any)
	action = func(m map[string]any) {
		for k := range m {
			match, err := path.Match(fieldParts[depth], k)
			if err == nil && match {
				keyParts = append(keyParts, k)
				if depth == len(fieldParts)-1 {
					// leaf matched, so process
					keys = append(keys, strings.Join(keyParts, "."))
					values = append(values, m[k])
				} else {
					if sm, ok := m[k].(map[string]any); ok {
						depth++
						action(sm)
						depth--
					}
				}
				keyParts = keyParts[:depth]
			}
		}
	}
	action(m)
	return
}
