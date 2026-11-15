// Decoding /api/v1/query and /api/v1/query_range results into rows.
//
// Every result type becomes long-format rows (one per series per timestamp):
//
//   - vector  -> one row per series (instant query)
//   - matrix  -> one row per series per point (range query, or an instant
//                query of a range selector such as up[5m], which returns raw
//                samples)
//   - scalar  -> one row with empty labels
//   - string  -> one row with string_value set and value NULL
//
// Float values arrive as strings: "NaN", "+Inf" and "-Inf" become the IEEE
// values. Native-histogram points carry a `histogram` object instead of a
// value; they become a typed STRUCT (count, sum, buckets[]) with value NULL.
// Staleness markers never reach the HTTP API (the engine drops them; a series
// that went stale is simply absent from later steps), so there is nothing to
// decode for them.

package prometheus

import (
	"bytes"
	"encoding/json"
	"fmt"
	"math"
	"sort"
	"strconv"
	"strings"
)

// bucket is one native-histogram bucket.
type bucket struct {
	Lower, Upper   float64
	Count          float64
	LowerInclusive bool
	UpperInclusive bool
}

// histogram is one native-histogram sample.
type histogram struct {
	Count, Sum float64
	Buckets    []bucket
}

// row is one output sample.
type row struct {
	ResultType string
	Labels     map[string]string
	TsMs       int64
	Value      float64
	HasValue   bool
	Hist       *histogram
	Str        *string
}

// label returns the label value, ok=false when absent (SQL NULL).
func (r *row) label(name string) (string, bool) {
	v, ok := r.Labels[name]
	if !ok || v == "" {
		return "", false
	}
	return v, true
}

// parseTimestamp converts the API's float seconds (e.g. 1767225600.015) to
// Unix milliseconds without float rounding.
func parseTimestamp(n json.Number) (int64, error) {
	s := n.String()
	if strings.ContainsAny(s, "eE") {
		f, err := n.Float64()
		if err != nil {
			return 0, err
		}
		return int64(math.Round(f * 1000)), nil
	}
	neg := strings.HasPrefix(s, "-")
	s = strings.TrimPrefix(s, "-")
	whole, frac, _ := strings.Cut(s, ".")
	sec, err := strconv.ParseInt(whole, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad timestamp %q", n)
	}
	frac = (frac + "000")[:3]
	ms, err := strconv.ParseInt(frac, 10, 64)
	if err != nil {
		return 0, fmt.Errorf("bad timestamp %q", n)
	}
	v := sec*1000 + ms
	if neg {
		v = -v
	}
	return v, nil
}

// parseValue converts the API's string sample value, including NaN/±Inf.
func parseValue(s string) (float64, error) {
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("bad sample value %q", s)
	}
	return v, nil
}

// decodePoint decodes a [ts, "value"] pair.
func decodePoint(raw json.RawMessage) (int64, string, error) {
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return 0, "", fmt.Errorf("bad sample %s", truncate(string(raw)))
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(pair[0]))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, "", fmt.Errorf("bad sample timestamp %s", truncate(string(pair[0])))
	}
	ts, err := parseTimestamp(n)
	if err != nil {
		return 0, "", err
	}
	var v string
	if err := json.Unmarshal(pair[1], &v); err != nil {
		return 0, "", fmt.Errorf("bad sample value %s", truncate(string(pair[1])))
	}
	return ts, v, nil
}

type rawHistogram struct {
	Count   string              `json:"count"`
	Sum     string              `json:"sum"`
	Buckets [][]json.RawMessage `json:"buckets"`
}

// decodeHistogramPoint decodes a [ts, {count, sum, buckets}] pair.
func decodeHistogramPoint(raw json.RawMessage) (int64, *histogram, error) {
	var pair []json.RawMessage
	if err := json.Unmarshal(raw, &pair); err != nil || len(pair) != 2 {
		return 0, nil, fmt.Errorf("bad histogram sample %s", truncate(string(raw)))
	}
	var n json.Number
	dec := json.NewDecoder(bytes.NewReader(pair[0]))
	dec.UseNumber()
	if err := dec.Decode(&n); err != nil {
		return 0, nil, fmt.Errorf("bad histogram timestamp")
	}
	ts, err := parseTimestamp(n)
	if err != nil {
		return 0, nil, err
	}
	var rh rawHistogram
	if err := json.Unmarshal(pair[1], &rh); err != nil {
		return 0, nil, fmt.Errorf("bad histogram %s", truncate(string(pair[1])))
	}
	h := &histogram{}
	if h.Count, err = parseValue(rh.Count); err != nil {
		return 0, nil, err
	}
	if h.Sum, err = parseValue(rh.Sum); err != nil {
		return 0, nil, err
	}
	for _, b := range rh.Buckets {
		if len(b) != 4 {
			return 0, nil, fmt.Errorf("bad histogram bucket")
		}
		var rule int
		var lo, hi, cnt string
		if json.Unmarshal(b[0], &rule) != nil || json.Unmarshal(b[1], &lo) != nil ||
			json.Unmarshal(b[2], &hi) != nil || json.Unmarshal(b[3], &cnt) != nil {
			return 0, nil, fmt.Errorf("bad histogram bucket")
		}
		var bk bucket
		if bk.Lower, err = parseValue(lo); err != nil {
			return 0, nil, err
		}
		if bk.Upper, err = parseValue(hi); err != nil {
			return 0, nil, err
		}
		if bk.Count, err = parseValue(cnt); err != nil {
			return 0, nil, err
		}
		// Boundary rule: 0 = (lo, hi], 1 = [lo, hi), 2 = (lo, hi), 3 = [lo, hi].
		bk.LowerInclusive = rule == 1 || rule == 3
		bk.UpperInclusive = rule == 0 || rule == 3
		h.Buckets = append(h.Buckets, bk)
	}
	return ts, h, nil
}

type rawSeries struct {
	Metric     map[string]string `json:"metric"`
	Value      json.RawMessage   `json:"value"`
	Values     []json.RawMessage `json:"values"`
	Histogram  json.RawMessage   `json:"histogram"`
	Histograms []json.RawMessage `json:"histograms"`
}

type rawResult struct {
	ResultType string          `json:"resultType"`
	Result     json.RawMessage `json:"result"`
}

// decodeResult turns a query/query_range data object into rows.
func decodeResult(data json.RawMessage) ([]row, error) {
	var rr rawResult
	if err := json.Unmarshal(data, &rr); err != nil {
		return nil, fmt.Errorf("decoding query result: %w", err)
	}
	switch rr.ResultType {
	case "scalar", "string":
		ts, v, err := decodePoint(rr.Result)
		if err != nil {
			return nil, err
		}
		r := row{ResultType: rr.ResultType, Labels: map[string]string{}, TsMs: ts}
		if rr.ResultType == "string" {
			r.Str = &v
		} else {
			if r.Value, err = parseValue(v); err != nil {
				return nil, err
			}
			r.HasValue = true
		}
		return []row{r}, nil
	case "vector", "matrix":
	default:
		return nil, fmt.Errorf("unsupported result type %q", rr.ResultType)
	}
	var series []rawSeries
	if err := json.Unmarshal(rr.Result, &series); err != nil {
		return nil, fmt.Errorf("decoding %s result: %w", rr.ResultType, err)
	}
	var out []row
	for _, s := range series {
		labels := s.Metric
		if labels == nil {
			labels = map[string]string{}
		}
		add := func(raw json.RawMessage) error {
			ts, v, err := decodePoint(raw)
			if err != nil {
				return err
			}
			f, err := parseValue(v)
			if err != nil {
				return err
			}
			out = append(out, row{ResultType: rr.ResultType, Labels: labels, TsMs: ts, Value: f, HasValue: true})
			return nil
		}
		addHist := func(raw json.RawMessage) error {
			ts, h, err := decodeHistogramPoint(raw)
			if err != nil {
				return err
			}
			out = append(out, row{ResultType: rr.ResultType, Labels: labels, TsMs: ts, Hist: h})
			return nil
		}
		if len(s.Value) > 0 && string(s.Value) != "null" {
			if err := add(s.Value); err != nil {
				return nil, err
			}
		}
		if len(s.Histogram) > 0 && string(s.Histogram) != "null" {
			if err := addHist(s.Histogram); err != nil {
				return nil, err
			}
		}
		// A series can mix float and histogram points; merge them in time order.
		start := len(out)
		for _, p := range s.Values {
			if err := add(p); err != nil {
				return nil, err
			}
		}
		for _, p := range s.Histograms {
			if err := addHist(p); err != nil {
				return nil, err
			}
		}
		if len(s.Values) > 0 && len(s.Histograms) > 0 {
			seg := out[start:]
			sort.SliceStable(seg, func(i, j int) bool { return seg[i].TsMs < seg[j].TsMs })
		}
	}
	return out, nil
}

func truncate(s string) string {
	if len(s) > 120 {
		return s[:120] + "..."
	}
	return s
}
