package prometheus

import (
	"encoding/json"
	"math"
	"regexp"
	"testing"
)

func col(name string) *expr            { return &expr{K: "col", S: name} }
func strLit(s string) *expr            { return &expr{K: "str", S: s} }
func call(fn string, a ...*expr) *expr { return &expr{K: "call", S: fn, Args: a} }
func tsLit(us int64) *expr             { return &expr{K: "ts", I: us} }

func TestPlanPushdownSelector(t *testing.T) {
	cols := columnsByName(samplesColumns([]string{"env"}))
	preds := []*expr{
		call("=", col("job"), strLit("api")),
		{K: "or", Args: []*expr{call("=", col("instance"), strLit("a:1")), call("=", col("instance"), strLit("b.2"))}},
		call("isnull", col("env")),
		call(">=", col("ts"), tsLit(1_767_225_600_000_000)),
		call("<", col("ts"), tsLit(1_767_229_200_000_000)),
		call(">", col("value"), &expr{K: "num", F: 1}), // not a label: stays in the worker
	}
	plan := planPushdown(preds, cols)
	got := buildSelector("up", plan.Matchers)
	want := `up{job="api",instance=~"a:1|b\\.2",env=""}`
	if got != want {
		t.Fatalf("selector = %s, want %s", got, want)
	}
	if plan.Window.Lo == nil || *plan.Window.Lo != 1_767_225_600_000 {
		t.Fatalf("lo = %v", plan.Window.Lo)
	}
	// ts < exact second excludes that millisecond.
	if plan.Window.Hi == nil || *plan.Window.Hi != 1_767_229_199_999 {
		t.Fatalf("hi = %v", *plan.Window.Hi)
	}
}

func TestBuildSelectorMergesBraces(t *testing.T) {
	ms := []matcher{{"job", "=", "x"}}
	for in, want := range map[string]string{
		`up{instance="a"}`: `up{instance="a",job="x"}`,
		`{__name__="up"}`:  `{__name__="up",job="x"}`,
		`up{}`:             `up{job="x"}`,
	} {
		if got := buildSelector(in, ms); got != want {
			t.Errorf("buildSelector(%s) = %s, want %s", in, got, want)
		}
	}
}

func TestNotLikeIsSuperset(t *testing.T) {
	cols := columnsByName(samplesColumns(nil))
	m, ok := labelMatcher(call("!~~", col("instance"), strLit("node-a%")), cols)
	if !ok || m.Op != "!~" {
		t.Fatalf("matcher = %+v, %v", m, ok)
	}
	re := regexp.MustCompile("^(?:" + m.Value + ")$")
	if re.MatchString("node-b") || !re.MatchString("node-a:9100") {
		t.Fatalf("regex %s", m.Value)
	}
}

func TestPrefixRangeRoundTrip(t *testing.T) {
	cols := columnsByName(samplesColumns(nil))
	e := &expr{K: "and", Args: []*expr{call(">=", col("job"), strLit("fix")), call("<", col("job"), strLit("fiy"))}}
	m, ok := prefixRange(e, cols)
	if !ok || m.Value != "fix(?s:.*)" {
		t.Fatalf("prefixRange = %+v %v", m, ok)
	}
}

func TestChunkEndNeverExceedsLimit(t *testing.T) {
	start, end, step := int64(0), int64(14_400_000), int64(1000)
	var points, chunks int64
	for next := start; next <= end; {
		last := chunkEnd(next, end, step, 11000)
		n := (last-next)/step + 1
		if n > 11000 {
			t.Fatalf("chunk of %d points", n)
		}
		points += n
		chunks++
		next = last + step
	}
	if points != 14401 || chunks != 2 {
		t.Fatalf("points=%d chunks=%d", points, chunks)
	}
}

func TestTimestamps(t *testing.T) {
	for in, want := range map[string]int64{"1767225600": 1767225600000, "1767225600.123": 1767225600123, "1767225600.1": 1767225600100, "-1.5": -1500} {
		got, err := parseTimestamp(json.Number(in))
		if err != nil || got != want {
			t.Errorf("parseTimestamp(%s) = %d, %v", in, got, err)
		}
	}
	if formatTime(1767225600123) != "1767225600.123" || formatTime(1767225600000) != "1767225600" {
		t.Fatal("formatTime")
	}
}

func TestDecodeVectorSpecialValues(t *testing.T) {
	data := json.RawMessage(`{"resultType":"vector","result":[
		{"metric":{"__name__":"x","job":"j","kind":"nan"},"value":[1767225600,"NaN"]},
		{"metric":{"__name__":"x","kind":"inf"},"value":[1767225600.5,"+Inf"]}]}`)
	rows, err := decodeResult(data)
	if err != nil || len(rows) != 2 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	if !rows[0].HasValue || !math.IsNaN(rows[0].Value) {
		t.Fatalf("row0 %+v", rows[0])
	}
	if !rows[1].HasValue || !math.IsInf(rows[1].Value, 1) || rows[1].TsMs != 1767225600500 {
		t.Fatalf("row1 %+v", rows[1])
	}
}

func TestDecodeNativeHistogram(t *testing.T) {
	data := json.RawMessage(`{"resultType":"vector","result":[{"metric":{"__name__":"h"},
		"histogram":[1767225600,{"count":"10","sum":"3.5","buckets":[[0,"0","1","4"],[3,"1","2","6"]]}]}]}`)
	rows, err := decodeResult(data)
	if err != nil || len(rows) != 1 {
		t.Fatalf("rows=%v err=%v", rows, err)
	}
	h := rows[0].Hist
	if rows[0].HasValue || h == nil || h.Count != 10 || h.Sum != 3.5 || len(h.Buckets) != 2 {
		t.Fatalf("hist %+v", h)
	}
	if b := h.Buckets[1]; !b.LowerInclusive || !b.UpperInclusive || b.Upper != 2 || b.Count != 6 {
		t.Fatalf("bucket %+v", b)
	}
	// boundary rule 0 = open left: (lower, upper].
	if b := h.Buckets[0]; b.LowerInclusive || !b.UpperInclusive {
		t.Fatalf("bucket0 %+v", b)
	}
}

func TestDecodeScalarAndString(t *testing.T) {
	rows, err := decodeResult(json.RawMessage(`{"resultType":"scalar","result":[1767225600,"2"]}`))
	if err != nil || len(rows) != 1 || !rows[0].HasValue || rows[0].Value != 2 {
		t.Fatalf("scalar %v %v", rows, err)
	}
	rows, err = decodeResult(json.RawMessage(`{"resultType":"string","result":[1767225600,"hello"]}`))
	if err != nil || len(rows) != 1 || rows[0].Str == nil || *rows[0].Str != "hello" {
		t.Fatalf("string %v %v", rows, err)
	}
}

func TestMetricNames(t *testing.T) {
	e, err := promqlParser.ParseExpr(`sum(rate(b_total[5m])) / on() a or {__name__="c"} or {__name__=~"d|e"}`)
	if err != nil {
		t.Fatal(err)
	}
	got := metricNames(e)
	if len(got) != 3 || got[0] != "a" || got[1] != "b_total" || got[2] != "c" {
		t.Fatalf("metricNames = %v", got)
	}
}

func TestParseLabelColumns(t *testing.T) {
	if got, err := parseLabelColumns(" env, site ,env"); err != nil || len(got) != 2 {
		t.Fatalf("%v %v", got, err)
	}
	for _, bad := range []string{"ts", "__name__", "1x"} {
		if _, err := parseLabelColumns(bad); err == nil {
			t.Errorf("parseLabelColumns(%q) accepted", bad)
		}
	}
}

func TestRetryDelayHonoursRetryAfter(t *testing.T) {
	if d := retryDelay("0", 3); d != 0 {
		t.Fatalf("Retry-After: 0 gave %v", d)
	}
	if d := retryDelay("", 1); d <= 0 {
		t.Fatalf("backoff %v", d)
	}
}
