// samples: a typed, filter-pushdown view of one metric's stored samples.
//
//	SELECT instance, ts, value
//	FROM prometheus.main.samples('node_load1', label_columns := 'env,site')
//	WHERE job = 'node' AND env IN ('prod', 'qa')
//	  AND ts >= TIMESTAMPTZ '2026-01-01 00:00:00+00' AND ts < TIMESTAMPTZ '2026-01-02 00:00:00+00';
//
// Label predicates compile into the PromQL selector and ts predicates into
// the request window (selector.go); every predicate is then re-applied
// exactly (filters.go). Two modes:
//
//   - step_seconds := 0 (default): raw stored samples. The window is walked
//     in chunks of chunk_seconds with instant queries of `selector[<n>ms]`
//     evaluated at each chunk's end; Prometheus range selectors are
//     left-open, so consecutive chunks (t0, t1], (t1, t2], ... never overlap
//     or skip a sample.
//   - step_seconds > 0: the selector is evaluated at every step with
//     query_range (last sample within the server's lookback, 5m by default),
//     chunked under max_points_per_chunk points per request.
//
// The cursor (next chunk start) is externalized in SamplesState.

package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/url"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

type samplesArgs struct {
	Metric            string     `vgi:"pos=0,doc=Metric name (e.g. 'node_load1') or a full series selector (e.g. 'up{job=\"api\"}'); SQL filters add matchers to it."`
	LabelColumns      string     `vgi:"name=label_columns,default=,doc=Comma-separated label names to expose as their own columns (in addition to metric/job/instance) so they can be filtered and pushed down, e.g. 'env,region'."`
	StepSeconds       float64    `vgi:"name=step_seconds,default=0,ge=0,doc=0 (default) returns raw stored samples; > 0 evaluates the selector at this fixed step (query_range)."`
	StartDate         *time.Time `vgi:"name=start_date,doc=Window start when not given by a WHERE ts >= ... predicate."`
	EndDate           *time.Time `vgi:"name=end_date,doc=Window end (inclusive) when not given by WHERE ts <= ...; default: now."`
	ChunkSeconds      int64      `vgi:"name=chunk_seconds,default=3600,ge=1,doc=Raw mode: seconds of data fetched per request."`
	MaxPointsPerChunk int64      `vgi:"name=max_points_per_chunk,default=11000,ge=1,le=11000,doc=Step mode: evaluation points per request (Prometheus rejects more than 11000)."`
	SecretName        *string    `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a samplesArgs) secret() string { return deref(a.SecretName) }

// SamplesState is the scan cursor. Exported fields only: it is gob-encoded
// into the HTTP continuation token.
type SamplesState struct {
	Selector    string
	Columns     []colSpec
	FiltersJSON string
	StepMs      int64
	ChunkMs     int64
	ChunkPoints int64
	StartMs     int64
	EndMs       int64
	// NextMs: raw mode = exclusive lower bound of the next chunk; step mode =
	// next evaluation time.
	NextMs     int64
	Chunks     int64
	SecretName string
	Empty      bool
}

// SamplesFunc implements samples(metric, ...).
type SamplesFunc struct{}

var _ vgi.TypedTableFunc[SamplesState] = (*SamplesFunc)(nil)

var samplesExamples = []vgi.CatalogExample{
	{SQL: "SELECT instance, count(*) AS samples, min(value) AS lo, max(value) AS hi FROM prometheus.main.samples('fixture_temperature_celsius') " +
		"WHERE job = 'fixture' AND instance IN ('node-a:9100', 'node-b:9100') AND ts >= TIMESTAMPTZ '2026-01-01 00:00:00+00' AND ts < TIMESTAMPTZ '2026-01-01 01:00:00+00' GROUP BY instance ORDER BY instance",
		Description: "Raw samples for two nodes over one hour; the job/instance filters and the ts range are pushed into the PromQL selector and request window"},
	{SQL: "SELECT env, site, avg(value) AS avg_c FROM prometheus.main.samples('fixture_temperature_celsius', label_columns := 'env,site', step_seconds := 300) " +
		"WHERE env LIKE 'prod%' AND ts BETWEEN TIMESTAMPTZ '2026-01-01 00:00:00+00' AND TIMESTAMPTZ '2026-01-01 06:00:00+00' GROUP BY ALL ORDER BY ALL",
		Description: "Five-minute resampled averages per environment and site, with env exposed as a pushdown column"},
}

// Name is the SQL name.
func (*SamplesFunc) Name() string { return "samples" }

// Metadata describes the function.
func (*SamplesFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "Metric Samples With Pushdown",
		Category: "samples",
		Keywords: "samples, raw samples, time series, filter pushdown, selector, label matcher, promql, metric, export",
		DocLLM: "Read one metric's samples as typed rows, with SQL WHERE filters pushed down. Positional arg: metric name or " +
			"series selector. Named: label_columns (comma-separated labels to expose as `VARCHAR` columns, e.g. 'env,region'), " +
			"step_seconds (0 = raw stored samples, default; > 0 = evaluate at a fixed step), start_date/end_date (alternative to a WHERE ts range), " +
			"chunk_seconds (raw mode request size, default 3600), max_points_per_chunk (step mode, default 11000), secret_name. " +
			"Columns: metric, job, instance, one column per label_columns entry, labels `MAP(VARCHAR,VARCHAR)`, ts `TIMESTAMPTZ`, value `DOUBLE`, " +
			"histogram (native histograms). Pushdown: =, <>, IN, OR of equalities on one column, IS [NOT] NULL, LIKE, NOT LIKE, prefix, suffix and " +
			"contains on metric/job/instance/label columns become PromQL label matchers; ts comparisons/BETWEEN become the request window. " +
			"DuckDB does not push regexp_matches or ILIKE to the worker, so put a regex matcher in the selector argument instead " +
			"(e.g. 'up{instance=~\"node-[ab].*\"}'). " +
			"A lower time bound is required (WHERE ts >= ... or start_date :=). Filters on labels['x'] are applied in DuckDB, so list the label in label_columns to push it.",
		DocMD: "## samples(metric, label_columns := '', step_seconds := 0, ...)\n\n" +
			"Typed rows for one metric. SQL filters on label columns compile into the PromQL selector and `ts` ranges into the " +
			"request window; every filter is also re-applied exactly by the worker, so results always match the SQL. " +
			"A lower time bound is required (`WHERE ts >= ...` or `start_date :=`).",
	})
	tags["vgi.result_dynamic_columns_md"] = "The result always has `metric`, `job`, `instance`, then one `VARCHAR` column per name in " +
		"`label_columns` (in the order given), then `labels`, `ts`, `value` and `histogram`.\n\n" +
		"| Name | Type | Description |\n|---|---|---|\n" +
		"| `metric` | VARCHAR | The __name__ label. |\n" +
		"| `job` | VARCHAR | The job label (NULL when absent). |\n" +
		"| `instance` | VARCHAR | The instance label (NULL when absent). |\n" +
		"| `<label>` | VARCHAR | One per label_columns entry, e.g. `env`; NULL when the series lacks the label. |\n" +
		"| `labels` | MAP(VARCHAR, VARCHAR) | Every label of the series. |\n" +
		"| `ts` | TIMESTAMP WITH TIME ZONE | Sample time. |\n" +
		"| `value` | DOUBLE | Float value (NaN/Inf kept); NULL for native histograms. |\n" +
		"| `histogram` | STRUCT | Native-histogram sample; NULL for float samples. |"
	return vgi.FunctionMetadata{
		Description:     "Stored or resampled samples of one metric as typed rows; label and time filters push down into the PromQL selector",
		Stability:       vgi.StabilityVolatile,
		FilterPushdown:  true,
		Tags:            withExamples(tags, samplesExamples),
		RequiredSecrets: secretRequirements,
		Examples:        samplesExamples,
	}
}

// ArgumentSpecs derives the signature.
func (*SamplesFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(samplesArgs{}) }

// reservedColumns cannot be used as label column names.
var reservedColumns = map[string]bool{"metric": true, "job": true, "instance": true, "labels": true, "ts": true, "value": true, "histogram": true}

// samplesColumns returns the layout for the given extra label columns.
func samplesColumns(extra []string) []colSpec {
	cols := []colSpec{
		{Name: "metric", Kind: kindLabel, Label: "__name__"},
		{Name: "job", Kind: kindLabel, Label: "job"},
		{Name: "instance", Kind: kindLabel, Label: "instance"},
	}
	for _, l := range extra {
		cols = append(cols, colSpec{Name: l, Kind: kindLabel, Label: l})
	}
	return append(cols,
		colSpec{Name: "labels", Kind: kindLabels},
		colSpec{Name: "ts", Kind: kindTs},
		colSpec{Name: "value", Kind: kindValue},
		colSpec{Name: "histogram", Kind: kindHistogram},
	)
}

// parseLabelColumns validates the label_columns argument.
func parseLabelColumns(s string) ([]string, error) {
	var out []string
	seen := map[string]bool{}
	for _, p := range strings.Split(s, ",") {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		if !legacyLabelName.MatchString(p) || strings.HasPrefix(p, "__") {
			return nil, fmt.Errorf("label_columns: %q is not a valid label name", p)
		}
		if reservedColumns[p] {
			return nil, fmt.Errorf("label_columns: %q is already a column; read it directly", p)
		}
		if !seen[p] {
			seen[p] = true
			out = append(out, p)
		}
	}
	return out, nil
}

// validateSelector accepts a metric name or a {…} selector.
func validateSelector(s string) error {
	s = strings.TrimSpace(s)
	if legacyMetricName.MatchString(s) {
		return nil
	}
	if strings.HasSuffix(s, "}") && strings.Contains(s, "{") {
		name := s[:strings.Index(s, "{")]
		if name == "" || legacyMetricName.MatchString(name) {
			return nil
		}
	}
	return fmt.Errorf("samples: metric must be a metric name or a series selector such as 'up{job=\"api\"}', got %q", s)
}

// OnBind declares the layout, which depends on label_columns.
func (*SamplesFunc) OnBind(params *vgi.BindParams) (*vgi.BindResponse, error) {
	var args samplesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	extra, err := parseLabelColumns(args.LabelColumns)
	if err != nil {
		return nil, err
	}
	if err := validateSelector(args.Metric); err != nil {
		return nil, err
	}
	return bindWithSecret[samplesArgs](params, schemaOf(samplesColumns(extra)))
}

// NewState compiles the pushed filters and plans the window.
func (*SamplesFunc) NewState(params *vgi.ProcessParams) (*SamplesState, error) {
	var args samplesArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	extra, err := parseLabelColumns(args.LabelColumns)
	if err != nil {
		return nil, err
	}
	cols := samplesColumns(extra)
	schema := schemaOf(cols)
	preds, err := compileFilters(params.CurrentPushdownFilters, schema)
	if err != nil {
		return nil, err
	}
	filtersJSON, err := json.Marshal(preds)
	if err != nil {
		return nil, err
	}
	byName := columnsByName(cols)
	plan := planPushdown(preds, byName)
	st := &SamplesState{
		Selector:    buildSelector(args.Metric, plan.Matchers),
		Columns:     cols,
		FiltersJSON: string(filtersJSON),
		ChunkMs:     args.ChunkSeconds * 1000,
		ChunkPoints: args.MaxPointsPerChunk,
		SecretName:  deref(args.SecretName),
	}
	if args.StepSeconds > 0 {
		if st.StepMs, err = stepMillis(args.StepSeconds); err != nil {
			return nil, err
		}
	}
	w := plan.Window
	if args.StartDate != nil {
		w.raiseLo(args.StartDate.UnixMilli())
	}
	if args.EndDate != nil {
		w.lowerHi(args.EndDate.UnixMilli())
	}
	if w.Lo == nil {
		return nil, fmt.Errorf("samples needs a time range: add WHERE ts >= TIMESTAMPTZ '...' (and optionally ts < ...), or pass start_date := TIMESTAMPTZ '...'")
	}
	st.StartMs = *w.Lo
	if w.Hi != nil {
		st.EndMs = *w.Hi
	} else {
		st.EndMs = time.Now().UnixMilli()
	}
	if st.EndMs < st.StartMs {
		st.Empty = true
	}
	if st.StepMs > 0 {
		st.NextMs = st.StartMs
	} else {
		st.NextMs = st.StartMs - 1
	}
	slog.Debug("samples pushdown", "selector", st.Selector, "start_ms", st.StartMs, "end_ms", st.EndMs, "filters", st.FiltersJSON)
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

func columnsByName(cols []colSpec) map[string]colSpec {
	m := make(map[string]colSpec, len(cols))
	for _, c := range cols {
		m[c.Name] = c
	}
	return m
}

// formatRange renders a millisecond duration for a range selector.
func formatRange(ms int64) string { return fmt.Sprintf("%dms", ms) }

// Process fetches one chunk per call and emits the rows that pass the filters.
func (*SamplesFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *SamplesState, out *vgirpc.OutputCollector) error {
	done := st.Empty || (st.StepMs > 0 && st.NextMs > st.EndMs) || (st.StepMs == 0 && st.NextMs >= st.EndMs)
	if done {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	var data json.RawMessage
	var advance int64
	if st.StepMs > 0 {
		last := chunkEnd(st.NextMs, st.EndMs, st.StepMs, st.ChunkPoints)
		data, err = c.get(ctx, "query_range", url.Values{
			"query": {st.Selector}, "start": {formatTime(st.NextMs)}, "end": {formatTime(last)}, "step": {formatStep(st.StepMs)},
		}, true)
		advance = last + st.StepMs
	} else {
		t := min(st.NextMs+st.ChunkMs, st.EndMs)
		data, err = c.get(ctx, "query", url.Values{
			"query": {st.Selector + "[" + formatRange(t-st.NextMs) + "]"}, "time": {formatTime(t)},
		}, true)
		advance = t
	}
	if err != nil {
		return fmt.Errorf("%w (selector %s, chunk %d)", err, st.Selector, st.Chunks+1)
	}
	rows, err := decodeResult(data)
	if err != nil {
		return err
	}
	var preds []*expr
	if err := json.Unmarshal([]byte(st.FiltersJSON), &preds); err != nil {
		return err
	}
	env := &rowEnv{cols: columnsByName(st.Columns)}
	kept := rows[:0]
	for i := range rows {
		env.r = &rows[i]
		ok, err := matchAll(preds, env)
		if err != nil {
			return err
		}
		if ok {
			kept = append(kept, rows[i])
		}
	}
	st.NextMs = advance
	st.Chunks++
	return emitRows(out, st.Columns, kept)
}
