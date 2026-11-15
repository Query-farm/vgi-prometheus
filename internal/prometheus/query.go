// query and query_range: evaluate any PromQL expression on the server.

package prometheus

import (
	"context"
	"fmt"
	"net/url"

	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
)

// DefaultMaxPoints is Prometheus's per-series point limit for one range
// query ((end-start)/step must not exceed 11,000).
const DefaultMaxPoints = 11000

// ---------------------------------------------------------------- query

type queryArgs struct {
	Promql     string     `vgi:"pos=0,doc=PromQL expression, e.g. 'sum by (job) (rate(http_requests_total[5m]))'."`
	AtTime     *time.Time `vgi:"name=at_time,doc=Evaluation time (TIMESTAMPTZ). Omit or NULL for the server's current time."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist; default: the unscoped one."`
}

// QueryState is the instant-query cursor.
type QueryState struct {
	Promql     string
	HasAt      bool
	AtMs       int64
	SecretName string
	Done       bool
}

// QueryFunc implements query(promql, at_time := ...).
type QueryFunc struct{}

var _ vgi.TypedTableFunc[QueryState] = (*QueryFunc)(nil)

var queryExamples = []vgi.CatalogExample{
	{SQL: "SELECT instance, value FROM prometheus.main.query('fixture_temperature_celsius{site=\"ams\"}', at_time := TIMESTAMPTZ '2026-01-01 00:15:00+00') ORDER BY instance",
		Description: "Current temperature of every node at one site at a fixed time"},
	{SQL: "SELECT labels['job'] AS job, value AS targets_up FROM prometheus.main.query('sum by (job) (up)') ORDER BY job",
		Description: "How many targets are up per job right now"},
}

// Name is the SQL name.
func (*QueryFunc) Name() string { return "query" }

// Metadata describes the function.
func (*QueryFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "PromQL Instant Query",
		Category: "query",
		Keywords: "promql, instant query, prometheus, thanos, mimir, victoriametrics, metrics, /api/v1/query",
		DocLLM: "Evaluate a PromQL expression at one instant (GET /api/v1/query) and return one row per " +
			"result series. Positional arg: the PromQL text. Named args: at_time (TIMESTAMPTZ; omitted = now on the server), " +
			"secret_name (pick a named prometheus secret). Columns: result_type ('vector', 'matrix', 'scalar' or 'string'), " +
			"metric (__name__), job, instance, labels MAP(VARCHAR,VARCHAR) with every label, ts TIMESTAMPTZ, value DOUBLE " +
			"(NaN/+Inf/-Inf preserved; NULL for histograms/strings), histogram STRUCT for native histograms, string_value. " +
			"A range selector such as 'up[5m]' returns raw samples as matrix rows. Aggregations like sum() drop __name__, so " +
			"metric is NULL; read grouping labels from labels['name']. Needs a prometheus secret (CREATE SECRET (TYPE prometheus, BASE_URL '...')).",
		DocMD: "## query(promql, at_time := NULL)\n\n" +
			"Instant PromQL evaluation (`/api/v1/query`). One row per result series; scalar and string results become one row.\n\n" +
			"```sql\nCREATE SECRET (TYPE prometheus, BASE_URL 'http://localhost:9090');\n" +
			"SELECT labels['job'] AS job, value FROM prometheus.main.query('sum by (job) (up)');\n```",
	})
	tags["vgi.result_columns_schema"] = mustJSON(resultColumnsOf(queryColumns))
	return vgi.FunctionMetadata{
		Description:     "Evaluate a PromQL expression at one instant and return the result series as rows",
		Stability:       vgi.StabilityVolatile,
		Tags:            withExamples(tags, queryExamples),
		RequiredSecrets: secretRequirements,
		Examples:        queryExamples,
	}
}

// ArgumentSpecs derives the signature.
func (*QueryFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(queryArgs{}) }

// OnBind declares the output schema.
func (*QueryFunc) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(schemaOf(queryColumns))
}

// NewState copies the arguments.
func (*QueryFunc) NewState(params *vgi.ProcessParams) (*QueryState, error) {
	var args queryArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	st := &QueryState{Promql: args.Promql, SecretName: deref(args.SecretName)}
	if st.Promql == "" {
		return nil, fmt.Errorf("query: promql must not be empty")
	}
	if args.AtTime != nil {
		st.HasAt, st.AtMs = true, args.AtTime.UnixMilli()
	}
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

// Process runs the query once.
func (*QueryFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *QueryState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	p := url.Values{"query": {st.Promql}}
	if st.HasAt {
		p.Set("time", formatTime(st.AtMs))
	}
	data, err := c.get(ctx, "query", p, true)
	if err != nil {
		return err
	}
	rows, err := decodeResult(data)
	if err != nil {
		return err
	}
	st.Done = true
	return emitRows(out, queryColumns, rows)
}

// ---------------------------------------------------------------- query_range

type queryRangeArgs struct {
	Promql            string    `vgi:"pos=0,doc=PromQL expression evaluated at every step."`
	StartDate         time.Time `vgi:"pos=1,doc=First evaluation time (TIMESTAMPTZ)."`
	EndDate           time.Time `vgi:"pos=2,doc=Last evaluation time (TIMESTAMPTZ); inclusive."`
	StepSeconds       float64   `vgi:"pos=3,gt=0,doc=Resolution step in seconds (e.g. 60). Never changed by the worker."`
	MaxPointsPerChunk int64     `vgi:"default=11000,ge=1,le=11000,doc=Evaluation points per request; long ranges are split into chunks of this many steps (Prometheus rejects more than 11000)."`
	SecretName        *string   `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

// RangeState is the externalized cursor of a chunked range query: the next
// chunk's first evaluation time. A scan interrupted at chunk k resumes there.
type RangeState struct {
	Promql      string
	StartMs     int64
	EndMs       int64
	StepMs      int64
	ChunkPoints int64
	NextMs      int64
	Chunks      int64
	SecretName  string
}

// QueryRangeFunc implements query_range(promql, start, end, step_seconds).
type QueryRangeFunc struct{}

var _ vgi.TypedTableFunc[RangeState] = (*QueryRangeFunc)(nil)

var queryRangeExamples = []vgi.CatalogExample{
	{SQL: "SELECT instance, avg(value) AS avg_c, max(value) AS max_c FROM prometheus.main.query_range('fixture_temperature_celsius', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 06:00:00+00', 60) GROUP BY instance ORDER BY instance",
		Description: "Six-hour average and peak temperature per node at one-minute resolution"},
	{SQL: "SELECT date_trunc('hour', ts) AS hour, labels['code'] AS code, avg(value) AS req_per_s FROM prometheus.main.query_range('sum by (code) (rate(fixture_requests_total[5m]))', TIMESTAMPTZ '2026-01-01 00:00:00+00', TIMESTAMPTZ '2026-01-01 06:00:00+00', 300) GROUP BY ALL ORDER BY hour, code",
		Description: "Hourly request rate by status code from a five-minute rate()"},
}

// Name is the SQL name.
func (*QueryRangeFunc) Name() string { return "query_range" }

// Metadata describes the function.
func (*QueryRangeFunc) Metadata() vgi.FunctionMetadata {
	tags := objectTags(docs{
		Title:    "PromQL Range Query",
		Category: "query",
		Keywords: "promql, range query, time series, prometheus, step, resolution, /api/v1/query_range, chunking",
		DocLLM: "Evaluate a PromQL expression at every step between two times (/api/v1/query_range) and return long-format " +
			"rows: one per series per step. Positional args: promql, start (TIMESTAMPTZ), end (TIMESTAMPTZ, inclusive), " +
			"step_seconds (DOUBLE > 0). Named: max_points_per_chunk (default 11000), secret_name. Ranges longer than the " +
			"server's 11,000-points-per-series limit are split into consecutive chunks automatically; the step is never " +
			"changed, so a chunked pull returns exactly the rows an unchunked one would. Same columns as query(); result_type is 'matrix'. " +
			"Use query() for a single instant and samples() for raw stored samples with SQL filter pushdown.",
		DocMD: "## query_range(promql, start, end, step_seconds, max_points_per_chunk := 11000)\n\n" +
			"Range PromQL evaluation (`/api/v1/query_range`) as long-format rows, chunked under the 11,000-point limit " +
			"with a resumable cursor.",
	})
	tags["vgi.result_columns_schema"] = mustJSON(resultColumnsOf(queryColumns))
	return vgi.FunctionMetadata{
		Description:     "Evaluate a PromQL expression over a time range at a fixed step, chunked under the server's point limit",
		Stability:       vgi.StabilityVolatile,
		Tags:            withExamples(tags, queryRangeExamples),
		RequiredSecrets: secretRequirements,
		Examples:        queryRangeExamples,
	}
}

// ArgumentSpecs derives the signature.
func (*QueryRangeFunc) ArgumentSpecs() []vgi.ArgSpec { return vgi.DeriveArgSpecs(queryRangeArgs{}) }

// OnBind declares the output schema.
func (*QueryRangeFunc) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(schemaOf(queryColumns))
}

// stepMillis converts a step in seconds to whole milliseconds (Prometheus's resolution).
func stepMillis(sec float64) (int64, error) {
	ms := int64(sec*1000 + 0.5)
	if ms < 1 {
		return 0, fmt.Errorf("step_seconds must be at least 0.001")
	}
	return ms, nil
}

// NewState validates the range and starts the cursor at the first step.
func (*QueryRangeFunc) NewState(params *vgi.ProcessParams) (*RangeState, error) {
	var args queryRangeArgs
	if err := vgi.BindArgs(params.Args, &args); err != nil {
		return nil, err
	}
	step, err := stepMillis(args.StepSeconds)
	if err != nil {
		return nil, err
	}
	st := &RangeState{Promql: args.Promql, StartMs: args.StartDate.UnixMilli(), EndMs: args.EndDate.UnixMilli(),
		StepMs: step, ChunkPoints: args.MaxPointsPerChunk, SecretName: deref(args.SecretName)}
	if st.Promql == "" {
		return nil, fmt.Errorf("query_range: promql must not be empty")
	}
	if st.EndMs < st.StartMs {
		return nil, fmt.Errorf("query_range: end (%s) is before start (%s)", args.EndDate.UTC().Format(time.RFC3339), args.StartDate.UTC().Format(time.RFC3339))
	}
	st.NextMs = st.StartMs
	if _, err := resolveConn(params.Secrets, st.SecretName); err != nil {
		return nil, err
	}
	return st, nil
}

// chunkEnd is the last evaluation time of the chunk starting at next.
func chunkEnd(next, end, step, points int64) int64 {
	return min(end, next+(points-1)*step)
}

// Process fetches one chunk per call.
func (*QueryRangeFunc) Process(ctx context.Context, params *vgi.ProcessParams, st *RangeState, out *vgirpc.OutputCollector) error {
	if st.NextMs > st.EndMs {
		return out.Finish()
	}
	c, err := resolveConn(params.Secrets, st.SecretName)
	if err != nil {
		return err
	}
	last := chunkEnd(st.NextMs, st.EndMs, st.StepMs, st.ChunkPoints)
	data, err := c.get(ctx, "query_range", url.Values{
		"query": {st.Promql}, "start": {formatTime(st.NextMs)}, "end": {formatTime(last)}, "step": {formatStep(st.StepMs)},
	}, true)
	if err != nil {
		return fmt.Errorf("%w (chunk %d starting %s)", err, st.Chunks+1, time.UnixMilli(st.NextMs).UTC().Format(time.RFC3339))
	}
	rows, err := decodeResult(data)
	if err != nil {
		return err
	}
	st.NextMs = last + st.StepMs
	st.Chunks++
	return emitRows(out, queryColumns, rows)
}

// ---------------------------------------------------------------- helpers

func deref(s *string) string {
	if s == nil {
		return ""
	}
	return *s
}

// emitRows emits rows as one batch (an empty batch is fine: the scan continues).
func emitRows(out *vgirpc.OutputCollector, cols []colSpec, rows []row) error {
	arrs := buildRows(cols, rows)
	for _, a := range arrs {
		defer a.Release()
	}
	return out.EmitArrays(arrs, int64(len(rows)))
}

// resultColumnsOf documents a column layout for vgi.result_columns_schema.
func resultColumnsOf(cols []colSpec) []resultColumn {
	out := make([]resultColumn, len(cols))
	for i, c := range cols {
		f := c.field()
		desc, _ := f.Metadata.GetValue("comment")
		out[i] = resultColumn{Name: c.Name, Type: sqlTypeOf(c), Description: desc}
	}
	return out
}

func sqlTypeOf(c colSpec) string {
	switch c.Kind {
	case kindLabels:
		return "MAP(VARCHAR, VARCHAR)"
	case kindTs:
		return "TIMESTAMP WITH TIME ZONE"
	case kindValue:
		return "DOUBLE"
	case kindHistogram:
		return histogramSQLType
	}
	return "VARCHAR"
}
