// PromQL tools: scalars that run Prometheus's own parser inside the worker,
// with no server and no secret. They lint, canonicalize and audit PromQL
// stored in SQL tables (alert rules, dashboards) and carry the worker's
// executable examples.
//
//	SELECT prometheus.main.is_valid('rate(x[5m])');             -- true
//	SELECT prometheus.main.parse_error('rate(x[5m]');           -- position + message
//	SELECT prometheus.main.format_expr('sum(rate(x[5m]))by(a)'); -- canonical text
//	SELECT prometheus.main.metric_names('a / on(x) b');         -- [a, b]
//	SELECT prometheus.main.duration_seconds('1h30m');           -- 5400.0

package prometheus

import (
	"context"
	"sort"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/prometheus/common/model"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/promql/parser"
)

// promqlParser accepts the experimental functions and duration expressions so
// that expressions valid on a current server are not rejected here.
var promqlParser = parser.NewParser(parser.Options{
	EnableExperimentalFunctions:  true,
	ExperimentalDurationExpr:     true,
	EnableExtendedRangeSelectors: true,
})

// exprArgs is the single PromQL text column every PromQL tool takes.
type exprArgs struct {
	Expr *array.String `vgi:"pos=0,const=false,doc=PromQL expression text, e.g. 'sum by (job) (rate(http_requests_total[5m]))'. NULL gives NULL."`
}

type durationArgs struct {
	Duration *array.String `vgi:"pos=0,const=false,doc=A Prometheus duration string, for example 1h30m. NULL or an invalid duration gives NULL."`
}

// executableExample is one vgi.executable_examples entry.
type executableExample struct {
	Description string  `json:"description"`
	SQL         string  `json:"sql"`
	Expected    [][]any `json:"expected_result"`
}

// metricNames lists the distinct metric names an expression reads, sorted.
// A selector without a literal name (e.g. {__name__=~"a|b"}) contributes nothing.
func metricNames(e parser.Expr) []string {
	seen := map[string]bool{}
	parser.Inspect(e, func(n parser.Node, _ []parser.Node) error {
		vs, ok := n.(*parser.VectorSelector)
		if !ok {
			return nil
		}
		name := vs.Name
		if name == "" {
			for _, m := range vs.LabelMatchers {
				if m.Name == labels.MetricName && m.Type == labels.MatchEqual {
					name = m.Value
				}
			}
		}
		if name != "" {
			seen[name] = true
		}
		return nil
	})
	out := make([]string, 0, len(seen))
	for n := range seen {
		out = append(out, n)
	}
	sort.Strings(out)
	return out
}

// promqlScalar is a vectorized one-column scalar over PromQL text.
type promqlScalar struct {
	name       string
	docs       docs
	desc       string
	examples   []vgi.CatalogExample
	executable []executableExample
	result     arrow.DataType
	// eval appends the result for one non-NULL input to b.
	eval func(b array.Builder, s string)
}

func (f *promqlScalar) Name() string { return f.name }

func (f *promqlScalar) Metadata() vgi.FunctionMetadata {
	tags := withExamples(objectTags(f.docs), f.examples)
	tags["vgi.executable_examples"] = mustJSON(f.executable)
	return vgi.FunctionMetadata{
		Description: f.desc,
		Stability:   vgi.StabilityConsistent,
		Tags:        tags,
		Examples:    f.examples,
	}
}

func (f *promqlScalar) OnBindTyped(*exprArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(f.result)
}

func (f *promqlScalar) ProcessTyped(_ context.Context, _ *exprArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return f.process(batch)
}

func (f *promqlScalar) process(batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	col := batch.Column(0)
	b := array.NewBuilder(memory.NewGoAllocator(), f.result)
	defer b.Release()
	for i := 0; i < int(batch.NumRows()); i++ {
		if col.IsNull(i) {
			b.AppendNull()
			continue
		}
		f.eval(b, vgi.GetStringValue(col, i))
	}
	arr := b.NewArray()
	defer arr.Release()
	schema := arrow.NewSchema([]arrow.Field{{Name: "result", Type: f.result, Nullable: true}}, nil)
	return array.NewRecordBatch(schema, []arrow.Array{arr}, batch.NumRows()), nil
}

// durationScalar is duration_seconds(d): same shape, different argument doc.
type durationScalar struct{ promqlScalar }

func (f *durationScalar) OnBindTyped(*durationArgs, *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindResult(f.result)
}

func (f *durationScalar) ProcessTyped(_ context.Context, _ *durationArgs, _ *vgi.ProcessParams, batch arrow.RecordBatch) (arrow.RecordBatch, error) {
	return f.process(batch)
}

const toolsDocSuffix = " Runs Prometheus's own PromQL parser inside the worker: no server, no secret, no network."

func newIsValid() *promqlScalar {
	return &promqlScalar{
		name: "is_valid",
		desc: "True when a PromQL expression parses, false otherwise",
		docs: docs{Title: "Validate PromQL", Category: "promql",
			Keywords: "promql, validate, lint, syntax check, parse, alert rules, recording rules",
			DocLLM: "is_valid(expr) returns true when the PromQL text parses and false when it does not (NULL for NULL). " +
				"Use it to lint PromQL kept in SQL tables, such as alert or recording rules, before sending it to a server; " +
				"parse_error(expr) says why one fails." + toolsDocSuffix,
			DocMD: "## is_valid(expr)\n\n`true` when the PromQL parses, `false` when it does not, `NULL` for `NULL`." +
				"\n\nUses the upstream Prometheus parser, so experimental functions are accepted."},
		examples: []vgi.CatalogExample{{
			SQL: "SELECT rule_name, query FROM prometheus.main.rules() WHERE NOT prometheus.main.is_valid(query)",
			Description: "Rules whose expression no longer parses with the current PromQL grammar"}},
		executable: []executableExample{{Description: "Check three expressions",
			SQL: "SELECT e, prometheus.main.is_valid(e) AS ok FROM (VALUES ('rate(x[5m])'), ('rate(x[5m]'), ('sum by (job) (up)')) t(e) ORDER BY e",
			Expected: [][]any{{"rate(x[5m])", true}, {"rate(x[5m]", false}, {"sum by (job) (up)", true}}}},
		result: arrow.FixedWidthTypes.Boolean,
		eval: func(b array.Builder, s string) {
			_, err := promqlParser.ParseExpr(s)
			b.(*array.BooleanBuilder).Append(err == nil)
		},
	}
}

func newParseError() *promqlScalar {
	return &promqlScalar{
		name: "parse_error",
		desc: "The PromQL parser's error message (with position) for an expression, or NULL when it parses",
		docs: docs{Title: "Explain PromQL Parse Error", Category: "promql",
			Keywords: "promql, parse error, syntax error, lint, diagnostics, position",
			DocLLM: "parse_error(expr) returns the parser's message for PromQL text that does not parse, prefixed with the " +
				"position (e.g. '1:11: parse error: unclosed left parenthesis'), and NULL when it parses or the input is NULL. " +
				"Pair it with is_valid to report what is wrong with stored rules." + toolsDocSuffix,
			DocMD: "## parse_error(expr)\n\n`NULL` when the PromQL parses; otherwise `line:column: message` from the upstream parser."},
		examples: []vgi.CatalogExample{{
			SQL: "SELECT rule_name, prometheus.main.parse_error(query) AS problem FROM prometheus.main.rules() WHERE prometheus.main.parse_error(query) IS NOT NULL",
			Description: "Why each broken rule fails to parse"}},
		executable: []executableExample{{Description: "Errors for a broken and a valid expression",
			SQL: "SELECT e, prometheus.main.parse_error(e) AS problem FROM (VALUES ('up'), ('sum(rate(x[5m])')) t(e) ORDER BY e",
			Expected: [][]any{{"sum(rate(x[5m])", "1:16: parse error: unclosed left parenthesis"}, {"up", nil}}}},
		result: arrow.BinaryTypes.String,
		eval: func(b array.Builder, s string) {
			if _, err := promqlParser.ParseExpr(s); err != nil {
				b.(*array.StringBuilder).Append(err.Error())
				return
			}
			b.AppendNull()
		},
	}
}

func newFormatExpr() *promqlScalar {
	return &promqlScalar{
		name: "format_expr",
		desc: "Canonical single-line text of a PromQL expression (NULL when it does not parse)",
		docs: docs{Title: "Canonicalize PromQL", Category: "promql",
			Keywords: "promql, format, canonical, normalize, pretty print, dedupe, diff rules",
			DocLLM: "format_expr(expr) parses PromQL and prints it back in Prometheus's canonical form: normalized spacing, " +
				"grouping clauses after the aggregation, quoted label values. Two spellings of the same query give the same " +
				"text, so it is the key for deduplicating or diffing rules and dashboard queries. NULL when the text does not " +
				"parse or is NULL." + toolsDocSuffix,
			DocMD: "## format_expr(expr)\n\nThe expression as Prometheus prints it (`String()` of the parsed AST), or `NULL` when it does not parse."},
		examples: []vgi.CatalogExample{{
			SQL: "SELECT prometheus.main.format_expr(query) AS canonical, count(*) AS rules FROM prometheus.main.rules() GROUP BY 1 HAVING count(*) > 1",
			Description: "Rules that are the same expression written differently"}},
		executable: []executableExample{{Description: "Two spellings, one canonical form",
			SQL: "SELECT DISTINCT prometheus.main.format_expr(e) AS canonical FROM (VALUES ('sum(rate(http_requests_total[5m]))by(job)'), ('sum by (job) (rate(http_requests_total[5m]))')) t(e)",
			Expected: [][]any{{"sum by (job) (rate(http_requests_total[5m]))"}}}},
		result: arrow.BinaryTypes.String,
		eval: func(b array.Builder, s string) {
			e, err := promqlParser.ParseExpr(s)
			if err != nil {
				b.AppendNull()
				return
			}
			b.(*array.StringBuilder).Append(e.String())
		},
	}
}

func newMetricNames() *promqlScalar {
	return &promqlScalar{
		name: "metric_names",
		desc: "Sorted distinct metric names a PromQL expression reads (NULL when it does not parse)",
		docs: docs{Title: "Metrics Referenced By PromQL", Category: "promql",
			Keywords: "promql, metric names, dependencies, audit, cardinality, lineage, selectors",
			DocLLM: "metric_names(expr) returns the sorted distinct metric names referenced by the PromQL expression's " +
				"selectors, as a list of strings; selectors with a literal {__name__=\"x\"} count, regex name matchers do not. " +
				"NULL when the text does not parse or is NULL. Use it to find which rules or dashboards depend on a metric " +
				"before renaming or dropping it, or join it to series() for cardinality." + toolsDocSuffix,
			DocMD: "## metric_names(expr)\n\nSorted, distinct metric names read by the expression's selectors (`VARCHAR[]`)."},
		examples: []vgi.CatalogExample{{
			SQL: "SELECT m AS metric, count(*) AS rules FROM prometheus.main.rules(), unnest(prometheus.main.metric_names(query)) u(m) GROUP BY m ORDER BY rules DESC",
			Description: "Which metrics the most rules depend on"}},
		executable: []executableExample{{Description: "Metrics read by a ratio query",
			SQL: "SELECT prometheus.main.metric_names('sum(rate(errors_total[5m])) / sum(rate(requests_total[5m])) > on() group_left vector(0.1) or errors_total') AS metrics",
			Expected: [][]any{{[]any{"errors_total", "requests_total"}}}}},
		result: arrow.ListOf(arrow.BinaryTypes.String),
		eval: func(b array.Builder, s string) {
			e, err := promqlParser.ParseExpr(s)
			if err != nil {
				b.AppendNull()
				return
			}
			lb := b.(*array.ListBuilder)
			lb.Append(true)
			vb := lb.ValueBuilder().(*array.StringBuilder)
			for _, n := range metricNames(e) {
				vb.Append(n)
			}
		},
	}
}

func newDurationSeconds() *durationScalar {
	return &durationScalar{promqlScalar{
		name: "duration_seconds",
		desc: "Seconds in a Prometheus duration string such as '5m' or '1h30m' (NULL when invalid)",
		docs: docs{Title: "Parse Prometheus Duration", Category: "promql",
			Keywords: "duration, interval, seconds, scrape interval, range, 5m, parse",
			DocLLM: "duration_seconds(d) converts a Prometheus duration ('30s', '5m', '1h30m', '2d', '1w', '1y', or '0') to " +
				"seconds as a double, using the same grammar as Prometheus configuration and PromQL range selectors. NULL for " +
				"NULL or an invalid duration. Use it to compare scrape intervals or rule `for` clauses, or to turn a range " +
				"into a step_seconds value." + toolsDocSuffix,
			DocMD: "## duration_seconds(d)\n\n`'1h30m'` → `5400.0`. Units: ms, s, m, h, d, w, y. `NULL` when invalid."},
		examples: []vgi.CatalogExample{{
			SQL: "SELECT scrape_pool, prometheus.main.duration_seconds('1m') / count(*) AS seconds_per_target FROM prometheus.main.targets() GROUP BY scrape_pool",
			Description: "Spread a one-minute budget over each scrape pool's targets"}},
		executable: []executableExample{{Description: "Convert durations",
			SQL: "SELECT d, prometheus.main.duration_seconds(d) AS seconds FROM (VALUES ('1h30m'), ('5m'), ('250ms'), ('1d'), ('soon')) t(d) ORDER BY d",
			Expected: [][]any{{"1d", 86400.0}, {"1h30m", 5400.0}, {"250ms", 0.25}, {"5m", 300.0}, {"soon", nil}}}},
		result: arrow.PrimitiveTypes.Float64,
		eval: func(b array.Builder, s string) {
			d, err := model.ParseDuration(s)
			if err != nil {
				b.AppendNull()
				return
			}
			b.(*array.Float64Builder).Append(float64(d) / 1e9)
		},
	}}
}

// toolsExamples are the PromQL tools' catalog examples.
func toolsExamples() [][]vgi.CatalogExample {
	return [][]vgi.CatalogExample{newIsValid().examples, newParseError().examples, newFormatExpr().examples,
		newMetricNames().examples, newDurationSeconds().examples}
}

func registerTools(w *vgi.Worker) {
	for _, f := range []*promqlScalar{newIsValid(), newParseError(), newFormatExpr(), newMetricNames()} {
		w.RegisterScalar(vgi.AsScalarFunction[exprArgs](f))
	}
	w.RegisterScalar(vgi.AsScalarFunction[durationArgs](newDurationSeconds()))
}
