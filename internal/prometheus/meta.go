// Discovery metadata: the one file to edit when you rename or extend the worker.
//
// DuckDB, coding agents and the vgi-lint metadata checker all read these tags.
// Keep every object documented and `make lint-explain` stays at 100.
//
//   - Catalog-level tags (title, license, author, links, long description)
//     live in CatalogTags below.
//   - Every function carries objectTags(...): a title, a category from
//     SchemaCategories, search keywords, and LLM- and human-facing docs.

package prometheus

import (
	"encoding/json"
	"fmt"
	"strings"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

// CatalogName is the name DuckDB attaches the worker under.
const CatalogName = "prometheus"

// SourceURL is where this worker's code lives.
const SourceURL = "https://github.com/Query-farm/vgi-prometheus"

// category is one entry of the schema's vgi.categories list.
type category struct {
	Name        string `json:"name"`
	Title       string `json:"title"`
	Description string `json:"description"`
}

// SchemaCategories group functions in docs and agent tools. Every function's
// vgi.category must name one of these.
var SchemaCategories = []category{
	{Name: "query", Title: "PromQL Queries", Description: "Evaluate arbitrary PromQL at an instant or over a range."},
	{Name: "samples", Title: "Samples", Description: "Typed metric samples with SQL filter pushdown into PromQL selectors."},
	{Name: "metadata", Title: "Series Metadata", Description: "Series, label names, label values and metric metadata (type, help, unit)."},
	{Name: "promql", Title: "PromQL Tools", Description: "Offline PromQL validation, formatting, metric extraction and duration parsing (no server)."},
	{Name: "status", Title: "Server Status", Description: "Scrape targets, rules, alerts, Alertmanagers, TSDB cardinality and build info."},
}

// mustJSON marshals v, panicking on failure (only static data is passed).
func mustJSON(v any) string {
	b, err := json.Marshal(v)
	if err != nil {
		panic(err)
	}
	return string(b)
}

// keywordsArray turns "a, b, c" into the JSON array string vgi.keywords requires.
func keywordsArray(keywords string) string {
	out := []string{}
	for _, k := range strings.Split(keywords, ",") {
		if k = strings.TrimSpace(k); k != "" {
			out = append(out, k)
		}
	}
	return mustJSON(out)
}

// docs is the set of standard per-object discovery fields.
type docs struct {
	Title    string
	Category string
	Keywords string
	DocLLM   string
	DocMD    string
}

// objectTags builds the standard per-object discovery tags.
func objectTags(d docs) map[string]string {
	known := false
	for _, c := range SchemaCategories {
		known = known || c.Name == d.Category
	}
	if !known {
		panic(fmt.Sprintf("unknown category %q; add it to SchemaCategories", d.Category))
	}
	return map[string]string{
		"vgi.title":    d.Title,
		"vgi.category": d.Category,
		"vgi.keywords": keywordsArray(d.Keywords),
		"vgi.doc_llm":  d.DocLLM,
		"vgi.doc_md":   d.DocMD,
	}
}

// resultColumn describes one output column for vgi.result_columns_schema.
type resultColumn struct {
	Name        string `json:"name"`
	Type        string `json:"type"`
	Description string `json:"description"`
}

// column builds an Arrow field whose comment shows up in DuckDB's DESCRIBE and agent tools.
func column(name string, typ arrow.DataType, comment string, nullable bool) arrow.Field {
	return arrow.Field{Name: name, Type: typ, Nullable: nullable,
		Metadata: arrow.NewMetadata([]string{"comment"}, []string{comment})}
}

// exampleQuery is one entry of vgi.example_queries.
type exampleQuery struct {
	Description string `json:"description"`
	SQL         string `json:"sql"`
}

// exampleQueries converts catalog examples to vgi.example_queries entries.
func exampleQueries(lists ...[]vgi.CatalogExample) []exampleQuery {
	out := []exampleQuery{}
	for _, examples := range lists {
		for _, ex := range examples {
			out = append(out, exampleQuery{Description: ex.Description, SQL: ex.SQL})
		}
	}
	return out
}

// withExamples publishes examples (with descriptions) as vgi.example_queries.
// Overloads share one SQL name, so pass every overload's examples.
func withExamples(tags map[string]string, lists ...[]vgi.CatalogExample) map[string]string {
	tags["vgi.example_queries"] = mustJSON(exampleQueries(lists...))
	return tags
}

const catalogDocLLM = "Query any Prometheus-compatible HTTP API (Prometheus, Thanos, Cortex/Mimir, VictoriaMetrics, " +
	"Grafana Cloud, Amazon Managed Prometheus through a bearer token proxy) from SQL. Every API function needs a " +
	"DuckDB secret: CREATE SECRET (TYPE prometheus, BASE_URL 'http://host:9090', BEARER_TOKEN or USERNAME/PASSWORD, " +
	"optional TENANT_ID sent as X-Scope-OrgID, optional EXTRA_HEADERS as a JSON object). " +
	"query(promql, time := ...) is an instant query; query_range(promql, start, end, step_seconds) evaluates at a fixed " +
	"step, split into requests under the 11,000-point limit; both return long rows (metric, job, instance, labels map, " +
	"ts, value, histogram). samples(metric) returns stored samples with SQL label and ts filters pushed into the PromQL " +
	"selector and request window. series, label_names, label_values and metric_metadata read series metadata; targets, " +
	"rules, alerts, alertmanagers, tsdb_status and build_info read server status. is_valid, parse_error, format_expr, " +
	"metric_names and duration_seconds run the PromQL parser locally with no server. All functions are read-only."

// CatalogDocMD is the catalog's comment: one short paragraph for directories.
const CatalogDocMD = "Query Prometheus-compatible HTTP APIs (Prometheus, Thanos, Mimir, VictoriaMetrics, Grafana Cloud) " +
	"from DuckDB: PromQL instant and range queries, typed samples with filter pushdown, series metadata, " +
	"server status and offline PromQL tools."

const catalogDocMDLong = "# Prometheus in DuckDB\n\n" +
	"Run PromQL against any Prometheus-compatible HTTP API and get typed rows back: labels as a " +
	"`MAP` of label names to values plus hoisted `metric`, `job` and `instance` columns, `TIMESTAMPTZ` times and " +
	"`DOUBLE` values (NaN and Inf kept), with native histograms as a typed struct.\n\n" +
	"- **query / query_range**: arbitrary PromQL; long ranges are split under the 11,000-point limit without changing the step.\n" +
	"- **samples**: one metric's samples; SQL filters on labels and `ts` become PromQL matchers and the request window.\n" +
	"- **Metadata and status**: series, label names and values, metric metadata, targets, rules, alerts, Alertmanagers, TSDB cardinality, build info.\n" +
	"- **PromQL tools**: validate, explain, canonicalize and audit PromQL with no server.\n\n" +
	"Credentials come from a `prometheus` DuckDB secret (bearer token or basic auth, optional tenant header)."

const schemaDocLLM = "The main schema holds every function. Table functions that call the server take an optional " +
	"secret_name named argument (default: the prometheus secret whose scope matches). Time arguments are named " +
	"start_date/end_date (query_range takes them positionally) and step arguments step_seconds. Rows are long: one row " +
	"per series and timestamp. Read a label with labels['name'] or, in samples, list it in label_columns to get a " +
	"pushdown column. Scalars (is_valid, parse_error, format_expr, metric_names, duration_seconds) take one `VARCHAR` " +
	"column and need no secret."

// SchemaDocMD is the main schema's human-facing description (also its comment).
const SchemaDocMD = "## main\n\n" +
	"Everything this worker exposes lives here. Table functions call the Prometheus HTTP API named by a " +
	"`prometheus` secret and return long rows (one per series and timestamp); scalars run the PromQL parser " +
	"locally.\n\n" +
	"Reach for `samples` when you want SQL filters pushed into the PromQL selector, and for the " +
	"query functions when you need arbitrary PromQL such as `rate()` or aggregations."

// agentTask is one public analyst task for `vgi-lint simulate`.
type agentTask struct {
	Name   string `json:"name"`
	Prompt string `json:"prompt"`
}

// agentTestTasks are graded privately by vgi-agent-tests.yaml. The API tasks
// assume a prometheus secret pointing at the fixture (test/fixture_server).
var agentTestTasks = []agentTask{
	{Name: "broken_expressions", Prompt: "Using the prometheus worker, which of these PromQL expressions do not parse: " +
		"'rate(x[5m])', 'sum(rate(x[5m])', 'up == 1', 'avg by (job (up)'? Return each broken one with the parser's error message."},
	{Name: "canonical_form", Prompt: "Using the prometheus worker, give the canonical form of the PromQL 'sum(rate(http_requests_total[5m]))by(job)'."},
	{Name: "rule_metrics", Prompt: "Using the prometheus worker, which metric names does the expression " +
		"'sum(rate(errors_total[5m])) / sum(rate(requests_total[5m]))' read?"},
	{Name: "duration", Prompt: "Using the prometheus worker, how many seconds is the Prometheus duration '1h30m'?"},
	{Name: "temperature_at", Prompt: "What was fixture_temperature_celsius for each instance at 2026-01-01 00:15:00 UTC?"},
	{Name: "hourly_range", Prompt: "Evaluate fixture_temperature_celsius every 60 seconds from 2026-01-01 00:00 to 06:00 UTC: how many points per instance?"},
	{Name: "raw_sample_count", Prompt: "How many raw stored samples does fixture_temperature_celsius have for instance node-a:9100 between 2026-01-01 00:00 (inclusive) and 01:00 UTC (exclusive)?"},
	{Name: "fixture_series", Prompt: "How many series does job 'fixture' have per metric name between 2026-01-01 00:00 and 06:00 UTC?"},
	{Name: "fixture_labels", Prompt: "Which label names does fixture_temperature_celsius have between 2026-01-01 00:00 and 06:00 UTC?"},
	{Name: "fixture_instances", Prompt: "Which instance values does fixture_temperature_celsius have between 2026-01-01 00:00 and 06:00 UTC?"},
	{Name: "metric_help", Prompt: "What type and help text does the metric prometheus_http_requests_total have?"},
	{Name: "down_targets", Prompt: "Which scrape pools have a target that is down?"},
	{Name: "alerting_rules", Prompt: "List the alerting rules with their 'for' duration in seconds."},
	{Name: "firing_alerts", Prompt: "Which alerts are currently firing?"},
	{Name: "alertmanager_urls", Prompt: "Which Alertmanagers does the server send alerts to?"},
	{Name: "head_series", Prompt: "How many series are in the TSDB head block?"},
	{Name: "server_version", Prompt: "Which Prometheus version is the server running?"},
}
// CatalogTags are the catalog-level discovery tags.
var CatalogTags = map[string]string{
	"vgi.title":              "VGI Prometheus Worker",
	"vgi.subtitle":           "PromQL queries, typed samples with pushdown, metadata and server status from any Prometheus-compatible API.",
	"vgi.doc_llm":            catalogDocLLM,
	"vgi.doc_md":             catalogDocMDLong,
	"vgi.source_url":         SourceURL,
	"vgi.icon_url":           "https://query.farm/media-kit/products/vgi/vgi-logo.png",
	"vgi.license":            "0BSD",
	"vgi.author":             "Query.Farm",
	"vgi.copyright":          "Copyright 2026 Query Farm LLC - https://query.farm",
	"vgi.keywords":           keywordsArray("prometheus, promql, metrics, time series, observability, thanos, mimir, victoriametrics, grafana cloud, monitoring"),
	"vgi.support_contact":    SourceURL + "/issues",
	"vgi.support_policy_url": SourceURL + "#support",
	// Public analyst tasks for `vgi-lint simulate`; private graders live in vgi-agent-tests.yaml.
	"vgi.agent_test_tasks": mustJSON(agentTestTasks),
}

// schemaTags builds the main schema's tags; allExamples feeds vgi.example_queries.
func schemaTags(allExamples []exampleQuery) map[string]string {
	return map[string]string{
		"vgi.title":           "Prometheus Functions",
		"domain":              "observability",
		"vgi.keywords":        keywordsArray("promql, query_range, samples, series, labels, targets, rules, alerts, tsdb"),
		"vgi.doc_llm":         schemaDocLLM,
		"vgi.doc_md":          SchemaDocMD,
		"vgi.categories":      mustJSON(SchemaCategories),
		"vgi.example_queries": mustJSON(allExamples),
	}
}
