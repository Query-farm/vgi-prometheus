// Metadata and status functions: series, label_names, label_values,
// metric_metadata, targets, rules, alerts, alertmanagers, tsdb_status and
// build_info. Each makes one request.

package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

var str = arrow.BinaryTypes.String

// timeRangeParams adds optional start/end parameters.
func timeRangeParams(p url.Values, start, end *time.Time) {
	if start != nil {
		p.Set("start", formatTime(start.UnixMilli()))
	}
	if end != nil {
		p.Set("end", formatTime(end.UnixMilli()))
	}
}

func meta(title, category, keywords, docLLM, docMD, desc string, cols []arrow.Field, examples []vgi.CatalogExample) func() vgi.FunctionMetadata {
	return func() vgi.FunctionMetadata {
		tags := objectTags(docs{Title: title, Category: category, Keywords: keywords, DocLLM: docLLM, DocMD: docMD})
		rc := make([]resultColumn, len(cols))
		for i, f := range cols {
			d, _ := f.Metadata.GetValue("comment")
			rc[i] = resultColumn{Name: f.Name, Type: duckType(f.Type), Description: d}
		}
		tags["vgi.result_columns_schema"] = mustJSON(rc)
		return vgi.FunctionMetadata{
			Description:     desc,
			Stability:       vgi.StabilityVolatile,
			Tags:            withExamples(tags, examples),
			RequiredSecrets: secretRequirements,
			Examples:        examples,
		}
	}
}

func duckType(t arrow.DataType) string {
	switch t.ID() {
	case arrow.STRING:
		return "VARCHAR"
	case arrow.INT64:
		return "BIGINT"
	case arrow.FLOAT64:
		return "DOUBLE"
	case arrow.BOOL:
		return "BOOLEAN"
	case arrow.TIMESTAMP:
		return "TIMESTAMP WITH TIME ZONE"
	case arrow.MAP:
		return "MAP(VARCHAR, VARCHAR)"
	}
	return t.String()
}

func labelCols(prefix string) []arrow.Field {
	return []arrow.Field{
		column("metric", str, "Metric name (the __name__ label); NULL when absent.", true),
		column("job", str, "The job label; NULL when absent.", true),
		column("instance", str, "The instance label; NULL when absent.", true),
		column("labels", labelsType, prefix, false),
	}
}

func hoisted(m map[string]string) []any {
	if m == nil {
		m = map[string]string{}
	}
	return []any{optString(m["__name__"]), optString(m["job"]), optString(m["instance"]), m}
}

// ---------------------------------------------------------------- series

type seriesArgs struct {
	Match      string     `vgi:"pos=0,doc=Series selector, e.g. 'up' or '{job=\"api\"}'."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Only series with samples after this time."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Only series with samples before this time."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a seriesArgs) secret() string { return deref(a.SecretName) }

var seriesColumns = labelCols("Every label of the series, __name__ included.")

var seriesExamples = []vgi.CatalogExample{
	{SQL: "SELECT metric, count(*) AS series FROM prometheus.main.series('{job=\"fixture\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00', end_date := TIMESTAMPTZ '2026-01-01 06:00:00+00') GROUP BY metric ORDER BY series DESC, metric",
		Description: "Cardinality audit: series per metric for one job"},
	{SQL: "SELECT labels['env'] AS env, count(*) AS series FROM prometheus.main.series('fixture_temperature_celsius', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00') GROUP BY env ORDER BY env",
		Description: "How many temperature series each environment contributes"},
}

func newSeries() vgi.TableFunction {
	return newSimple(&simpleFunc[seriesArgs]{
		name:    "series",
		columns: seriesColumns,
		meta: meta("Series Matching A Selector", "metadata", "series, cardinality, label sets, /api/v1/series, selector, match",
			"List every series (label set) matching a selector, via /api/v1/series. Positional arg: the selector (e.g. '{job=\"api\"}'). "+
				"Named: start_date, end_date (`TIMESTAMPTZ`; default: the server's recent window), secret_name. Columns: metric, job, instance, labels `MAP`. "+
				"Use it for cardinality audits (GROUP BY metric / labels['x']) without fetching samples.",
			"## series(match, start_date := NULL, end_date := NULL)\n\nEvery series matching a selector, one row per label set.",
			"List the series (label sets) that match a selector in a time window", seriesColumns, seriesExamples),
		fetch: func(ctx context.Context, c *conn, a seriesArgs) ([][]any, error) {
			p := url.Values{"match[]": {a.Match}}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.get(ctx, "series", p, true)
			if err != nil {
				return nil, err
			}
			var sets []map[string]string
			if err := json.Unmarshal(data, &sets); err != nil {
				return nil, fmt.Errorf("decoding series: %w", err)
			}
			rows := make([][]any, len(sets))
			for i, s := range sets {
				rows[i] = hoisted(s)
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- label_names

type labelNamesArgs struct {
	Match      *string    `vgi:"name=match,doc=Optional series selector restricting which series' labels are listed."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a labelNamesArgs) secret() string { return deref(a.SecretName) }

var labelNamesColumns = []arrow.Field{column("label", str, "A label name present on at least one matching series.", false)}

var labelNamesExamples = []vgi.CatalogExample{
	{SQL: "SELECT label FROM prometheus.main.label_names(match := 'fixture_temperature_celsius', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00') ORDER BY label",
		Description: "Which labels the temperature metric carries"},
	{SQL: "SELECT count(*) AS label_names FROM prometheus.main.label_names()",
		Description: "How many distinct label names the server knows"},
}

func newLabelNames() vgi.TableFunction {
	return newSimple(&simpleFunc[labelNamesArgs]{
		name:    "label_names",
		columns: labelNamesColumns,
		meta: meta("Label Names", "metadata", "labels, label names, /api/v1/labels, schema discovery",
			"List label names (/api/v1/labels). Named args: match (optional selector), start_date, end_date, secret_name. One column: label.",
			"## label_names(match := NULL, start_date := NULL, end_date := NULL)\n\nEvery label name, optionally for the series matching a selector.",
			"List the label names known to the server, optionally for matching series", labelNamesColumns, labelNamesExamples),
		fetch: func(ctx context.Context, c *conn, a labelNamesArgs) ([][]any, error) {
			p := url.Values{}
			if a.Match != nil {
				p.Set("match[]", *a.Match)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.get(ctx, "labels", p, true)
			if err != nil {
				return nil, err
			}
			return stringRows(data)
		},
	})
}

func stringRows(data json.RawMessage) ([][]any, error) {
	var names []string
	if err := json.Unmarshal(data, &names); err != nil {
		return nil, fmt.Errorf("decoding names: %w", err)
	}
	rows := make([][]any, len(names))
	for i, n := range names {
		rows[i] = []any{n}
	}
	return rows, nil
}

// ---------------------------------------------------------------- label_values

type labelValuesArgs struct {
	Label      string     `vgi:"pos=0,doc=Label name, e.g. 'job' or '__name__' (metric names)."`
	Match      *string    `vgi:"name=match,doc=Optional series selector restricting which series' values are listed."`
	StartDate  *time.Time `vgi:"name=start_date,doc=Window start."`
	EndDate    *time.Time `vgi:"name=end_date,doc=Window end."`
	SecretName *string    `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a labelValuesArgs) secret() string { return deref(a.SecretName) }

var labelValuesColumns = []arrow.Field{column("value", str, "One distinct value of the label.", false)}

var labelValuesExamples = []vgi.CatalogExample{
	{SQL: "SELECT value AS metric FROM prometheus.main.label_values('__name__', match := '{job=\"fixture\"}', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00') WHERE value LIKE 'fixture_%' ORDER BY value",
		Description: "Every fixture metric name"},
	{SQL: "SELECT value AS instance FROM prometheus.main.label_values('instance', match := 'fixture_temperature_celsius', start_date := TIMESTAMPTZ '2026-01-01 00:00:00+00') ORDER BY value",
		Description: "The instances reporting temperature"},
}

// escapeLabelName encodes a label name for /label/<name>/values: legacy names
// pass through; others use Prometheus's U__ value escaping.
func escapeLabelName(name string) string {
	if legacyLabelName.MatchString(name) {
		return name
	}
	var b strings.Builder
	b.WriteString("U__")
	for _, r := range name {
		switch {
		case r == '_':
			b.WriteString("__")
		case r < 128 && (r == ':' || (r >= '0' && r <= '9') || (r >= 'a' && r <= 'z') || (r >= 'A' && r <= 'Z')):
			b.WriteRune(r)
		default:
			b.WriteString("_" + strconv.FormatInt(int64(r), 16) + "_")
		}
	}
	return b.String()
}

func newLabelValues() vgi.TableFunction {
	return newSimple(&simpleFunc[labelValuesArgs]{
		name:    "label_values",
		columns: labelValuesColumns,
		validate: func(a labelValuesArgs) error {
			if a.Label == "" {
				return fmt.Errorf("label_values: label must not be empty")
			}
			return nil
		},
		meta: meta("Label Values", "metadata", "label values, metric names, __name__, /api/v1/label/values, enumerate",
			"List the distinct values of one label (/api/v1/label/<name>/values). Positional: label name ('__name__' lists metric names). "+
				"Named: match (optional selector), start_date, end_date, secret_name. One column: value.",
			"## label_values(label, match := NULL, start_date := NULL, end_date := NULL)\n\nDistinct values of a label; `'__name__'` lists metric names.",
			"List the distinct values of one label, optionally for matching series", labelValuesColumns, labelValuesExamples),
		fetch: func(ctx context.Context, c *conn, a labelValuesArgs) ([][]any, error) {
			p := url.Values{}
			if a.Match != nil {
				p.Set("match[]", *a.Match)
			}
			timeRangeParams(p, a.StartDate, a.EndDate)
			data, err := c.get(ctx, "label/"+url.PathEscape(escapeLabelName(a.Label))+"/values", p, false)
			if err != nil {
				return nil, err
			}
			return stringRows(data)
		},
	})
}

// ---------------------------------------------------------------- metric_metadata

type metadataArgs struct {
	Metric     *string `vgi:"name=metric,doc=Only this metric's metadata; default: every metric."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a metadataArgs) secret() string { return deref(a.SecretName) }

var metadataColumns = []arrow.Field{
	column("metric", str, "Metric family name (for counters the name without _total, as exposed in # TYPE).", false),
	column("type", str, "Metric type: counter, gauge, histogram, gaugehistogram, summary, info, stateset or unknown.", false),
	column("help", str, "HELP text; NULL when empty.", true),
	column("unit", str, "UNIT (OpenMetrics); NULL when empty.", true),
}

var metadataExamples = []vgi.CatalogExample{
	{SQL: "SELECT metric, type, help FROM prometheus.main.metric_metadata() WHERE metric LIKE 'fixture_%' ORDER BY metric",
		Description: "Type and help text of every fixture metric"},
	{SQL: "SELECT type, count(*) AS metrics FROM prometheus.main.metric_metadata() GROUP BY type ORDER BY metrics DESC, type",
		Description: "How many metrics of each type the server has seen"},
}

func newMetadata() vgi.TableFunction {
	return newSimple(&simpleFunc[metadataArgs]{
		name:    "metric_metadata",
		columns: metadataColumns,
		meta: meta("Metric Metadata", "metadata", "metadata, metric type, help, unit, counter, gauge, histogram, /api/v1/metadata",
			"Metric metadata from scrapes (/api/v1/metadata): one row per metric per distinct (type, help, unit). Named: metric (only one metric), secret_name. "+
				"Columns: metric, type, help, unit. Backfilled or remote-written data may have no metadata.",
			"## metric_metadata(metric := NULL)\n\nType, help and unit of each scraped metric.",
			"List each metric's type, help text and unit", metadataColumns, metadataExamples),
		fetch: func(ctx context.Context, c *conn, a metadataArgs) ([][]any, error) {
			p := url.Values{}
			if a.Metric != nil {
				p.Set("metric", *a.Metric)
			}
			data, err := c.get(ctx, "metadata", p, false)
			if err != nil {
				return nil, err
			}
			var m map[string][]struct {
				Type string `json:"type"`
				Help string `json:"help"`
				Unit string `json:"unit"`
			}
			if err := json.Unmarshal(data, &m); err != nil {
				return nil, fmt.Errorf("decoding metadata: %w", err)
			}
			names := make([]string, 0, len(m))
			for k := range m {
				names = append(names, k)
			}
			sort.Strings(names)
			var rows [][]any
			for _, n := range names {
				for _, e := range m[n] {
					rows = append(rows, []any{n, e.Type, optString(e.Help), optString(e.Unit)})
				}
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- targets

type targetsArgs struct {
	State      string  `vgi:"name=state,default=any,choices='any,active,dropped',doc=Which targets: 'active' (being scraped), 'dropped' (removed by relabelling) or 'any'."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a targetsArgs) secret() string { return deref(a.SecretName) }

var targetsColumns = []arrow.Field{
	column("state", str, "'active' or 'dropped'.", false),
	column("scrape_pool", str, "Scrape pool (usually the job_name of the scrape config).", true),
	column("scrape_url", str, "URL scraped; NULL for dropped targets.", true),
	column("global_url", str, "Scrape URL with the server's external hostname.", true),
	column("health", str, "'up', 'down' or 'unknown'; NULL for dropped targets.", true),
	column("last_error", str, "Last scrape error; NULL when the last scrape succeeded.", true),
	column("last_scrape", tsType, "Time of the last scrape; NULL if never scraped.", true),
	column("last_scrape_duration_seconds", arrow.PrimitiveTypes.Float64, "Duration of the last scrape in seconds.", true),
	column("scrape_interval", str, "Configured scrape interval, e.g. '15s'.", true),
	column("scrape_timeout", str, "Configured scrape timeout, e.g. '10s'.", true),
	column("job", str, "The target's job label.", true),
	column("instance", str, "The target's instance label.", true),
	column("labels", labelsType, "Target labels after relabelling (empty for dropped targets).", false),
	column("discovered_labels", labelsType, "Labels from service discovery, before relabelling.", false),
}

var targetsExamples = []vgi.CatalogExample{
	{SQL: "SELECT scrape_pool, health, count(*) AS targets FROM prometheus.main.targets(state := 'active') GROUP BY ALL ORDER BY ALL",
		Description: "Target health per scrape pool"},
	{SQL: "SELECT job, instance, last_error FROM prometheus.main.targets() WHERE health = 'down' ORDER BY job, instance",
		Description: "Down targets and why"},
}

func newTargets() vgi.TableFunction {
	return newSimple(&simpleFunc[targetsArgs]{
		name:    "targets",
		columns: targetsColumns,
		meta: meta("Scrape Targets", "status", "targets, scrape, health, up, down, service discovery, /api/v1/targets",
			"Scrape targets (/api/v1/targets) with health, last error, last scrape time and labels. Named: state ('any' default, 'active', 'dropped'), secret_name. "+
				"Thanos/Mimir queriers do not serve this endpoint; point the secret at a Prometheus or agent.",
			"## targets(state := 'any')\n\nEvery active and dropped scrape target with its health and labels.",
			"List scrape targets with health, last scrape and labels", targetsColumns, targetsExamples),
		fetch: func(ctx context.Context, c *conn, a targetsArgs) ([][]any, error) {
			p := url.Values{}
			if a.State != "any" {
				p.Set("state", a.State)
			}
			data, err := c.get(ctx, "targets", p, false)
			if err != nil {
				return nil, err
			}
			type target struct {
				DiscoveredLabels   map[string]string `json:"discoveredLabels"`
				Labels             map[string]string `json:"labels"`
				ScrapePool         string            `json:"scrapePool"`
				ScrapeURL          string            `json:"scrapeUrl"`
				GlobalURL          string            `json:"globalUrl"`
				LastError          string            `json:"lastError"`
				LastScrape         string            `json:"lastScrape"`
				LastScrapeDuration *float64          `json:"lastScrapeDuration"`
				Health             string            `json:"health"`
				ScrapeInterval     string            `json:"scrapeInterval"`
				ScrapeTimeout      string            `json:"scrapeTimeout"`
			}
			var resp struct {
				Active  []target `json:"activeTargets"`
				Dropped []target `json:"droppedTargets"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding targets: %w", err)
			}
			var rows [][]any
			add := func(state string, t target) {
				labels := t.Labels
				if labels == nil {
					labels = map[string]string{}
				}
				disc := t.DiscoveredLabels
				if disc == nil {
					disc = map[string]string{}
				}
				var dur any
				if t.LastScrapeDuration != nil && state == "active" {
					dur = *t.LastScrapeDuration
				}
				pool := t.ScrapePool
				if pool == "" {
					pool = disc["job"]
				}
				rows = append(rows, []any{state, optString(pool), optString(t.ScrapeURL), optString(t.GlobalURL),
					optString(t.Health), optString(t.LastError), optTime(t.LastScrape), dur,
					optString(t.ScrapeInterval), optString(t.ScrapeTimeout), optString(labels["job"]), optString(labels["instance"]), labels, disc})
			}
			for _, t := range resp.Active {
				add("active", t)
			}
			for _, t := range resp.Dropped {
				add("dropped", t)
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- rules

type rulesArgs struct {
	RuleType   string  `vgi:"name=rule_type,default=any,choices='any,alert,record',doc=Which rules: 'alert', 'record' or 'any'."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a rulesArgs) secret() string { return deref(a.SecretName) }

var f64 = arrow.PrimitiveTypes.Float64

var rulesColumns = []arrow.Field{
	column("group_name", str, "Rule group name.", false),
	column("group_file", str, "File the group was loaded from.", true),
	column("group_interval_seconds", f64, "Group evaluation interval in seconds.", true),
	column("rule_name", str, "Alert name or recorded metric name.", false),
	column("rule_type", str, "'alerting' or 'recording'.", false),
	column("query", str, "The rule's PromQL expression.", false),
	column("duration_seconds", f64, "Alerting rules: the 'for' duration in seconds; NULL for recording rules.", true),
	column("keep_firing_for_seconds", f64, "Alerting rules: keep_firing_for in seconds; NULL for recording rules.", true),
	column("labels", labelsType, "Labels the rule adds.", false),
	column("annotations", labelsType, "Alerting rules: annotations; empty for recording rules.", false),
	column("health", str, "'ok', 'err' or 'unknown'.", true),
	column("last_error", str, "Last evaluation error; NULL when none.", true),
	column("state", str, "Alerting rules: 'firing', 'pending' or 'inactive'; NULL for recording rules.", true),
	column("active_alerts", arrow.PrimitiveTypes.Int64, "Alerting rules: number of active (pending or firing) alerts; NULL for recording rules.", true),
	column("last_evaluation", tsType, "Time of the rule's last evaluation; NULL if never evaluated.", true),
	column("evaluation_time_seconds", f64, "Duration of the last evaluation in seconds.", true),
}

var rulesExamples = []vgi.CatalogExample{
	{SQL: "SELECT group_name, rule_name, state, active_alerts FROM prometheus.main.rules(rule_type := 'alert') ORDER BY group_name, rule_name",
		Description: "Every alerting rule with its state"},
	{SQL: "SELECT rule_type, health, count(*) AS rules FROM prometheus.main.rules() GROUP BY ALL ORDER BY ALL",
		Description: "Rule health summary"},
}

func newRules() vgi.TableFunction {
	return newSimple(&simpleFunc[rulesArgs]{
		name:    "rules",
		columns: rulesColumns,
		meta: meta("Recording And Alerting Rules", "status", "rules, alerting rules, recording rules, rule groups, health, /api/v1/rules",
			"Recording and alerting rules (/api/v1/rules), one row per rule with its group, PromQL query, for-duration, labels, annotations, health, "+
				"last error, state and active alert count. Named: rule_type ('any', 'alert', 'record'), secret_name. Mimir/Cortex serve this from the ruler.",
			"## rules(rule_type := 'any')\n\nEvery rule in every group with health and evaluation stats.",
			"List recording and alerting rules with their health and state", rulesColumns, rulesExamples),
		fetch: func(ctx context.Context, c *conn, a rulesArgs) ([][]any, error) {
			p := url.Values{}
			if a.RuleType != "any" {
				p.Set("type", a.RuleType)
			}
			data, err := c.get(ctx, "rules", p, false)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Groups []struct {
					Name     string   `json:"name"`
					File     string   `json:"file"`
					Interval *float64 `json:"interval"`
					Rules    []struct {
						Name           string            `json:"name"`
						Query          string            `json:"query"`
						Type           string            `json:"type"`
						Duration       *float64          `json:"duration"`
						KeepFiringFor  *float64          `json:"keepFiringFor"`
						Labels         map[string]string `json:"labels"`
						Annotations    map[string]string `json:"annotations"`
						Health         string            `json:"health"`
						LastError      string            `json:"lastError"`
						State          string            `json:"state"`
						Alerts         []json.RawMessage `json:"alerts"`
						LastEvaluation string            `json:"lastEvaluation"`
						EvaluationTime *float64          `json:"evaluationTime"`
					} `json:"rules"`
				} `json:"groups"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding rules: %w", err)
			}
			var rows [][]any
			for _, g := range resp.Groups {
				for _, r := range g.Rules {
					labels, ann := r.Labels, r.Annotations
					if labels == nil {
						labels = map[string]string{}
					}
					if ann == nil {
						ann = map[string]string{}
					}
					var dur, keep, state, active any
					if r.Type == "alerting" {
						dur, keep, state, active = ptrFloat(r.Duration, 0), ptrFloat(r.KeepFiringFor, 0), optString(r.State), int64(len(r.Alerts))
					}
					rows = append(rows, []any{g.Name, optString(g.File), ptrFloat(g.Interval, nil), r.Name, r.Type, r.Query,
						dur, keep, labels, ann, optString(r.Health), optString(r.LastError), state, active,
						optTime(r.LastEvaluation), ptrFloat(r.EvaluationTime, nil)})
				}
			}
			return rows, nil
		},
	})
}

func ptrFloat(p *float64, missing any) any {
	if p == nil {
		return missing
	}
	return *p
}

// ---------------------------------------------------------------- alerts

type noArgs struct {
	SecretName *string `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a noArgs) secret() string { return deref(a.SecretName) }

var alertsColumns = []arrow.Field{
	column("alert_name", str, "The alertname label.", true),
	column("state", str, "'pending' or 'firing'.", false),
	column("active_at", tsType, "When the alert became active (pending).", true),
	column("value", f64, "Value of the alert expression when last evaluated.", true),
	column("labels", labelsType, "Alert labels.", false),
	column("annotations", labelsType, "Alert annotations.", false),
	column("keep_firing_since", tsType, "When keep_firing_for started holding the alert; NULL otherwise.", true),
}

var alertsExamples = []vgi.CatalogExample{
	{SQL: "SELECT alert_name, state, labels['severity'] AS severity FROM prometheus.main.alerts() ORDER BY alert_name",
		Description: "Active alerts with severity"},
	{SQL: "SELECT labels['severity'] AS severity, count(*) AS alerts FROM prometheus.main.alerts() WHERE state = 'firing' GROUP BY severity ORDER BY alerts DESC",
		Description: "Firing alerts per severity"},
}

func newAlerts() vgi.TableFunction {
	return newSimple(&simpleFunc[noArgs]{
		name:    "alerts",
		columns: alertsColumns,
		meta: meta("Active Alerts", "status", "alerts, firing, pending, alerting, /api/v1/alerts, incidents",
			"Active (pending or firing) alerts (/api/v1/alerts) with labels, annotations, active_at and value. Named: secret_name. "+
				"Join rules() on alert_name = rule_name for the rule definition.",
			"## alerts()\n\nEvery pending and firing alert.",
			"List the currently pending and firing alerts", alertsColumns, alertsExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs) ([][]any, error) {
			data, err := c.get(ctx, "alerts", url.Values{}, false)
			if err != nil {
				return nil, err
			}
			var resp struct {
				Alerts []struct {
					Labels          map[string]string `json:"labels"`
					Annotations     map[string]string `json:"annotations"`
					State           string            `json:"state"`
					ActiveAt        string            `json:"activeAt"`
					KeepFiringSince string            `json:"keepFiringSince"`
					Value           string            `json:"value"`
				} `json:"alerts"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding alerts: %w", err)
			}
			rows := make([][]any, 0, len(resp.Alerts))
			for _, a := range resp.Alerts {
				labels, ann := a.Labels, a.Annotations
				if labels == nil {
					labels = map[string]string{}
				}
				if ann == nil {
					ann = map[string]string{}
				}
				var v any
				if f, err := strconv.ParseFloat(a.Value, 64); err == nil {
					v = f
				}
				rows = append(rows, []any{optString(labels["alertname"]), a.State, optTime(a.ActiveAt), v, labels, ann, optTime(a.KeepFiringSince)})
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- alertmanagers

var alertmanagersColumns = []arrow.Field{
	column("url", str, "Alertmanager API URL alerts are sent to.", false),
	column("state", str, "'active' or 'dropped'.", false),
}

var alertmanagersExamples = []vgi.CatalogExample{
	{SQL: "SELECT state, count(*) AS alertmanagers FROM prometheus.main.alertmanagers() GROUP BY state ORDER BY state",
		Description: "How many Alertmanagers are active and dropped"},
	{SQL: "SELECT url FROM prometheus.main.alertmanagers() WHERE state = 'active' ORDER BY url",
		Description: "Where alerts are being sent"},
}

func newAlertmanagers() vgi.TableFunction {
	return newSimple(&simpleFunc[noArgs]{
		name:    "alertmanagers",
		columns: alertmanagersColumns,
		meta: meta("Alertmanager Discovery", "status", "alertmanager, alerting, notification, /api/v1/alertmanagers",
			"Alertmanagers the server sends alerts to (/api/v1/alertmanagers): url and state ('active'/'dropped'). Named: secret_name.",
			"## alertmanagers()\n\nActive and dropped Alertmanager endpoints.",
			"List the Alertmanagers the server discovered", alertmanagersColumns, alertmanagersExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs) ([][]any, error) {
			data, err := c.get(ctx, "alertmanagers", url.Values{}, false)
			if err != nil {
				return nil, err
			}
			type am struct {
				URL string `json:"url"`
			}
			var resp struct {
				Active  []am `json:"activeAlertmanagers"`
				Dropped []am `json:"droppedAlertmanagers"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding alertmanagers: %w", err)
			}
			var rows [][]any
			for _, a := range resp.Active {
				rows = append(rows, []any{a.URL, "active"})
			}
			for _, a := range resp.Dropped {
				rows = append(rows, []any{a.URL, "dropped"})
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- tsdb_status

type tsdbArgs struct {
	TopN       int64   `vgi:"name=top_n,default=10,ge=1,le=10000,doc=Entries per top-N list (the API's limit parameter)."`
	SecretName *string `vgi:"name=secret_name,doc=Name of the prometheus secret to use when several exist."`
}

func (a tsdbArgs) secret() string { return deref(a.SecretName) }

var tsdbColumns = []arrow.Field{
	column("section", str, "'head' (head-block totals), 'series_count_by_metric_name', 'label_value_count_by_label_name', 'memory_in_bytes_by_label_name' or 'series_count_by_label_value_pair'.", false),
	column("name", str, "Statistic or entry name (for head: num_series, num_label_pairs, chunk_count, min_time, max_time).", false),
	column("value", arrow.PrimitiveTypes.Int64, "Count, bytes, or for head min_time/max_time Unix milliseconds.", false),
}

var tsdbExamples = []vgi.CatalogExample{
	{SQL: "SELECT name AS metric, value AS series FROM prometheus.main.tsdb_status(top_n := 5) WHERE section = 'series_count_by_metric_name' ORDER BY series DESC, metric",
		Description: "The five metrics with the most head series"},
	{SQL: "SELECT name, value FROM prometheus.main.tsdb_status() WHERE section = 'head' ORDER BY name",
		Description: "Head-block totals"},
}

func newTSDBStatus() vgi.TableFunction {
	return newSimple(&simpleFunc[tsdbArgs]{
		name:    "tsdb_status",
		columns: tsdbColumns,
		meta: meta("TSDB Cardinality Status", "status", "tsdb, cardinality, head, series count, memory, /api/v1/status/tsdb",
			"Head-block cardinality statistics (/api/v1/status/tsdb) in long format: section, name, value. Named: top_n (default 10), secret_name. "+
				"Sections: head, series_count_by_metric_name, label_value_count_by_label_name, memory_in_bytes_by_label_name, series_count_by_label_value_pair.",
			"## tsdb_status(top_n := 10)\n\nHead cardinality: totals and top-N metrics/labels.",
			"Head-block cardinality statistics and top-N lists", tsdbColumns, tsdbExamples),
		fetch: func(ctx context.Context, c *conn, a tsdbArgs) ([][]any, error) {
			data, err := c.get(ctx, "status/tsdb", url.Values{"limit": {strconv.FormatInt(a.TopN, 10)}}, false)
			if err != nil {
				return nil, err
			}
			type entry struct {
				Name  string `json:"name"`
				Value int64  `json:"value"`
			}
			var resp struct {
				Head struct {
					NumSeries     int64 `json:"numSeries"`
					NumLabelPairs int64 `json:"numLabelPairs"`
					ChunkCount    int64 `json:"chunkCount"`
					MinTime       int64 `json:"minTime"`
					MaxTime       int64 `json:"maxTime"`
				} `json:"headStats"`
				ByMetric  []entry `json:"seriesCountByMetricName"`
				ByLabel   []entry `json:"labelValueCountByLabelName"`
				Memory    []entry `json:"memoryInBytesByLabelName"`
				ByLVPairs []entry `json:"seriesCountByLabelValuePair"`
			}
			if err := json.Unmarshal(data, &resp); err != nil {
				return nil, fmt.Errorf("decoding tsdb status: %w", err)
			}
			h := resp.Head
			rows := [][]any{
				{"head", "num_series", h.NumSeries}, {"head", "num_label_pairs", h.NumLabelPairs},
				{"head", "chunk_count", h.ChunkCount}, {"head", "min_time", h.MinTime}, {"head", "max_time", h.MaxTime},
			}
			for _, s := range []struct {
				name string
				es   []entry
			}{
				{"series_count_by_metric_name", resp.ByMetric}, {"label_value_count_by_label_name", resp.ByLabel},
				{"memory_in_bytes_by_label_name", resp.Memory}, {"series_count_by_label_value_pair", resp.ByLVPairs},
			} {
				for _, e := range s.es {
					rows = append(rows, []any{s.name, e.Name, e.Value})
				}
			}
			return rows, nil
		},
	})
}

// ---------------------------------------------------------------- build_info

var buildInfoColumns = []arrow.Field{
	column("version", str, "Server version, e.g. '3.5.0'.", true),
	column("revision", str, "Source revision.", true),
	column("branch", str, "Source branch.", true),
	column("build_user", str, "Who built it.", true),
	column("build_date", str, "Build date as reported, e.g. '20250714-16:18:17'.", true),
	column("go_version", str, "Go toolchain version.", true),
}

var buildInfoExamples = []vgi.CatalogExample{
	{SQL: "SELECT version, go_version FROM prometheus.main.build_info()",
		Description: "Which Prometheus version the secret points at"},
	{SQL: "SELECT version LIKE '3.%' AS is_v3 FROM prometheus.main.build_info()",
		Description: "Check the server is Prometheus 3.x"},
}

func newBuildInfo() vgi.TableFunction {
	return newSimple(&simpleFunc[noArgs]{
		name:    "build_info",
		columns: buildInfoColumns,
		meta: meta("Server Build Info", "status", "build info, version, revision, /api/v1/status/buildinfo, connectivity check",
			"The server's build information (/api/v1/status/buildinfo): version, revision, branch, build_user, build_date, go_version. One row. "+
				"Use it as a connectivity check for a new prometheus secret. Named: secret_name.",
			"## build_info()\n\nVersion and build details of the server; a quick connectivity check.",
			"Version and build details of the server", buildInfoColumns, buildInfoExamples),
		fetch: func(ctx context.Context, c *conn, _ noArgs) ([][]any, error) {
			data, err := c.get(ctx, "status/buildinfo", url.Values{}, false)
			if err != nil {
				return nil, err
			}
			var b struct {
				Version   string `json:"version"`
				Revision  string `json:"revision"`
				Branch    string `json:"branch"`
				BuildUser string `json:"buildUser"`
				BuildDate string `json:"buildDate"`
				GoVersion string `json:"goVersion"`
			}
			if err := json.Unmarshal(data, &b); err != nil {
				return nil, fmt.Errorf("decoding build info: %w", err)
			}
			return [][]any{{optString(b.Version), optString(b.Revision), optString(b.Branch), optString(b.BuildUser), optString(b.BuildDate), optString(b.GoVersion)}}, nil
		},
	})
}
