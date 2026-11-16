<p align="center">
  <img src="https://query.farm/media-kit/products/vgi/vgi-logo.png" alt="Vector Gateway Interface (VGI)" width="320">
</p>

# Query Prometheus Metrics With PromQL and SQL in DuckDB

> **vgi-prometheus** · a [Query.Farm](https://query.farm) VGI worker

[![CI](https://github.com/Query-farm/vgi-prometheus/actions/workflows/ci.yml/badge.svg)](https://github.com/Query-farm/vgi-prometheus/actions/workflows/ci.yml)
[![vgi worker: query.farm](https://query.farm/media-kit/shields/vgi-worker.svg)](https://query.farm)

Prometheus answers PromQL, but the answer is a JSON tree that dashboards draw and nothing else
can join. This worker turns any **Prometheus-compatible HTTP API** into SQL tables in DuckDB:
run PromQL and get typed rows, filter stored samples with a `WHERE` clause that becomes a
PromQL selector, audit cardinality, targets, rules and alerts, and join it all with your
Parquet, CSV or warehouse tables.

It works with **Prometheus**, **Thanos**, **Cortex / Mimir**, **VictoriaMetrics**,
**Grafana Cloud** (basic auth) and anything that speaks `/api/v1` behind a bearer token
(for example a SigV4 proxy in front of Amazon Managed Prometheus). It is **read-only**: no
admin endpoints, no deletes, no writes.

- `query` / `query_range` run arbitrary PromQL and return long rows: hoisted `metric`, `job`,
  `instance`, every label in a `MAP(VARCHAR, VARCHAR)`, `ts TIMESTAMPTZ`, `value DOUBLE`
  (NaN and ±Inf kept), native histograms as a typed struct. Long ranges are split under the
  server's 11,000-points-per-series limit; the step you asked for is never changed.
- `samples` returns one metric's samples with SQL label and time filters **pushed into the
  PromQL selector and the request window**, then re-applied exactly.
- Five scalars run Prometheus's own PromQL parser locally: lint, explain, canonicalize and
  audit PromQL with no server.

## Quick start

```sql
INSTALL vgi FROM community;     -- Haybarn 2.0; on DuckDB 2.0 use the query_farm repository
LOAD vgi;
ATTACH 'prometheus' (TYPE vgi, LOCATION 'github-auto://Query-farm/vgi-prometheus@v0.1.2');

CREATE SECRET (TYPE prometheus, BASE_URL 'http://localhost:9090');

-- Request rate per job over the last hour, one row per job per minute.
SELECT labels['job'] AS job, ts, value AS rps
FROM prometheus.main.query_range('sum by (job) (rate(http_requests_total[5m]))',
                                 now() - INTERVAL 1 HOUR, now(), 60)
ORDER BY job, ts;
```

The secret type is registered by the worker, so run `CREATE SECRET` after `ATTACH`.

<details><summary>Other ways to run it</summary>

- **DuckDB 2.0**:
  `SET allow_extension_repositories = 'allowed'; CREATE EXTENSION REPOSITORY IF NOT EXISTS query_farm WITH PREFIX 'https://duckdb-extensions.query.farm'; INSTALL vgi FROM query_farm; LOAD vgi FROM query_farm;`
  then attach as above.
- **Docker** (HTTP): `docker run -p 8000:8000 ghcr.io/query-farm/vgi-prometheus:0.1.2`, then
  `ATTACH 'prometheus' (TYPE vgi, LOCATION 'http://localhost:8000')`.
- **Go**: `go install github.com/Query-farm/vgi-prometheus/cmd/vgi-prometheus-worker@v0.1.2`,
  then attach the binary by absolute path.
- **From source**: `make build` (Go 1.26+, pure Go, no cgo), then
  `ATTACH 'prometheus' (TYPE vgi, LOCATION '/abs/path/to/bin/vgi-prometheus-worker')`.

</details>

## Secrets and configuration

Every function that calls a server reads a DuckDB secret of type `prometheus`:

| Field | Meaning |
|---|---|
| `BASE_URL` | Server root; `/api/v1/...` is appended. `http://prometheus:9090`, `https://prometheus-prod-01-eu-west-0.grafana.net/api/prom`, `http://mimir:8080/prometheus`, `http://victoria:8428`. |
| `BEARER_TOKEN` | Sent as `Authorization: Bearer …`. Redacted. |
| `USERNAME` / `PASSWORD` | Basic auth (Grafana Cloud: instance id and API key). Password redacted. |
| `TENANT_ID` | Sent as `X-Scope-OrgID` (Mimir, Cortex, Thanos receive multi-tenancy). |
| `EXTRA_HEADERS` | JSON object of extra request headers, e.g. `'{"X-Custom": "v"}'`. Redacted. |

```sql
-- Grafana Cloud
CREATE SECRET grafana (TYPE prometheus, BASE_URL 'https://prometheus-prod-01-eu-west-0.grafana.net/api/prom',
                       USERNAME '123456', PASSWORD 'glc_...');
-- Mimir, one tenant
CREATE SECRET mimir (TYPE prometheus, BASE_URL 'http://mimir:8080/prometheus', TENANT_ID 'team-a');

SELECT * FROM prometheus.main.build_info(secret_name := 'mimir');
```

With several secrets, pick one per call with `secret_name := '...'`. Credentials never enter
table-function state or logs. Requests that get 429, 502, 503 or 504 are retried with
exponential backoff, honouring `Retry-After`; other errors carry the HTTP status and the
server's `errorType` and message.

## What's inside

All objects are in `prometheus.main`.

| Function | What it returns |
|---|---|
| `query(promql, time := …)` | Instant query (`/api/v1/query`). `result_type`, `metric`, `job`, `instance`, `labels`, `ts`, `value`, `histogram`, `string_value`. `time` omitted = now. |
| `query_range(promql, start, end, step_seconds, max_points_per_chunk := 11000)` | Range query, same columns, one row per series per step, chunked. |
| `samples(metric, label_columns := '', step_seconds := 0, start_date, end_date, chunk_seconds := 3600)` | Typed samples with pushdown: `metric`, `job`, `instance`, one column per `label_columns` entry, `labels`, `ts`, `value`, `histogram`. |
| `series(match, start_date, end_date)` | Every label set matching a selector (`/api/v1/series`). |
| `label_names(match := …, start_date, end_date)` | Label names (`/api/v1/labels`). |
| `label_values(label, match := …, start_date, end_date)` | Values of one label (`/api/v1/label/<name>/values`, UTF-8 names escaped). |
| `metric_metadata(metric := …)` | Type, help and unit per metric family (`/api/v1/metadata`). |
| `targets(state := 'any')` | Scrape targets: health, last error, last scrape, labels, discovered labels. |
| `rules(rule_type := 'any')` | Alerting and recording rules with health, state, `for` and evaluation timings. |
| `alerts()` | Active (pending or firing) alerts with labels and annotations. |
| `alertmanagers()` | Active and dropped Alertmanager URLs. |
| `tsdb_status(top_n := 10)` | Head-block stats and the top-N cardinality lists, in long form (`section`, `name`, `value`). |
| `build_info()` | Server version, revision, Go version. |
| `is_valid(expr)` | `BOOLEAN`: does the PromQL parse? |
| `parse_error(expr)` | The parser's `line:col: message`, or `NULL`. |
| `format_expr(expr)` | Canonical single-line PromQL, or `NULL` when it does not parse. |
| `metric_names(expr)` | `VARCHAR[]` of metric names the expression reads. |
| `duration_seconds(d)` | `'1h30m'` → `5400.0`; `NULL` when invalid. |

**Native histograms** (`histogram` column, `value` is `NULL` on those rows):
`STRUCT(count DOUBLE, sum DOUBLE, buckets STRUCT(lower DOUBLE, upper DOUBLE, count DOUBLE,
lower_inclusive BOOLEAN, upper_inclusive BOOLEAN)[])`, decoded from the API's bucket boundary
rule (0 = `(lower, upper]`, 1 = `[lower, upper)`, 2 = `(lower, upper)`, 3 = `[lower, upper]`). A
series that switches between float and histogram samples gives rows of both kinds in time
order. Classic histograms stay ordinary `_bucket` / `_sum` / `_count` float series.

**Scalars and strings**: a scalar result is one row with `metric`/`labels` empty and `value`
set; a string result sets `string_value`.

## What pushes down

`samples` is the pushdown surface. Its `WHERE` clause compiles into the request:

| SQL on `samples(...)` | Becomes |
|---|---|
| `job = 'api'`, `env <> 'qa'` (any label column) | `job="api"`, `env!="qa"` |
| `instance IN ('a', 'b')`, `instance = 'a' OR instance = 'b'` | `instance=~"a|b"` |
| `col LIKE 'p%'`, `prefix(col, 'p')`, `suffix(col, 's')`, `contains(col, 'x')`, `col LIKE '%x%'` | `col=~"…"` |
| `col NOT LIKE '…'` | `col!~"…"` |
| `col IS NULL` / `col IS NOT NULL` | `col=""` / `col!=""` |
| `ts >= …`, `ts > …`, `ts < …`, `ts <= …`, `ts BETWEEN … AND …` | the request window |

Everything else (`value` comparisons, `labels['x']` lookups) runs in the worker or DuckDB, and
every pushed predicate is re-applied exactly, so results always match the SQL. DuckDB 2.0 does
not push `regexp_matches` or `ILIKE` to table functions: write the regex into the selector
argument instead, e.g. `samples('up{instance=~"node-[ab].*"}')`. To push a label other than
`metric`/`job`/`instance`, list it in `label_columns := 'env,region'`.

Modes:

- `step_seconds := 0` (default): **raw stored samples**. The window is read in
  `chunk_seconds` pieces with instant queries of `selector[<n>ms]`, which are left-open, so
  chunks never overlap or skip a sample.
- `step_seconds > 0`: the selector is evaluated at that fixed step with `query_range`, chunked
  under `max_points_per_chunk` (≤ 11,000).

A lower time bound is required (`WHERE ts >= …` or `start_date :=`); the upper bound defaults to
now. The scan cursor (next chunk) is part of the table-function state, so over the HTTP
transport each chunk can be served by a different request.

`query_range` chunks the same way: `(end - start) / step + 1` points are split into requests of
at most `max_points_per_chunk` steps, with the cursor externalized.

## Examples

```sql
-- Which jobs have targets down right now, and why?
SELECT job, instance, last_error FROM prometheus.main.targets() WHERE health = 'down';

-- Top 10 metrics by series count.
SELECT name AS metric, value AS series
FROM prometheus.main.tsdb_status(top_n := 10) WHERE section = 'series_count_by_metric_name'
ORDER BY series DESC;

-- Raw samples for two nodes, filters pushed into the selector.
SELECT instance, count(*), avg(value)
FROM prometheus.main.samples('node_load1')
WHERE job = 'node' AND instance IN ('a:9100', 'b:9100') AND ts >= now() - INTERVAL 1 DAY
GROUP BY instance;

-- Which metrics do the most alert rules depend on, and are all rules still valid PromQL?
SELECT m AS metric, count(*) AS rules
FROM prometheus.main.rules(), unnest(prometheus.main.metric_names(query)) u(m)
GROUP BY m ORDER BY rules DESC;
SELECT rule_name, prometheus.main.parse_error(query) FROM prometheus.main.rules()
WHERE NOT prometheus.main.is_valid(query);

-- Export a day of samples to Parquet.
COPY (SELECT * FROM prometheus.main.query_range('up', now() - INTERVAL 1 DAY, now(), 60))
TO 'up.parquet';
```

## How it is tested

`make check` runs gofmt, go vet, golangci-lint, `go test`, the SQL suite
(`test/sql/prometheus.test`, 80+ statements) over the stdio, launch and HTTP transports,
and vgi-lint at `fail_on = "info"`.

**CI needs no Docker and no credentials.** The SQL suite talks to
`test/fixture_server/server.py`, which replays **recorded responses** from
`test/fixture_server/recorded.json` (keyed by path and sorted query/form parameters) plus a few
hand-written ones in `handwritten.json` for shapes a stock server will not produce on demand
(errors 422/502/503, warnings, mixed float/native-histogram series). The runner starts two
copies: one answers every request with `429 + Retry-After: 0` once (so retries are always
exercised), the other demands a bearer token or basic auth plus `X-Scope-OrgID: tenant-a`.

**The recordings come from a real Prometheus.** `test/fixture/run-prometheus.sh start` runs
`prom/prometheus:v3.5.0` (about 125 MB RAM, capped at 512 MB) with deterministic data backfilled by
`promtool tsdb create-blocks-from openmetrics` from `test/fixture/gen_openmetrics.py`:
2026-01-01 00:00–06:00 UTC at a 15 s interval, gauges, counters with a reset, a classic
histogram, NaN/±Inf, a series with a one-hour gap and an escaped label value. Prometheus also
scrapes itself (native histograms on), and loads `rules.yml` so rules and alerts are non-empty.
To re-record after changing the tests:

```bash
test/fixture/run-prometheus.sh start            # prints http://127.0.0.1:19090
rm test/fixture_server/recorded.json
PROMETHEUS_RECORD_UPSTREAM=http://127.0.0.1:19090 make test-sql
test/fixture/run-prometheus.sh stop
```

Point the worker at either fixture through the secret's `BASE_URL`; there are no test-only
code paths.

## Known limitations

- **Time zone.** vgi-go 0.42.0 cannot decode a pushed `TIMESTAMPTZ` filter in a session whose
  `TimeZone` is not UTC. Use `SET TimeZone = 'UTC'` with `WHERE ts …` on `samples`, or pass
  `start_date :=` / `end_date :=`, which work in any time zone.
- `regexp_matches` / `ILIKE` are not pushed by DuckDB (see above).
- `query` and `query_range` do not parse PromQL to push SQL filters; filter in the PromQL.
- No remote-read, exemplars, or TSDB-block (offline) reading yet; no TLS client certificates
  (put a proxy in front, or use a bearer token).
- `labels` is a `MAP`; read a label with `labels['name']` (missing key gives `NULL`).

## Development

See [AGENTS.md](AGENTS.md) for the layout, the recipe for adding a function and the sharp
edges. `make help` lists the targets; `make template-diff` shows upstream starter changes
(this worker started from vgi-starter-go v1.3.0).

## Support

Open an issue on this repository. For VGI itself, see [query.farm](https://query.farm).

## Authorship & License

Copyright 2026 Query Farm LLC - https://query.farm

Released under the [BSD Zero Clause License](LICENSE) (0BSD). Prometheus is a Cloud Native
Computing Foundation project; this worker is not affiliated with or endorsed by the CNCF, Grafana
Labs or any vendor named here. The PromQL tools embed the upstream
`github.com/prometheus/prometheus/promql/parser` (Apache-2.0).
