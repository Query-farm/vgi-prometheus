// HTTP client for the Prometheus-compatible query API.
//
// Every function talks to /api/v1/... on the server named by a `prometheus`
// DuckDB secret. The same API is served by Prometheus, Thanos Querier, Mimir,
// Cortex, VictoriaMetrics, Grafana Cloud and Amazon Managed Prometheus (via a
// SigV4 proxy or a token); the secret carries the base URL, a bearer token or
// basic-auth pair, an optional tenant id (sent as X-Scope-OrgID) and extra
// headers. Credentials are read from the secret on every call and never kept
// in table-function state, which travels back to DuckDB over HTTP.

package prometheus

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"net/url"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
)

const (
	// SecretType is the DuckDB secret type every function reads.
	SecretType = "prometheus"
	// MaxAttempts bounds retries of 429/502/503/504 responses and network errors.
	MaxAttempts = 4
	// Timeout bounds every request.
	Timeout = 120 * time.Second
	// maxGetURL is the longest URL sent as GET; longer requests are POSTed as a form.
	maxGetURL = 4000
)

// userAgent identifies the worker to the API.
const userAgent = "vgi-prometheus-worker/" + Version

func secretField(name, doc string, redact bool) arrow.Field {
	keys := []string{"vgi_doc"}
	vals := []string{doc}
	if redact {
		keys = append(keys, "redact")
		vals = append(vals, "true")
	}
	return arrow.Field{Name: name, Type: arrow.BinaryTypes.String, Nullable: true,
		Metadata: arrow.NewMetadata(keys, vals)}
}

// APISecretType is registered with DuckDB so users can run
// CREATE SECRET (TYPE prometheus, BASE_URL '...', BEARER_TOKEN '...').
var APISecretType = vgi.SecretTypeSpec{
	Name:        SecretType,
	Description: "Connection to a Prometheus-compatible HTTP API (Prometheus, Thanos, Mimir, Cortex, VictoriaMetrics, Grafana Cloud, Amazon Managed Prometheus).",
	Schema: arrow.NewSchema([]arrow.Field{
		secretField("base_url", "Server root, e.g. 'http://prometheus:9090' or 'https://prometheus-prod-01.grafana.net/api/prom'. /api/v1/... is appended.", false),
		secretField("bearer_token", "Token sent as 'Authorization: Bearer <token>'.", true),
		secretField("username", "Basic-auth user name (Grafana Cloud: the instance id).", false),
		secretField("password", "Basic-auth password or API key.", true),
		secretField("tenant_id", "Tenant sent as the X-Scope-OrgID header (Mimir, Cortex, Loki-style multi-tenancy).", false),
		secretField("extra_headers", "Additional request headers as a JSON object, e.g. '{\"X-Custom\": \"v\"}'.", true),
	}, &secretSchemaMetadata),
}

// secretSchemaMetadata links the secret type to its documentation.
var secretSchemaMetadata = arrow.NewMetadata(
	[]string{"vgi_doc_url"},
	[]string{SourceURL + "#secrets-and-configuration"})

// secretRequirements is declared by every function: resolve the session's
// prometheus secret(s) and pass them to the worker.
var secretRequirements = []vgi.SecretRequirement{{SecretType: SecretType}}

// bindWithSecret binds a table function's schema. When secret_name names a
// secret, the first bind asks the extension to resolve that secret by name
// (two-phase bind); otherwise only the default secret of the type is sent.
func bindWithSecret[A secretNamed](params *vgi.BindParams, schema *arrow.Schema) (*vgi.BindResponse, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if name := a.secret(); name != "" && !params.ResolvedSecretsProvided {
		return &vgi.BindResponse{SecretScopeRequest: []vgi.SecretLookup{{SecretType: SecretType, SecretName: name}}}, nil
	}
	return vgi.BindSchema(schema)
}

// conn is a resolved connection. It is built per call from the secret and is
// never stored in scan state.
type conn struct {
	BaseURL string
	Headers http.Header
}

// resolveConn picks the secret (the named one when name is non-empty) and
// turns it into a connection.
func resolveConn(secrets vgi.Secrets, name string) (*conn, error) {
	var fields map[string]interface{}
	var ok bool
	if name != "" {
		fields, ok = secrets.LookupNamed(SecretType, name)
		if !ok {
			return nil, fmt.Errorf("no prometheus secret named %q: create it with CREATE SECRET %s (TYPE prometheus, BASE_URL '...')", name, name)
		}
	} else {
		fields, ok = secrets.Lookup(SecretType)
		if !ok {
			return nil, errors.New("no prometheus secret found: create one with CREATE SECRET (TYPE prometheus, BASE_URL 'http://localhost:9090') and add BEARER_TOKEN, USERNAME/PASSWORD or TENANT_ID as your server needs")
		}
	}
	get := func(k string) string { return strings.TrimSpace(vgi.RenderSecretValue(fields[k])) }
	base := strings.TrimRight(get("base_url"), "/")
	if base == "" {
		return nil, errors.New("the prometheus secret has no BASE_URL")
	}
	u, err := url.Parse(base)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") || u.Host == "" {
		return nil, fmt.Errorf("the prometheus secret's BASE_URL must be an http(s) URL, got %q", base)
	}
	h := http.Header{}
	h.Set("Accept", "application/json")
	h.Set("User-Agent", userAgent)
	if extra := get("extra_headers"); extra != "" {
		var m map[string]string
		if err := json.Unmarshal([]byte(extra), &m); err != nil {
			return nil, errors.New("the prometheus secret's EXTRA_HEADERS must be a JSON object of strings, e.g. '{\"X-Custom\": \"value\"}'")
		}
		for k, v := range m {
			h.Set(k, v)
		}
	}
	if t := get("bearer_token"); t != "" {
		h.Set("Authorization", "Bearer "+t)
	} else if user := get("username"); user != "" {
		req := &http.Request{Header: http.Header{}}
		req.SetBasicAuth(user, vgi.RenderSecretValue(fields["password"]))
		h.Set("Authorization", req.Header.Get("Authorization"))
	}
	if t := get("tenant_id"); t != "" {
		h.Set("X-Scope-OrgID", t)
	}
	return &conn{BaseURL: base, Headers: h}, nil
}

// apiError is a failed API call. The message names the HTTP status and the
// server's errorType so users can tell a bad expression from an outage.
type apiError struct {
	Status    int
	ErrorType string
	Message   string
	Path      string
}

func (e *apiError) Error() string {
	msg := fmt.Sprintf("prometheus %s: HTTP %d", e.Path, e.Status)
	if e.ErrorType != "" {
		msg += " " + e.ErrorType
	}
	if e.Message != "" {
		msg += ": " + e.Message
	}
	return msg
}

// envelope is the standard /api/v1 response wrapper.
type envelope struct {
	Status    string          `json:"status"`
	Data      json.RawMessage `json:"data"`
	ErrorType string          `json:"errorType"`
	Error     string          `json:"error"`
	Warnings  []string        `json:"warnings"`
	Infos     []string        `json:"infos"`
}

// client is shared so connections are reused across chunks.
var client = &http.Client{Timeout: Timeout}

// sleep is replaced in tests so retries don't wait.
var sleep = func(ctx context.Context, d time.Duration) {
	select {
	case <-ctx.Done():
	case <-time.After(d):
	}
}

// retryDelay honours a Retry-After header (seconds or HTTP date), else backs off exponentially.
func retryDelay(header string, attempt int) time.Duration {
	header = strings.TrimSpace(header)
	if s, err := strconv.Atoi(header); err == nil && s >= 0 {
		return min(time.Duration(s)*time.Second, 30*time.Second)
	}
	if t, err := http.ParseTime(header); err == nil {
		return min(max(time.Until(t), 0), 30*time.Second)
	}
	return time.Duration(250<<attempt) * time.Millisecond
}

func retryable(status int) bool {
	return status == http.StatusTooManyRequests || status == http.StatusBadGateway ||
		status == http.StatusServiceUnavailable || status == http.StatusGatewayTimeout
}

// encodeParams sorts keys so request URLs are deterministic (recorded
// fixtures key on them).
func encodeParams(params url.Values) string {
	keys := make([]string, 0, len(params))
	for k := range params {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var b strings.Builder
	for _, k := range keys {
		for _, v := range params[k] {
			if b.Len() > 0 {
				b.WriteByte('&')
			}
			b.WriteString(url.QueryEscape(k))
			b.WriteByte('=')
			b.WriteString(url.QueryEscape(v))
		}
	}
	return b.String()
}

// get calls GET <base>/api/v1/<path>?params (POSTing a form when the URL
// would be too long and the endpoint accepts POST) and returns the envelope's
// data, retrying rate-limited and unavailable responses.
func (c *conn) get(ctx context.Context, path string, params url.Values, postOK bool) (json.RawMessage, error) {
	endpoint := c.BaseURL + "/api/v1/" + path
	query := encodeParams(params)
	usePost := postOK && len(endpoint)+len(query)+1 > maxGetURL
	for attempt := 1; ; attempt++ {
		var req *http.Request
		var err error
		if usePost {
			req, err = http.NewRequestWithContext(ctx, http.MethodPost, endpoint, bytes.NewReader([]byte(query)))
			if err == nil {
				req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
			}
		} else {
			target := endpoint
			if query != "" {
				target += "?" + query
			}
			req, err = http.NewRequestWithContext(ctx, http.MethodGet, target, nil)
		}
		if err != nil {
			return nil, err
		}
		for k, vs := range c.Headers {
			for _, v := range vs {
				req.Header.Add(k, v)
			}
		}
		resp, err := client.Do(req)
		if err != nil {
			if attempt >= MaxAttempts || errors.Is(err, context.Canceled) {
				return nil, fmt.Errorf("prometheus /api/v1/%s: %w", path, err)
			}
			sleep(ctx, retryDelay("", attempt))
			continue
		}
		body, readErr := io.ReadAll(io.LimitReader(resp.Body, 1<<30))
		_ = resp.Body.Close()
		if retryable(resp.StatusCode) && attempt < MaxAttempts {
			slog.Debug("prometheus: retrying", "path", path, "status", resp.StatusCode, "attempt", attempt)
			sleep(ctx, retryDelay(resp.Header.Get("Retry-After"), attempt))
			continue
		}
		if readErr != nil {
			return nil, fmt.Errorf("prometheus /api/v1/%s: reading response: %w", path, readErr)
		}
		var env envelope
		jsonErr := json.Unmarshal(body, &env)
		if resp.StatusCode/100 != 2 || jsonErr != nil || env.Status != "success" {
			e := &apiError{Status: resp.StatusCode, Path: "/api/v1/" + path}
			if jsonErr == nil {
				e.ErrorType, e.Message = env.ErrorType, env.Error
			} else {
				snippet := strings.TrimSpace(string(body))
				if len(snippet) > 300 {
					snippet = snippet[:300]
				}
				e.Message = snippet
			}
			return nil, e
		}
		for _, w := range env.Warnings {
			slog.Warn("prometheus warning", "path", path, "warning", w)
		}
		return env.Data, nil
	}
}

// formatTime renders a Unix-millisecond time as the API's seconds form.
func formatTime(ms int64) string {
	sec := ms / 1000
	frac := ms % 1000
	if frac < 0 {
		sec--
		frac += 1000
	}
	if frac == 0 {
		return strconv.FormatInt(sec, 10)
	}
	return fmt.Sprintf("%d.%03d", sec, frac)
}

// formatStep renders a step in milliseconds as seconds for the API.
func formatStep(ms int64) string { return formatTime(ms) }
