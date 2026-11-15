// Exact evaluation of pushed SQL filters, in pure Go.
//
// DuckDB removes every predicate it pushes into a table function from its own
// plan and never re-checks the rows the function returns. samples() pushes
// label filters into the PromQL selector and timestamp ranges into the request
// window, but a PromQL selector is not an exact translation of SQL (absent
// labels, NULLs, NaN ordering, LIKE vs regex), so every pushed predicate is
// also evaluated here, row by row, and the selector only has to return a
// superset.
//
// Every pushed filter is first converted to a small serializable expression
// tree (expr), stored in the scan state so it survives HTTP continuation:
//
//   - the SDK's typed filters (comparison, IN, IS [NOT] NULL, AND, OR) are
//     converted directly, keeping TIMESTAMPTZ literals as exact microseconds;
//   - any other pushed expression (regexp_matches, prefix, LIKE, ...) is
//     rendered to SQL by the SDK and parsed by sqlparse.go. The worker
//     registers its own ExpressionEvaluator (no cgo, no embedded DuckDB) only
//     to capture that SQL text.
//
// Dynamic runtime filters (join keys, Top-N) are advisory and skipped: they
// only ever remove rows DuckDB discards itself.

package prometheus

import (
	"context"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"sync"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/apache/arrow-go/v18/arrow/scalar"
)

// expr is one node of a pushed predicate. K is the node kind:
// col, str, num, ts, bool, null, call (Fn = operator or function name), and, or, not.
type expr struct {
	K    string
	S    string  // col name, str literal, call function name
	F    float64 // num literal
	I    int64   // ts literal (Unix microseconds)
	B    bool
	Args []*expr
}

// value is a runtime value during evaluation.
type value struct {
	kind byte // 0 null, 's' string, 'f' float, 't' timestamp(us), 'b' bool
	s    string
	f    float64
	t    int64
	b    bool
}

var nullValue = value{}

// ---------------------------------------------------------------- conversion

// filterToExpr converts one SDK filter.
func filterToExpr(f vgi.Filter, schema *arrow.Schema) (*expr, error) {
	switch x := f.(type) {
	case *vgi.AndFilter:
		n := &expr{K: "and"}
		for _, c := range x.Children {
			e, err := filterToExpr(c, schema)
			if err != nil {
				return nil, err
			}
			n.Args = append(n.Args, e)
		}
		return n, nil
	case *vgi.OrFilter:
		n := &expr{K: "or"}
		for _, c := range x.Children {
			e, err := filterToExpr(c, schema)
			if err != nil {
				return nil, err
			}
			n.Args = append(n.Args, e)
		}
		return n, nil
	case *vgi.IsNullFilter:
		return &expr{K: "call", S: "isnull", Args: []*expr{{K: "col", S: x.ColumnName()}}}, nil
	case *vgi.IsNotNullFilter:
		return &expr{K: "call", S: "isnotnull", Args: []*expr{{K: "col", S: x.ColumnName()}}}, nil
	case *vgi.ConstantFilter:
		lit, err := scalarToExpr(x.Value)
		if err != nil {
			return nil, err
		}
		op := x.Op.Symbol()
		switch op {
		case "=", "!=", "<", "<=", ">", ">=":
		default:
			return nil, fmt.Errorf("unsupported comparison %q", op)
		}
		return &expr{K: "call", S: op, Args: []*expr{{K: "col", S: x.ColumnName()}, lit}}, nil
	case *vgi.InFilter:
		n := &expr{K: "call", S: "in", Args: []*expr{{K: "col", S: x.ColumnName()}}}
		for i := 0; x.Values != nil && i < x.Values.Len(); i++ {
			if x.Values.IsNull(i) {
				continue
			}
			s, err := scalar.GetScalar(x.Values, i)
			if err != nil {
				return nil, err
			}
			lit, err := scalarToExpr(s)
			if err != nil {
				return nil, err
			}
			n.Args = append(n.Args, lit)
		}
		return n, nil
	}
	sql, err := captureSQL(f, schema)
	if err != nil {
		return nil, err
	}
	e, err := parseSQL(sql)
	if err != nil {
		return nil, fmt.Errorf("the pushed filter %s cannot be evaluated by the prometheus worker (%v); rewrite it with =, <>, IN, LIKE, prefix(), regexp_matches() or a comparison", sql, err)
	}
	return e, nil
}

func scalarToExpr(s scalar.Scalar) (*expr, error) {
	if s == nil || !s.IsValid() {
		return &expr{K: "null"}, nil
	}
	switch v := s.(type) {
	case *scalar.String:
		return &expr{K: "str", S: v.String()}, nil
	case *scalar.LargeString:
		return &expr{K: "str", S: v.String()}, nil
	case *scalar.Boolean:
		return &expr{K: "bool", B: v.Value}, nil
	case *scalar.Float64:
		return &expr{K: "num", F: v.Value}, nil
	case *scalar.Float32:
		return &expr{K: "num", F: float64(v.Value)}, nil
	case *scalar.Timestamp:
		us, ns := splitMicros(int64(v.Value), v.Type.(*arrow.TimestampType).Unit)
		n := &expr{K: "ts", I: us}
		if ns != 0 {
			// Sub-microsecond literal: F places it between two microsecond
			// column values so ordering stays exact.
			n.F = 0.5
		}
		return n, nil
	case *scalar.Date32:
		return &expr{K: "ts", I: v.Value.ToTime().UnixMicro()}, nil
	}
	if f, ok := numericScalar(s); ok {
		return &expr{K: "num", F: f}, nil
	}
	return nil, fmt.Errorf("unsupported filter literal type %s", s.DataType())
}

func numericScalar(s scalar.Scalar) (float64, bool) {
	switch v := s.(type) {
	case *scalar.Int8:
		return float64(v.Value), true
	case *scalar.Int16:
		return float64(v.Value), true
	case *scalar.Int32:
		return float64(v.Value), true
	case *scalar.Int64:
		return float64(v.Value), true
	case *scalar.Uint8:
		return float64(v.Value), true
	case *scalar.Uint16:
		return float64(v.Value), true
	case *scalar.Uint32:
		return float64(v.Value), true
	case *scalar.Uint64:
		return float64(v.Value), true
	}
	return 0, false
}

// splitMicros converts a timestamp in unit to microseconds plus a nanosecond
// remainder (0..999), flooring so pre-1970 values stay ordered.
func splitMicros(v int64, unit arrow.TimeUnit) (int64, int64) {
	switch unit {
	case arrow.Second:
		return v * 1_000_000, 0
	case arrow.Millisecond:
		return v * 1_000, 0
	case arrow.Nanosecond:
		us, ns := v/1_000, v%1_000
		if ns < 0 {
			us--
			ns += 1_000
		}
		return us, ns
	}
	return v, 0
}

// ---------------------------------------------------------------- SQL capture

type captureKey struct{}

type captureBox struct{ sql string }

var registerOnce sync.Once

// registerEvaluator installs the capturing ExpressionEvaluator.
func registerEvaluator() {
	registerOnce.Do(func() {
		vgi.RegisterExpressionEvaluator(func(ctx context.Context, batch arrow.RecordBatch, sqlExpr string) (arrow.Array, error) {
			box, ok := ctx.Value(captureKey{}).(*captureBox)
			if !ok {
				return nil, errors.New("prometheus worker: filters are evaluated natively, not through an embedded DuckDB")
			}
			box.sql = sqlExpr
			b := array.NewBooleanBuilder(memory.DefaultAllocator)
			defer b.Release()
			for i := 0; i < int(batch.NumRows()); i++ {
				b.Append(true)
			}
			return b.NewArray(), nil
		})
	})
}

// errRuntimeFilter marks an advisory dynamic filter that has no static SQL.
var errRuntimeFilter = errors.New("runtime filter")

// captureSQL renders a filter to SQL through the SDK, over a one-row
// all-NULL batch laid out like the bind schema.
func captureSQL(f vgi.Filter, schema *arrow.Schema) (string, error) {
	registerEvaluator()
	mem := memory.DefaultAllocator
	cols := make([]arrow.Array, schema.NumFields())
	for i, fd := range schema.Fields() {
		cols[i] = array.MakeArrayOfNull(mem, fd.Type, 1)
	}
	batch := array.NewRecordBatch(schema, cols, 1)
	for _, c := range cols {
		c.Release()
	}
	defer batch.Release()
	box := &captureBox{}
	res, err := f.Evaluate(context.WithValue(context.Background(), captureKey{}, box), batch)
	if err != nil {
		if strings.Contains(err.Error(), "runtime filter has no evaluator") {
			return "", errRuntimeFilter
		}
		return "", fmt.Errorf("rendering a pushed filter: %w", err)
	}
	res.Release()
	if box.sql == "" {
		return "", errors.New("the SDK rendered no SQL for a pushed filter")
	}
	return box.sql, nil
}

// compileFilters converts every pushed filter (AND-ed at the top level).
// Advisory runtime filters are dropped.
func compileFilters(pf *vgi.PushdownFilters, schema *arrow.Schema) ([]*expr, error) {
	if pf == nil {
		return nil, nil
	}
	var out []*expr
	for _, f := range pf.Filters {
		e, err := filterToExpr(f, schema)
		if errors.Is(err, errRuntimeFilter) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out = append(out, e)
	}
	return out, nil
}

// ---------------------------------------------------------------- evaluation

type tri int8

const (
	triFalse tri = iota
	triTrue
	triNull
)

func boolTri(b bool) tri {
	if b {
		return triTrue
	}
	return triFalse
}

// rowEnv resolves column names against a row.
type rowEnv struct {
	cols map[string]colSpec
	r    *row
}

func (e *rowEnv) column(name string) (value, error) {
	c, ok := e.cols[name]
	if !ok {
		return nullValue, fmt.Errorf("filter on unknown column %q", name)
	}
	switch c.Kind {
	case kindLabel:
		if v, ok := e.r.label(c.Label); ok {
			return value{kind: 's', s: v}, nil
		}
		return nullValue, nil
	case kindResultType:
		return value{kind: 's', s: e.r.ResultType}, nil
	case kindString:
		if e.r.Str != nil {
			return value{kind: 's', s: *e.r.Str}, nil
		}
		return nullValue, nil
	case kindTs:
		return value{kind: 't', t: e.r.TsMs * 1000}, nil
	case kindValue:
		if e.r.HasValue {
			return value{kind: 'f', f: e.r.Value}, nil
		}
		return nullValue, nil
	}
	return nullValue, fmt.Errorf("filters on column %q (%s) cannot be evaluated by the worker", name, sqlTypeOf(c))
}

// matchAll reports whether r satisfies every predicate.
func matchAll(preds []*expr, env *rowEnv) (bool, error) {
	for _, p := range preds {
		v, err := evalExpr(p, env)
		if err != nil {
			return false, err
		}
		if v.kind != 'b' || !v.b {
			return false, nil
		}
	}
	return true, nil
}

func triValue(t tri) value {
	switch t {
	case triTrue:
		return value{kind: 'b', b: true}
	case triFalse:
		return value{kind: 'b', b: false}
	}
	return nullValue
}

func toTri(v value) tri {
	if v.kind == 0 {
		return triNull
	}
	if v.kind == 'b' {
		return boolTri(v.b)
	}
	return triNull
}

var regexCache sync.Map // pattern -> *regexp.Regexp

func compileRegex(p string) (*regexp.Regexp, error) {
	if re, ok := regexCache.Load(p); ok {
		return re.(*regexp.Regexp), nil
	}
	re, err := regexp.Compile(p)
	if err != nil {
		return nil, err
	}
	regexCache.Store(p, re)
	return re, nil
}

func evalExpr(e *expr, env *rowEnv) (value, error) {
	switch e.K {
	case "col":
		return env.column(e.S)
	case "str":
		return value{kind: 's', s: e.S}, nil
	case "num":
		return value{kind: 'f', f: e.F}, nil
	case "ts":
		return value{kind: 't', t: e.I, f: e.F}, nil
	case "bool":
		return value{kind: 'b', b: e.B}, nil
	case "null":
		return nullValue, nil
	case "and", "or":
		out := triTrue
		if e.K == "or" {
			out = triFalse
		}
		for _, a := range e.Args {
			v, err := evalExpr(a, env)
			if err != nil {
				return nullValue, err
			}
			t := toTri(v)
			if e.K == "and" {
				if t == triFalse {
					return triValue(triFalse), nil
				}
				if t == triNull {
					out = triNull
				}
			} else {
				if t == triTrue {
					return triValue(triTrue), nil
				}
				if t == triNull {
					out = triNull
				}
			}
		}
		return triValue(out), nil
	case "not":
		v, err := evalExpr(e.Args[0], env)
		if err != nil {
			return nullValue, err
		}
		switch toTri(v) {
		case triTrue:
			return triValue(triFalse), nil
		case triFalse:
			return triValue(triTrue), nil
		}
		return nullValue, nil
	case "call":
		return evalCall(e, env)
	}
	return nullValue, fmt.Errorf("unsupported filter node %q", e.K)
}

func evalCall(e *expr, env *rowEnv) (value, error) {
	args := make([]value, len(e.Args))
	for i, a := range e.Args {
		v, err := evalExpr(a, env)
		if err != nil {
			return nullValue, err
		}
		args[i] = v
	}
	switch e.S {
	case "isnull":
		return triValue(boolTri(args[0].kind == 0)), nil
	case "isnotnull":
		return triValue(boolTri(args[0].kind != 0)), nil
	case "in":
		if args[0].kind == 0 {
			return nullValue, nil
		}
		for _, v := range args[1:] {
			if c, ok := compareValues(args[0], v); ok && c == 0 {
				return triValue(triTrue), nil
			}
		}
		return triValue(triFalse), nil
	case "=", "!=", "<>", "<", "<=", ">", ">=":
		if args[0].kind == 0 || args[1].kind == 0 {
			return nullValue, nil
		}
		c, ok := compareValues(args[0], args[1])
		if !ok {
			return nullValue, fmt.Errorf("cannot compare %s with %s in a pushed filter", kindName(args[0]), kindName(args[1]))
		}
		var r bool
		switch e.S {
		case "=":
			r = c == 0
		case "!=", "<>":
			r = c != 0
		case "<":
			r = c < 0
		case "<=":
			r = c <= 0
		case ">":
			r = c > 0
		case ">=":
			r = c >= 0
		}
		return triValue(boolTri(r)), nil
	}
	// String functions: NULL in, NULL out.
	for _, a := range args {
		if a.kind == 0 {
			return nullValue, nil
		}
	}
	str := func(i int) (string, error) {
		if i >= len(args) || args[i].kind != 's' {
			return "", fmt.Errorf("%s expects VARCHAR arguments", e.S)
		}
		return args[i].s, nil
	}
	switch e.S {
	case "lower", "lcase", "upper", "ucase":
		s, err := str(0)
		if err != nil {
			return nullValue, err
		}
		if e.S == "lower" || e.S == "lcase" {
			return value{kind: 's', s: strings.ToLower(s)}, nil
		}
		return value{kind: 's', s: strings.ToUpper(s)}, nil
	case "prefix", "starts_with", "suffix", "ends_with", "contains":
		s, err := str(0)
		if err != nil {
			return nullValue, err
		}
		p, err := str(1)
		if err != nil {
			return nullValue, err
		}
		switch e.S {
		case "prefix", "starts_with":
			return triValue(boolTri(strings.HasPrefix(s, p))), nil
		case "suffix", "ends_with":
			return triValue(boolTri(strings.HasSuffix(s, p))), nil
		}
		return triValue(boolTri(strings.Contains(s, p))), nil
	case "~~", "!~~", "~~*", "!~~*", "like", "ilike", "not_like", "not_ilike":
		s, err := str(0)
		if err != nil {
			return nullValue, err
		}
		p, err := str(1)
		if err != nil {
			return nullValue, err
		}
		ci := strings.Contains(e.S, "*") || strings.Contains(e.S, "ilike")
		neg := strings.HasPrefix(e.S, "!") || strings.HasPrefix(e.S, "not")
		re, err := compileRegex(likeToRegex(p, ci, true))
		if err != nil {
			return nullValue, err
		}
		return triValue(boolTri(re.MatchString(s) != neg)), nil
	case "regexp_matches", "regexp_full_match":
		s, err := str(0)
		if err != nil {
			return nullValue, err
		}
		p, err := str(1)
		if err != nil {
			return nullValue, err
		}
		flags := ""
		if len(args) > 2 {
			opts, err := str(2)
			if err != nil {
				return nullValue, err
			}
			if flags, err = regexFlags(opts); err != nil {
				return nullValue, err
			}
		}
		pat := flags + "(?:" + p + ")"
		if e.S == "regexp_full_match" {
			pat = "^" + pat + "$"
		}
		re, err := compileRegex(pat)
		if err != nil {
			return nullValue, fmt.Errorf("bad regular expression %q: %w", p, err)
		}
		return triValue(boolTri(re.MatchString(s))), nil
	}
	return nullValue, fmt.Errorf("function %s is not supported in pushed filters", e.S)
}

// regexFlags maps DuckDB regex options to RE2 flags ("" when none apply).
func regexFlags(opts string) (string, error) {
	flags := ""
	for _, c := range opts {
		switch c {
		case 'c':
		case 'i':
			flags = "(?i)"
		case 's':
			// DuckDB 's': non-newline-sensitive (dot matches newline).
			flags += "(?s)"
		default:
			return "", fmt.Errorf("regexp option %q is not supported in pushed filters", string(c))
		}
	}
	return flags, nil
}

// likeToRegex converts a SQL LIKE pattern (% and _, no escape) to RE2.
func likeToRegex(p string, ci, anchored bool) string {
	var b strings.Builder
	if ci {
		b.WriteString("(?i)")
	}
	b.WriteString("(?s)")
	if anchored {
		b.WriteString("^")
	}
	for _, r := range p {
		switch r {
		case '%':
			b.WriteString(".*")
		case '_':
			b.WriteString(".")
		default:
			b.WriteString(regexp.QuoteMeta(string(r)))
		}
	}
	if anchored {
		b.WriteString("$")
	}
	return b.String()
}

func kindName(v value) string {
	switch v.kind {
	case 's':
		return "VARCHAR"
	case 'f':
		return "DOUBLE"
	case 't':
		return "TIMESTAMPTZ"
	case 'b':
		return "BOOLEAN"
	}
	return "NULL"
}

// compareValues orders two non-null values the way DuckDB does.
func compareValues(a, b value) (int, bool) {
	switch {
	case a.kind == 's' && b.kind == 's':
		return strings.Compare(a.s, b.s), true
	case a.kind == 'f' && b.kind == 'f':
		return cmpFloat(a.f, b.f), true
	case a.kind == 't' && b.kind == 't':
		if a.t != b.t {
			if a.t < b.t {
				return -1, true
			}
			return 1, true
		}
		// F carries a sub-microsecond literal offset (column values have none).
		return cmpFloat(a.f, b.f), true
	case a.kind == 'b' && b.kind == 'b':
		if a.b == b.b {
			return 0, true
		}
		if !a.b {
			return -1, true
		}
		return 1, true
	}
	return 0, false
}

// cmpFloat orders NaN above every other value and equal to itself, as DuckDB does.
func cmpFloat(a, b float64) int {
	switch {
	case math.IsNaN(a) && math.IsNaN(b):
		return 0
	case math.IsNaN(a):
		return 1
	case math.IsNaN(b):
		return -1
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}
