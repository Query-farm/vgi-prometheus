// Compiling pushed SQL predicates into a PromQL selector and a time window.
//
// The selector only has to return a superset of the matching rows: every
// pushed predicate is re-evaluated exactly in filters.go. Each rule below is
// chosen so that anything the SQL predicate accepts, the PromQL matcher also
// accepts:
//
//	label = 'v'                     -> label="v"
//	label <> 'v'                    -> label!="v"
//	label IN ('a','b')              -> label=~"a|b"
//	label IS NULL / IS NOT NULL     -> label="" / label!=""
//	prefix/suffix/contains(label,…) -> label=~"…"
//	label LIKE / ILIKE 'p%'         -> label=~"…" (case-insensitive for ILIKE)
//	label NOT LIKE 'p%'             -> label!~"…"
//	regexp_matches(label, 're')     -> label=~".*(?:re).*" (RE2 on both sides)
//	regexp_full_match(label, 're')  -> label=~"re"
//	a OR b on one label             -> one =~ alternation
//	ts >= / > / <= / < / = literal  -> request window
//
// metric is the __name__ label; job, instance and every label_columns column
// are labels. Predicates on value, labels or anything else are evaluated by
// the worker only.

package prometheus

import (
	"regexp"
	"strconv"
	"strings"
)

// matcher is one PromQL label matcher.
type matcher struct {
	Label string
	Op    string // =, !=, =~, !~
	Value string
}

func (m matcher) String() string {
	name := m.Label
	if !legacyLabelName.MatchString(name) {
		name = strconv.Quote(name)
	}
	return name + m.Op + strconv.Quote(m.Value)
}

var legacyLabelName = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)
var legacyMetricName = regexp.MustCompile(`^[a-zA-Z_:][a-zA-Z0-9_:]*$`)

// timeWindow is an inclusive millisecond range; nil ends are unbounded.
type timeWindow struct {
	Lo, Hi *int64
}

func (w *timeWindow) raiseLo(ms int64) {
	if w.Lo == nil || ms > *w.Lo {
		w.Lo = &ms
	}
}

func (w *timeWindow) lowerHi(ms int64) {
	if w.Hi == nil || ms < *w.Hi {
		w.Hi = &ms
	}
}

func floorDiv(a, b int64) int64 {
	q := a / b
	if (a%b != 0) && ((a < 0) != (b < 0)) {
		q--
	}
	return q
}

// pushdownPlan is what the pushed predicates compile to.
type pushdownPlan struct {
	Matchers []matcher
	Window   timeWindow
}

// planPushdown walks the top-level conjunction of preds.
func planPushdown(preds []*expr, cols map[string]colSpec) pushdownPlan {
	var plan pushdownPlan
	var walk func(e *expr)
	walk = func(e *expr) {
		if e.K == "and" {
			if m, ok := prefixRange(e, cols); ok {
				plan.Matchers = append(plan.Matchers, m)
				return
			}
			for _, a := range e.Args {
				walk(a)
			}
			return
		}
		if tsBound(e, cols, &plan.Window) {
			return
		}
		if m, ok := labelMatcher(e, cols); ok {
			plan.Matchers = append(plan.Matchers, m)
		}
	}
	for _, p := range preds {
		walk(p)
	}
	return plan
}

// colLabel returns the label a column reference reads, if any.
func colLabel(e *expr, cols map[string]colSpec) (string, bool) {
	if e == nil || e.K != "col" {
		return "", false
	}
	c, ok := cols[e.S]
	if !ok || c.Kind != kindLabel {
		return "", false
	}
	return c.Label, true
}

func isTsCol(e *expr, cols map[string]colSpec) bool {
	if e == nil || e.K != "col" {
		return false
	}
	c, ok := cols[e.S]
	return ok && c.Kind == kindTs
}

// tsBound narrows w for a `ts <op> literal` comparison.
func tsBound(e *expr, cols map[string]colSpec, w *timeWindow) bool {
	if e.K != "call" || len(e.Args) != 2 {
		return false
	}
	op, a, b := e.S, e.Args[0], e.Args[1]
	if !isTsCol(a, cols) {
		a, b = b, a
		op = map[string]string{"<": ">", "<=": ">=", ">": "<", ">=": "<=", "=": "="}[op]
	}
	if !isTsCol(a, cols) || b.K != "ts" {
		return false
	}
	// Conservative millisecond bounds: never exclude a matching row.
	lo := floorDiv(b.I, 1000)
	hi := floorDiv(b.I+999, 1000)
	if b.F > 0 {
		hi = floorDiv(b.I+1000, 1000)
	}
	switch op {
	case ">", ">=":
		w.raiseLo(lo)
	case "<":
		// ts < t on an exact millisecond excludes t itself.
		if b.F == 0 && b.I%1000 == 0 {
			hi = floorDiv(b.I, 1000) - 1
		}
		w.lowerHi(hi)
	case "<=":
		w.lowerHi(hi)
	case "=":
		w.raiseLo(lo)
		w.lowerHi(hi)
	default:
		return false
	}
	return true
}

// labelRegex returns an RE2 pattern (fully anchored, Prometheus style) that
// accepts every label value the predicate accepts, for positive predicates.
func labelRegex(e *expr, cols map[string]colSpec) (string, string, bool) {
	if e.K == "or" {
		var label string
		var alts []string
		for _, a := range e.Args {
			l, re, ok := labelRegex(a, cols)
			if !ok || (label != "" && l != label) {
				return "", "", false
			}
			label = l
			alts = append(alts, re)
		}
		return label, strings.Join(alts, "|"), label != ""
	}
	if e.K != "call" || len(e.Args) < 2 {
		return "", "", false
	}
	label, ok := colLabel(e.Args[0], cols)
	if !ok {
		return "", "", false
	}
	var lits []string
	for _, a := range e.Args[1:] {
		if a.K != "str" {
			return "", "", false
		}
		lits = append(lits, a.S)
	}
	switch e.S {
	case "=":
		return label, regexp.QuoteMeta(lits[0]), true
	case "in":
		q := make([]string, len(lits))
		for i, l := range lits {
			q[i] = regexp.QuoteMeta(l)
		}
		return label, strings.Join(q, "|"), true
	case "prefix", "starts_with":
		return label, regexp.QuoteMeta(lits[0]) + "(?s:.*)", true
	case "suffix", "ends_with":
		return label, "(?s:.*)" + regexp.QuoteMeta(lits[0]), true
	case "contains":
		return label, "(?s:.*)" + regexp.QuoteMeta(lits[0]) + "(?s:.*)", true
	case "~~", "like":
		return label, "(?:" + likeToRegex(lits[0], false, false) + ")", true
	case "~~*", "ilike":
		return label, "(?:" + likeToRegex(lits[0], true, false) + ")", true
	case "regexp_matches", "regexp_full_match":
		flags := ""
		if len(lits) > 1 {
			f, err := regexFlags(lits[1])
			if err != nil {
				return "", "", false
			}
			flags = f
		}
		if _, err := regexp.Compile(flags + lits[0]); err != nil {
			return "", "", false
		}
		if e.S == "regexp_full_match" {
			return label, "(?:" + flags + "(?:" + lits[0] + "))", true
		}
		return label, "(?s:.*)(?:" + flags + "(?:" + lits[0] + "))(?s:.*)", true
	}
	return "", "", false
}

// prefixSuccessor is the smallest string greater than every string starting
// with p ("" when there is none).
func prefixSuccessor(p string) string {
	b := []byte(p)
	for i := len(b) - 1; i >= 0; i-- {
		if b[i] < 0xff {
			b[i]++
			return string(b[:i+1])
		}
	}
	return ""
}

// prefixRange recognises DuckDB's rewrite of prefix(col, 'p') / col LIKE 'p%'
// into `col >= 'p' AND col < succ('p')` and turns it back into a prefix regex.
func prefixRange(e *expr, cols map[string]colSpec) (matcher, bool) {
	if len(e.Args) != 2 {
		return matcher{}, false
	}
	var label, lo, hi string
	for _, a := range e.Args {
		if a.K != "call" || len(a.Args) != 2 || a.Args[1].K != "str" {
			return matcher{}, false
		}
		l, ok := colLabel(a.Args[0], cols)
		if !ok || (label != "" && l != label) {
			return matcher{}, false
		}
		label = l
		switch a.S {
		case ">=":
			lo = a.Args[1].S
		case "<":
			hi = a.Args[1].S
		default:
			return matcher{}, false
		}
	}
	if lo == "" || hi == "" || prefixSuccessor(lo) != hi {
		return matcher{}, false
	}
	return matcher{label, "=~", regexp.QuoteMeta(lo) + "(?s:.*)"}, true
}

// labelMatcher compiles one predicate to a matcher.
func labelMatcher(e *expr, cols map[string]colSpec) (matcher, bool) {
	if e.K == "not" && len(e.Args) == 1 {
		// NOT p(label) is true only for non-NULL labels that fail p, so the
		// negated regex (which also admits absent labels) is a superset.
		if label, re, ok := labelRegex(e.Args[0], cols); ok {
			if _, err := regexp.Compile("^(?:" + re + ")$"); err == nil {
				return matcher{label, "!~", re}, true
			}
		}
		return matcher{}, false
	}
	if e.K == "call" {
		switch e.S {
		case "isnull", "isnotnull":
			label, ok := colLabel(e.Args[0], cols)
			if !ok {
				return matcher{}, false
			}
			if e.S == "isnull" {
				return matcher{label, "=", ""}, true
			}
			return matcher{label, "!=", ""}, true
		case "=":
			if label, ok := colLabel(e.Args[0], cols); ok && e.Args[1].K == "str" {
				return matcher{label, "=", e.Args[1].S}, true
			}
			if label, ok := colLabel(e.Args[1], cols); ok && e.Args[0].K == "str" {
				return matcher{label, "=", e.Args[0].S}, true
			}
			return matcher{}, false
		case "!=", "<>":
			if label, ok := colLabel(e.Args[0], cols); ok && e.Args[1].K == "str" {
				return matcher{label, "!=", e.Args[1].S}, true
			}
			return matcher{}, false
		case "!~~", "not_like", "!~~*", "not_ilike":
			label, ok := colLabel(e.Args[0], cols)
			if !ok || len(e.Args) != 2 || e.Args[1].K != "str" {
				return matcher{}, false
			}
			ci := strings.Contains(e.S, "*") || strings.Contains(e.S, "ilike")
			return matcher{label, "!~", likeToRegex(e.Args[1].S, ci, false)}, true
		}
	}
	label, re, ok := labelRegex(e, cols)
	if !ok {
		return matcher{}, false
	}
	if _, err := regexp.Compile("^(?:" + re + ")$"); err != nil {
		return matcher{}, false
	}
	return matcher{label, "=~", re}, true
}

// buildSelector appends matchers to a metric name or selector.
func buildSelector(base string, ms []matcher) string {
	parts := make([]string, len(ms))
	for i, m := range ms {
		parts[i] = m.String()
	}
	extra := strings.Join(parts, ",")
	base = strings.TrimSpace(base)
	if extra == "" {
		return base
	}
	if strings.HasSuffix(base, "}") {
		inner := strings.TrimSpace(base[strings.Index(base, "{")+1 : len(base)-1])
		if inner == "" {
			return base[:len(base)-1] + extra + "}"
		}
		return base[:len(base)-1] + "," + extra + "}"
	}
	return base + "{" + extra + "}"
}
