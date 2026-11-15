// A parser for the SQL the SDK renders for pushed expression filters.
//
// The SDK renders every expression filter it cannot express as a typed
// filter to a DuckDB SQL boolean expression: fully parenthesised binary
// operators, function calls, quoted column references and literals, e.g.
//
//	regexp_matches("job", 'api.*')
//	("instance" ~~ 'node-%')
//	((prefix("env", 'prod')) OR ("env" IN ('qa','dev')))
//
// This parser accepts exactly that subset and produces the expr tree that
// filters.go evaluates. Anything else is an error, surfaced to the user with
// the SQL text, never silently widened.

package prometheus

import (
	"fmt"
	"strconv"
	"strings"
	"unicode"
)

type sqlToken struct {
	kind byte // 'i' ident, 'q' quoted ident, 's' string, 'n' number, 'o' operator, 'p' punct, 0 EOF
	text string
}

func tokenizeSQL(s string) ([]sqlToken, error) {
	var out []sqlToken
	i := 0
	for i < len(s) {
		c := s[i]
		switch {
		case c == ' ' || c == '\t' || c == '\n' || c == '\r':
			i++
		case c == '(' || c == ')' || c == ',':
			out = append(out, sqlToken{'p', string(c)})
			i++
		case c == '"':
			var b strings.Builder
			i++
			for {
				if i >= len(s) {
					return nil, fmt.Errorf("unterminated identifier")
				}
				if s[i] == '"' {
					if i+1 < len(s) && s[i+1] == '"' {
						b.WriteByte('"')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			out = append(out, sqlToken{'q', b.String()})
		case c == '\'':
			var b strings.Builder
			i++
			for {
				if i >= len(s) {
					return nil, fmt.Errorf("unterminated string")
				}
				if s[i] == '\'' {
					if i+1 < len(s) && s[i+1] == '\'' {
						b.WriteByte('\'')
						i += 2
						continue
					}
					i++
					break
				}
				b.WriteByte(s[i])
				i++
			}
			out = append(out, sqlToken{'s', b.String()})
		case c >= '0' && c <= '9' || (c == '.' && i+1 < len(s) && s[i+1] >= '0' && s[i+1] <= '9'):
			j := i
			for j < len(s) && (s[j] >= '0' && s[j] <= '9' || s[j] == '.' || s[j] == 'e' || s[j] == 'E' ||
				((s[j] == '+' || s[j] == '-') && j > i && (s[j-1] == 'e' || s[j-1] == 'E'))) {
				j++
			}
			out = append(out, sqlToken{'n', s[i:j]})
			i = j
		case c == '_' || unicode.IsLetter(rune(c)):
			j := i
			for j < len(s) && (s[j] == '_' || s[j] == '.' || s[j] == '/' || s[j] == '@' ||
				unicode.IsLetter(rune(s[j])) || unicode.IsDigit(rune(s[j]))) {
				j++
			}
			out = append(out, sqlToken{'i', s[i:j]})
			i = j
		default:
			j := i
			for j < len(s) && strings.ContainsRune("=!<>~*|+-/%:&^", rune(s[j])) {
				j++
			}
			if j == i {
				return nil, fmt.Errorf("unexpected character %q", string(c))
			}
			out = append(out, sqlToken{'o', s[i:j]})
			i = j
		}
	}
	return append(out, sqlToken{}), nil
}

type sqlParser struct {
	toks []sqlToken
	pos  int
}

func (p *sqlParser) peek() sqlToken { return p.toks[p.pos] }
func (p *sqlParser) next() sqlToken {
	t := p.toks[p.pos]
	if t.kind != 0 {
		p.pos++
	}
	return t
}

func (p *sqlParser) isKeyword(kw string) bool {
	t := p.peek()
	return t.kind == 'i' && strings.EqualFold(t.text, kw)
}

func (p *sqlParser) expect(kind byte, text string) error {
	t := p.next()
	if t.kind != kind || (text != "" && !strings.EqualFold(t.text, text)) {
		return fmt.Errorf("expected %q, found %q", text, t.text)
	}
	return nil
}

// parseSQL parses one rendered boolean expression.
func parseSQL(s string) (*expr, error) {
	toks, err := tokenizeSQL(s)
	if err != nil {
		return nil, err
	}
	p := &sqlParser{toks: toks}
	e, err := p.parseExpr()
	if err != nil {
		return nil, err
	}
	if p.peek().kind != 0 {
		return nil, fmt.Errorf("unexpected %q", p.peek().text)
	}
	return e, nil
}

// normalizeFunction strips a namespace and version ("duckdb.core/prefix@1" -> "prefix").
func normalizeFunction(name string) string {
	if i := strings.LastIndex(name, "/"); i >= 0 {
		name = name[i+1:]
	}
	if i := strings.Index(name, "@"); i >= 0 {
		name = name[:i]
	}
	return strings.ToLower(name)
}

// parseExpr parses a left-associative chain of binary operators.
func (p *sqlParser) parseExpr() (*expr, error) {
	left, err := p.parsePrimary()
	if err != nil {
		return nil, err
	}
	for {
		t := p.peek()
		switch {
		case p.isKeyword("AND") || p.isKeyword("OR"):
			kind := strings.ToLower(p.next().text)
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			if left.K == kind {
				left.Args = append(left.Args, right)
			} else {
				left = &expr{K: kind, Args: []*expr{left, right}}
			}
		case p.isKeyword("IS"):
			p.next()
			neg := false
			if p.isKeyword("NOT") {
				p.next()
				neg = true
			}
			if !p.isKeyword("NULL") {
				return nil, fmt.Errorf("IS DISTINCT FROM is not supported")
			}
			p.next()
			fn := "isnull"
			if neg {
				fn = "isnotnull"
			}
			left = &expr{K: "call", S: fn, Args: []*expr{left}}
		case p.isKeyword("IN") || (p.isKeyword("NOT") && p.toks[p.pos+1].kind == 'i' && strings.EqualFold(p.toks[p.pos+1].text, "IN")):
			neg := p.isKeyword("NOT")
			if neg {
				p.next()
			}
			p.next()
			if err := p.expect('p', "("); err != nil {
				return nil, err
			}
			n := &expr{K: "call", S: "in", Args: []*expr{left}}
			for {
				item, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				if item.K == "null" {
					// x IN (..., NULL) can never be TRUE because of the NULL.
				} else {
					n.Args = append(n.Args, item)
				}
				if p.peek().kind == 'p' && p.peek().text == "," {
					p.next()
					continue
				}
				break
			}
			if err := p.expect('p', ")"); err != nil {
				return nil, err
			}
			left = n
			if neg {
				left = &expr{K: "not", Args: []*expr{n}}
			}
		case t.kind == 'o' && t.text != "::":
			op := p.next().text
			right, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			left = &expr{K: "call", S: op, Args: []*expr{left, right}}
		case t.kind == 'o' && t.text == "::":
			p.next()
			typ := p.next()
			if typ.kind != 'i' {
				return nil, fmt.Errorf("bad cast")
			}
			return nil, fmt.Errorf("casts to %s are not supported", typ.text)
		default:
			return left, nil
		}
	}
}

func (p *sqlParser) parsePrimary() (*expr, error) {
	t := p.next()
	switch t.kind {
	case 'p':
		if t.text != "(" {
			return nil, fmt.Errorf("unexpected %q", t.text)
		}
		e, err := p.parseExpr()
		if err != nil {
			return nil, err
		}
		if err := p.expect('p', ")"); err != nil {
			return nil, err
		}
		return e, nil
	case 'q':
		return &expr{K: "col", S: t.text}, nil
	case 's':
		return &expr{K: "str", S: t.text}, nil
	case 'n':
		f, err := strconv.ParseFloat(t.text, 64)
		if err != nil {
			return nil, fmt.Errorf("bad number %q", t.text)
		}
		return &expr{K: "num", F: f}, nil
	case 'o':
		if t.text == "-" {
			e, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			if e.K == "num" {
				e.F = -e.F
				return e, nil
			}
		}
		return nil, fmt.Errorf("unexpected operator %q", t.text)
	case 'i':
		switch strings.ToUpper(t.text) {
		case "TRUE":
			return &expr{K: "bool", B: true}, nil
		case "FALSE":
			return &expr{K: "bool", B: false}, nil
		case "NULL":
			return &expr{K: "null"}, nil
		case "NOT":
			e, err := p.parsePrimary()
			if err != nil {
				return nil, err
			}
			return &expr{K: "not", Args: []*expr{e}}, nil
		case "CAST":
			return nil, fmt.Errorf("CAST is not supported")
		}
		if p.peek().kind == 'p' && p.peek().text == "(" {
			p.next()
			n := &expr{K: "call", S: normalizeFunction(t.text)}
			if p.peek().kind == 'p' && p.peek().text == ")" {
				p.next()
				return n, nil
			}
			for {
				a, err := p.parseExpr()
				if err != nil {
					return nil, err
				}
				n.Args = append(n.Args, a)
				if p.peek().kind == 'p' && p.peek().text == "," {
					p.next()
					continue
				}
				break
			}
			if err := p.expect('p', ")"); err != nil {
				return nil, err
			}
			return n, nil
		}
		return nil, fmt.Errorf("unexpected identifier %q", t.text)
	}
	return nil, fmt.Errorf("unexpected end of expression")
}
