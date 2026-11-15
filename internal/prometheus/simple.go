// A small framework for the one-request metadata and status functions:
// declare the arguments, the columns and a fetch that returns rows of Go
// values; the shared code handles state, secrets and Arrow building.

package prometheus

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"time"

	"github.com/Query-farm/vgi-go/vgi"
	"github.com/Query-farm/vgi-rpc-go/vgirpc"
	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// secretNamed is implemented by every argument struct.
type secretNamed interface{ secret() string }

// SimpleState carries the bound arguments (JSON) and whether the single
// request has been made.
type SimpleState struct {
	ArgsJSON string
	Done     bool
}

// simpleFunc is a one-request table function with argument struct A.
type simpleFunc[A secretNamed] struct {
	name     string
	meta     func() vgi.FunctionMetadata
	columns  []arrow.Field
	validate func(A) error
	fetch    func(ctx context.Context, c *conn, a A) ([][]any, error)
}

func (f *simpleFunc[A]) Name() string                   { return f.name }
func (f *simpleFunc[A]) Metadata() vgi.FunctionMetadata { return f.meta() }
func (f *simpleFunc[A]) ArgumentSpecs() []vgi.ArgSpec   { var a A; return vgi.DeriveArgSpecs(a) }
func (f *simpleFunc[A]) schema() *arrow.Schema          { return arrow.NewSchema(f.columns, nil) }
func (f *simpleFunc[A]) OnBind(_ *vgi.BindParams) (*vgi.BindResponse, error) {
	return vgi.BindSchema(f.schema())
}

func (f *simpleFunc[A]) NewState(params *vgi.ProcessParams) (*SimpleState, error) {
	var a A
	if err := vgi.BindArgs(params.Args, &a); err != nil {
		return nil, err
	}
	if f.validate != nil {
		if err := f.validate(a); err != nil {
			return nil, err
		}
	}
	if _, err := resolveConn(params.Secrets, a.secret()); err != nil {
		return nil, err
	}
	b, err := json.Marshal(a)
	if err != nil {
		return nil, err
	}
	return &SimpleState{ArgsJSON: string(b)}, nil
}

func (f *simpleFunc[A]) Process(ctx context.Context, params *vgi.ProcessParams, st *SimpleState, out *vgirpc.OutputCollector) error {
	if st.Done {
		return out.Finish()
	}
	var a A
	if err := json.Unmarshal([]byte(st.ArgsJSON), &a); err != nil {
		return err
	}
	c, err := resolveConn(params.Secrets, a.secret())
	if err != nil {
		return err
	}
	rows, err := f.fetch(ctx, c, a)
	if err != nil {
		return err
	}
	st.Done = true
	cols, err := buildGeneric(f.columns, rows)
	if err != nil {
		return err
	}
	for _, col := range cols {
		defer col.Release()
	}
	return out.EmitArrays(cols, int64(len(rows)))
}

func newSimple[A secretNamed](f *simpleFunc[A]) vgi.TableFunction {
	return vgi.AsTableFunction[SimpleState](f)
}

// nullable value helpers for fetch implementations.

// optString is NULL for "".
func optString(s string) any {
	if s == "" {
		return nil
	}
	return s
}

// optTime parses an RFC 3339 time; the zero time and "" are NULL.
func optTime(s string) any {
	if s == "" {
		return nil
	}
	t, err := time.Parse(time.RFC3339Nano, s)
	if err != nil || t.Year() <= 1 {
		return nil
	}
	return t
}

// buildGeneric builds columns from rows of Go values. Supported values per
// column type: string; int64; float64; bool; time.Time; map[string]string;
// nil for NULL.
func buildGeneric(fields []arrow.Field, rows [][]any) ([]arrow.Array, error) {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, 0, len(fields))
	release := func() {
		for _, a := range out {
			a.Release()
		}
	}
	for ci, fd := range fields {
		b := array.NewBuilder(mem, fd.Type)
		for ri, r := range rows {
			v := r[ci]
			if v == nil {
				if !fd.Nullable {
					b.Release()
					release()
					return nil, fmt.Errorf("internal: NULL in non-null column %s (row %d)", fd.Name, ri)
				}
				b.AppendNull()
				continue
			}
			if err := appendGeneric(b, v); err != nil {
				b.Release()
				release()
				return nil, fmt.Errorf("column %s: %w", fd.Name, err)
			}
		}
		out = append(out, b.NewArray())
		b.Release()
	}
	return out, nil
}

func appendGeneric(b array.Builder, v any) error {
	switch bb := b.(type) {
	case *array.StringBuilder:
		s, ok := v.(string)
		if !ok {
			return fmt.Errorf("want string, got %T", v)
		}
		bb.Append(s)
	case *array.Int64Builder:
		i, ok := v.(int64)
		if !ok {
			return fmt.Errorf("want int64, got %T", v)
		}
		bb.Append(i)
	case *array.Float64Builder:
		f, ok := v.(float64)
		if !ok {
			return fmt.Errorf("want float64, got %T", v)
		}
		bb.Append(f)
	case *array.BooleanBuilder:
		x, ok := v.(bool)
		if !ok {
			return fmt.Errorf("want bool, got %T", v)
		}
		bb.Append(x)
	case *array.TimestampBuilder:
		t, ok := v.(time.Time)
		if !ok {
			return fmt.Errorf("want time, got %T", v)
		}
		bb.Append(arrow.Timestamp(t.UnixMicro()))
	case *array.MapBuilder:
		m, ok := v.(map[string]string)
		if !ok {
			return fmt.Errorf("want map, got %T", v)
		}
		bb.Append(true)
		keys := make([]string, 0, len(m))
		for k := range m {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		kb := bb.KeyBuilder().(*array.StringBuilder)
		ib := bb.ItemBuilder().(*array.StringBuilder)
		for _, k := range keys {
			kb.Append(k)
			ib.Append(m[k])
		}
	default:
		return fmt.Errorf("unsupported builder %T", b)
	}
	return nil
}
