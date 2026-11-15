// Arrow layout of sample rows, shared by query, query_range and samples.

package prometheus

import (
	"sort"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// colKind says how a sample column is filled from a row.
type colKind int

const (
	kindResultType colKind = iota
	kindLabel              // one label's value (metric = __name__)
	kindLabels             // every label as MAP(VARCHAR, VARCHAR)
	kindTs
	kindValue
	kindHistogram
	kindString
)

// colSpec is one output column of a sample table. Exported for gob (it is
// kept in scan state).
type colSpec struct {
	Name  string
	Kind  colKind
	Label string // for kindLabel
}

var (
	labelsType = arrow.MapOf(arrow.BinaryTypes.String, arrow.BinaryTypes.String)
	tsType     = &arrow.TimestampType{Unit: arrow.Microsecond, TimeZone: "UTC"}
	bucketType = arrow.StructOf(
		arrow.Field{Name: "lower", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "upper", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "lower_inclusive", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
		arrow.Field{Name: "upper_inclusive", Type: arrow.FixedWidthTypes.Boolean, Nullable: false},
	)
	histogramType = arrow.StructOf(
		arrow.Field{Name: "count", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "sum", Type: arrow.PrimitiveTypes.Float64, Nullable: false},
		arrow.Field{Name: "buckets", Type: arrow.ListOf(bucketType), Nullable: false},
	)
)

const histogramSQLType = "STRUCT(count DOUBLE, sum DOUBLE, buckets STRUCT(lower DOUBLE, upper DOUBLE, count DOUBLE, lower_inclusive BOOLEAN, upper_inclusive BOOLEAN)[])"

// field returns the Arrow field for a column spec.
func (c colSpec) field() arrow.Field {
	switch c.Kind {
	case kindResultType:
		return column(c.Name, arrow.BinaryTypes.String, "PromQL result type: vector, matrix, scalar or string.", false)
	case kindLabel:
		if c.Label == "__name__" {
			return column(c.Name, arrow.BinaryTypes.String, "Metric name (the __name__ label); NULL for expressions that drop it, e.g. rate() or sum().", true)
		}
		return column(c.Name, arrow.BinaryTypes.String, "Value of the '"+c.Label+"' label; NULL when the series has no such label.", true)
	case kindLabels:
		return column(c.Name, labelsType, "Every label of the series, __name__ included, as MAP(VARCHAR, VARCHAR). Read one with labels['env'].", false)
	case kindTs:
		return column(c.Name, tsType, "Sample timestamp (UTC, millisecond precision).", false)
	case kindValue:
		return column(c.Name, arrow.PrimitiveTypes.Float64, "Float sample value; NaN, +Inf and -Inf are kept. NULL for native-histogram and string results.", true)
	case kindHistogram:
		return column(c.Name, histogramType, "Native-histogram sample (count, sum, buckets with bounds and inclusivity); NULL for float samples.", true)
	case kindString:
		return column(c.Name, arrow.BinaryTypes.String, "Value of a PromQL string result; NULL otherwise.", true)
	}
	panic("unknown column kind")
}

func schemaOf(cols []colSpec) *arrow.Schema {
	fields := make([]arrow.Field, len(cols))
	for i, c := range cols {
		fields[i] = c.field()
	}
	return arrow.NewSchema(fields, nil)
}

// queryColumns is the fixed layout of query and query_range.
var queryColumns = []colSpec{
	{Name: "result_type", Kind: kindResultType},
	{Name: "metric", Kind: kindLabel, Label: "__name__"},
	{Name: "job", Kind: kindLabel, Label: "job"},
	{Name: "instance", Kind: kindLabel, Label: "instance"},
	{Name: "labels", Kind: kindLabels},
	{Name: "ts", Kind: kindTs},
	{Name: "value", Kind: kindValue},
	{Name: "histogram", Kind: kindHistogram},
	{Name: "string_value", Kind: kindString},
}

// buildRows converts rows to Arrow arrays in cols order.
func buildRows(cols []colSpec, rows []row) []arrow.Array {
	mem := memory.NewGoAllocator()
	out := make([]arrow.Array, len(cols))
	for i, c := range cols {
		out[i] = buildColumn(mem, c, rows)
	}
	return out
}

func buildColumn(mem memory.Allocator, c colSpec, rows []row) arrow.Array {
	switch c.Kind {
	case kindResultType, kindLabel, kindString:
		b := array.NewStringBuilder(mem)
		defer b.Release()
		for i := range rows {
			r := &rows[i]
			switch c.Kind {
			case kindResultType:
				b.Append(r.ResultType)
			case kindLabel:
				if v, ok := r.label(c.Label); ok {
					b.Append(v)
				} else {
					b.AppendNull()
				}
			default:
				if r.Str != nil {
					b.Append(*r.Str)
				} else {
					b.AppendNull()
				}
			}
		}
		return b.NewArray()
	case kindLabels:
		b := array.NewMapBuilder(mem, arrow.BinaryTypes.String, arrow.BinaryTypes.String, false)
		defer b.Release()
		kb := b.KeyBuilder().(*array.StringBuilder)
		vb := b.ItemBuilder().(*array.StringBuilder)
		for i := range rows {
			b.Append(true)
			keys := make([]string, 0, len(rows[i].Labels))
			for k := range rows[i].Labels {
				keys = append(keys, k)
			}
			sort.Strings(keys)
			for _, k := range keys {
				kb.Append(k)
				vb.Append(rows[i].Labels[k])
			}
		}
		return b.NewArray()
	case kindTs:
		b := array.NewTimestampBuilder(mem, tsType)
		defer b.Release()
		for i := range rows {
			b.Append(arrow.Timestamp(rows[i].TsMs * 1000))
		}
		return b.NewArray()
	case kindValue:
		b := array.NewFloat64Builder(mem)
		defer b.Release()
		for i := range rows {
			if rows[i].HasValue {
				b.Append(rows[i].Value)
			} else {
				b.AppendNull()
			}
		}
		return b.NewArray()
	case kindHistogram:
		b := array.NewStructBuilder(mem, histogramType)
		defer b.Release()
		cb := b.FieldBuilder(0).(*array.Float64Builder)
		sb := b.FieldBuilder(1).(*array.Float64Builder)
		lb := b.FieldBuilder(2).(*array.ListBuilder)
		bb := lb.ValueBuilder().(*array.StructBuilder)
		lo := bb.FieldBuilder(0).(*array.Float64Builder)
		hi := bb.FieldBuilder(1).(*array.Float64Builder)
		cnt := bb.FieldBuilder(2).(*array.Float64Builder)
		li := bb.FieldBuilder(3).(*array.BooleanBuilder)
		ui := bb.FieldBuilder(4).(*array.BooleanBuilder)
		for i := range rows {
			h := rows[i].Hist
			if h == nil {
				b.AppendNull()
				continue
			}
			b.Append(true)
			cb.Append(h.Count)
			sb.Append(h.Sum)
			lb.Append(true)
			for _, bk := range h.Buckets {
				bb.Append(true)
				lo.Append(bk.Lower)
				hi.Append(bk.Upper)
				cnt.Append(bk.Count)
				li.Append(bk.LowerInclusive)
				ui.Append(bk.UpperInclusive)
			}
		}
		return b.NewArray()
	}
	panic("unknown column kind")
}
