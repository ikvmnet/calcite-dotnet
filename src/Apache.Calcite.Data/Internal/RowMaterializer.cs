using System;
using System.Collections.Generic;

using org.apache.calcite.avatica;
using org.apache.calcite.avatica.util;


namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Materializes a single row produced by a Calcite enumerator into a managed object array,
    /// honoring the <see cref="Meta.CursorFactory"/> style chosen by the planner and the SQL type
    /// of each column.
    /// </summary>
    internal sealed class RowMaterializer
    {

        readonly Meta.CursorFactory _cursorFactory;
        readonly IReadOnlyList<CalciteColumn> _columns;

        RowMaterializer(Meta.CursorFactory cursorFactory, IReadOnlyList<CalciteColumn> columns)
        {
            _cursorFactory = cursorFactory;
            _columns = columns;
        }

        public static RowMaterializer For(Meta.CursorFactory cursorFactory, IReadOnlyList<CalciteColumn> columns)
        {
            return new RowMaterializer(cursorFactory, columns);
        }

        public IReadOnlyList<object?> Materialize(object? row)
        {
            var style = _cursorFactory.style;

            if (style == Meta.Style.OBJECT)
            {
                return new[] { ConvertValue(row, 0) };
            }

            if (style == Meta.Style.ARRAY)
            {
                var arr = (object[])row!;
                var values = new object?[arr.Length];
                for (var i = 0; i < arr.Length; i++)
                    values[i] = ConvertValue(arr[i], i);
                return values;
            }

            if (style == Meta.Style.LIST)
            {
                var list = (java.util.List)row!;
                var size = list.size();
                var values = new object?[size];
                for (var i = 0; i < size; i++)
                    values[i] = ConvertValue(list.get(i), i);
                return values;
            }

            throw new NotSupportedException($"Cursor style '{style}' is not yet supported.");
        }

        object? ConvertValue(object? v, int columnIndex)
        {
            if (v is null)
                return null;

            var sqlType = columnIndex < _columns.Count ? _columns[columnIndex].ProviderTypeName : null;

            switch (sqlType)
            {
                case "DATE":
                    return v switch
                    {
                        java.lang.Integer i => UnixEpoch.AddDays(i.intValue()),
                        java.lang.Number n => UnixEpoch.AddDays(n.longValue()),
                        java.sql.Date d => UnixEpoch.AddMilliseconds(d.getTime()),
                        _ => v,
                    };
                case "TIME":
                    return v switch
                    {
                        java.lang.Integer i => TimeSpan.FromMilliseconds(i.intValue()),
                        java.lang.Number n => TimeSpan.FromMilliseconds(n.longValue()),
                        java.sql.Time t => TimeSpan.FromMilliseconds(t.getTime()),
                        _ => v,
                    };
                case "TIME WITH LOCAL TIME ZONE":
                case "TIME WITH TIME ZONE":
                    // Calcite's wire form is a count of milliseconds-since-midnight; the offset is
                    // not carried per-row, so surface as DateTimeOffset at the epoch date with UTC offset
                    // (matches IKVM.Jdbc's OffsetTime path which anchors at 0001-01-01).
                    return v switch
                    {
                        java.lang.Integer i => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(i.intValue())),
                        java.lang.Number n => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(n.longValue())),
                        java.sql.Time t => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(t.getTime())),
                        _ => v,
                    };
                case "TIMESTAMP":
                    return v switch
                    {
                        java.lang.Number n => UnixEpoch.AddMilliseconds(n.longValue()),
                        java.sql.Timestamp ts => UnixEpoch.AddMilliseconds(ts.getTime()),
                        _ => v,
                    };
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                case "TIMESTAMP WITH TIME ZONE":
                    // UTC instant; surface as DateTimeOffset with UTC offset.
                    return v switch
                    {
                        java.lang.Number n => new DateTimeOffset(UnixEpoch.AddMilliseconds(n.longValue()), TimeSpan.Zero),
                        java.sql.Timestamp ts => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                        _ => v,
                    };
                case "BINARY":
                case "VARBINARY":
                    return v switch
                    {
                        ByteString bs => bs.getBytes(),
                        byte[] b => b,
                        _ => v,
                    };
            }

            return ConvertValue(v);
        }

        static object? ConvertValue(object? v)
        {
            if (v is null)
                return null;

            if (v is java.lang.String) return v.ToString();
            if (v is java.math.BigDecimal bd) return BigDecimalConverter.ToDecimal(bd);
            if (v is java.lang.Boolean b) return b.booleanValue();
            if (v is java.lang.Byte by) return (sbyte)by.byteValue();
            if (v is java.lang.Short sh) return sh.shortValue();
            if (v is java.lang.Integer i) return i.intValue();
            if (v is java.lang.Long l) return l.longValue();
            if (v is java.lang.Float f) return f.floatValue();
            if (v is java.lang.Double d) return d.doubleValue();
            if (v is java.lang.Character c) return c.charValue();
            if (v is java.sql.Timestamp ts) return UnixEpoch.AddMilliseconds(ts.getTime());
            if (v is java.sql.Date dt) return UnixEpoch.AddMilliseconds(dt.getTime());
            if (v is java.sql.Time tm) return TimeSpan.FromMilliseconds(tm.getTime());
            if (v is ByteString bs) return bs.getBytes();

            return v;
        }

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    }

}
