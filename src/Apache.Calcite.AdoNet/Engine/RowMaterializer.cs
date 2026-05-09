using System;
using System.Collections.Generic;

using org.apache.calcite.avatica;

using Apache.Calcite.AdoNet.Protocol;

namespace Apache.Calcite.AdoNet.Engine
{

    /// <summary>
    /// Materializes a single row produced by a Calcite enumerator into a managed object array,
    /// honoring the <see cref="Meta.CursorFactory"/> style chosen by the planner.
    /// </summary>
    internal sealed class RowMaterializer
    {

        readonly Meta.CursorFactory _cursorFactory;
        readonly int _columnCount;

        RowMaterializer(Meta.CursorFactory cursorFactory, int columnCount)
        {
            _cursorFactory = cursorFactory;
            _columnCount = columnCount;
        }

        public static RowMaterializer For(Meta.CursorFactory cursorFactory, IReadOnlyList<CalciteColumn> columns)
        {
            return new RowMaterializer(cursorFactory, columns.Count);
        }

        public IReadOnlyList<object?> Materialize(object? row)
        {
            var style = _cursorFactory.style;

            if (style == Meta.Style.OBJECT)
            {
                // Single column, value is the row itself.
                return new[] { ConvertValue(row) };
            }

            if (style == Meta.Style.ARRAY)
            {
                var arr = (object[])row!;
                var values = new object?[arr.Length];
                for (var i = 0; i < arr.Length; i++)
                    values[i] = ConvertValue(arr[i]);
                return values;
            }

            if (style == Meta.Style.LIST)
            {
                var list = (java.util.List)row!;
                var size = list.size();
                var values = new object?[size];
                for (var i = 0; i < size; i++)
                    values[i] = ConvertValue(list.get(i));
                return values;
            }

            // RECORD / MAP styles fall back to reflection over public fields if needed.
            throw new NotSupportedException($"Cursor style '{style}' is not yet supported.");
        }

        static object? ConvertValue(object? v)
        {
            if (v is null)
                return null;

            if (v is java.lang.String) return v.ToString();
            if (v is java.math.BigDecimal bd) return decimal.Parse(bd.toPlainString(), System.Globalization.CultureInfo.InvariantCulture);
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

            return v;
        }

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

    }

}
