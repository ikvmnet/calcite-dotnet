using System;
using System.Collections.Generic;
using System.Data;

using org.apache.calcite.avatica.util;


namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Converts <see cref="CalciteParameterValue"/> entries supplied by the ADO.NET layer into the
    /// Java/Calcite-native representations expected by the planner and runtime.
    /// </summary>
    /// <remarks>
    /// Calcite's dynamic parameters are positional (<c>?</c>) and exposed at execution time through
    /// the <c>DataContext</c> as <c>?0</c>, <c>?1</c>, ... This binder mirrors what the JDBC driver
    /// does internally for <c>PreparedStatement.setXxx</c>: it converts each CLR value to the
    /// representation Calcite's runtime expects (Java boxed primitives, <c>BigDecimal</c>,
    /// <c>java.sql.Date</c>/<c>Time</c>/<c>Timestamp</c>, <c>ByteString</c>, ...).
    /// </remarks>
    internal static class ParameterBinder
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        public static IReadOnlyList<object?> Bind(IReadOnlyList<CalciteParameterValue> parameters)
        {
            if (parameters is null || parameters.Count == 0)
                return Array.Empty<object?>();

            var result = new object?[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
                result[i] = Convert(parameters[i]);

            return result;
        }

        static object? Convert(CalciteParameterValue p)
        {
            var value = p.Value;
            if (value is null || value is DBNull)
                return null;

            return p.DbType switch
            {
                DbType.Boolean => java.lang.Boolean.valueOf(System.Convert.ToBoolean(value)),
                DbType.Byte => java.lang.Short.valueOf(System.Convert.ToInt16(value)),
                DbType.SByte => java.lang.Byte.valueOf(unchecked((byte)System.Convert.ToSByte(value))),
                DbType.Int16 => java.lang.Short.valueOf(System.Convert.ToInt16(value)),
                DbType.UInt16 => java.lang.Integer.valueOf(System.Convert.ToInt32(value)),
                DbType.Int32 => java.lang.Integer.valueOf(System.Convert.ToInt32(value)),
                DbType.UInt32 => java.lang.Long.valueOf(System.Convert.ToInt64(value)),
                DbType.Int64 => java.lang.Long.valueOf(System.Convert.ToInt64(value)),
                DbType.UInt64 => UInt64ToBigDecimal(System.Convert.ToUInt64(value)),
                DbType.Single => java.lang.Float.valueOf(System.Convert.ToSingle(value)),
                DbType.Double => java.lang.Double.valueOf(System.Convert.ToDouble(value)),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric =>
                    BigDecimalConverter.ToBigDecimal(System.Convert.ToDecimal(value)),
                DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength =>
                    System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture),
                DbType.Guid => value is Guid g ? g.ToString() : value.ToString(),
                DbType.Date => ConvertDate(value),
                DbType.DateTime or DbType.DateTime2 => ConvertTimestamp(value),
                DbType.DateTimeOffset => ConvertTimestamp(value is DateTimeOffset dto ? dto.UtcDateTime : (DateTime)value),
                DbType.Time => ConvertTime(value),
                DbType.Binary => ConvertBinary(value),
                _ => InferFromClr(value),
            };
        }

        static object InferFromClr(object value)
        {
            return value switch
            {
                bool b => java.lang.Boolean.valueOf(b),
                sbyte sb => java.lang.Byte.valueOf(unchecked((byte)sb)),
                byte by => java.lang.Short.valueOf(by),
                short s => java.lang.Short.valueOf(s),
                int i => java.lang.Integer.valueOf(i),
                long l => java.lang.Long.valueOf(l),
                float f => java.lang.Float.valueOf(f),
                double d => java.lang.Double.valueOf(d),
                decimal m => BigDecimalConverter.ToBigDecimal(m),
                string s => s,
                Guid g => g.ToString(),
                DateTime dt => ConvertTimestamp(dt),
                DateTimeOffset dto => ConvertTimestamp(dto.UtcDateTime),
                TimeSpan ts => ConvertTime(ts),
                byte[] b => ConvertBinary(b),
                _ => value,
            };
        }

        static java.lang.Integer ConvertDate(object value)
        {
            var dt = value switch
            {
                DateTime d => DateTime.SpecifyKind(d.Date, DateTimeKind.Utc),
                DateTimeOffset dto => DateTime.SpecifyKind(dto.UtcDateTime.Date, DateTimeKind.Utc),
                DateOnly d => new DateTime(d.Year, d.Month, d.Day, 0, 0, 0, DateTimeKind.Utc),
                _ => DateTime.SpecifyKind(System.Convert.ToDateTime(value).Date, DateTimeKind.Utc),
            };
            var days = (int)(dt - UnixEpoch).TotalDays;
            return java.lang.Integer.valueOf(days);
        }

        static java.lang.Long ConvertTimestamp(object value)
        {
            var dt = value switch
            {
                DateTime d => d.Kind == DateTimeKind.Unspecified ? DateTime.SpecifyKind(d, DateTimeKind.Utc) : d.ToUniversalTime(),
                DateTimeOffset dto => dto.UtcDateTime,
                DateOnly d => new DateTime(d.Year, d.Month, d.Day, 0, 0, 0, DateTimeKind.Utc),
                _ => DateTime.SpecifyKind(System.Convert.ToDateTime(value), DateTimeKind.Utc),
            };
            var ms = (long)(dt - UnixEpoch).TotalMilliseconds;
            return java.lang.Long.valueOf(ms);
        }

        static java.lang.Integer ConvertTime(object value)
        {
            var ts = value switch
            {
                TimeSpan t => t,
                DateTime d => d.TimeOfDay,
                TimeOnly t => t.ToTimeSpan(),
                _ => TimeSpan.Parse(System.Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture) ?? "0"),
            };
            return java.lang.Integer.valueOf((int)ts.TotalMilliseconds);
        }

        static java.math.BigDecimal UInt64ToBigDecimal(ulong value)
        {
            // Big-endian unsigned magnitude; signum=1 (or 0 for zero).
            var bytes = new byte[8];
            bytes[0] = (byte)(value >> 56);
            bytes[1] = (byte)(value >> 48);
            bytes[2] = (byte)(value >> 40);
            bytes[3] = (byte)(value >> 32);
            bytes[4] = (byte)(value >> 24);
            bytes[5] = (byte)(value >> 16);
            bytes[6] = (byte)(value >> 8);
            bytes[7] = (byte)value;
            var unscaled = new java.math.BigInteger(value == 0 ? 0 : 1, bytes);
            return new java.math.BigDecimal(unscaled);
        }

        static ByteString ConvertBinary(object value)
        {
            if (value is byte[] bytes)
                return new ByteString(bytes);
            if (value is string s)
                return new ByteString(System.Text.Encoding.UTF8.GetBytes(s));

            throw new InvalidCastException($"Cannot bind value of type '{value.GetType()}' as DbType.Binary.");
        }

    }

}
