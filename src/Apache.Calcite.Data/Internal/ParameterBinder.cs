using System;
using System.Collections.Generic;
using System.Data;

using Apache.Calcite.Extensions.Interop;

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
    ///
    /// <para>The <see cref="DbType"/> decides where the caller named one, and where it did not
    /// <see cref="CalciteParameter.DbType"/> infers it from the value's CLR type — which is
    /// <see cref="DbType.Object"/> for everything <see cref="CalciteTypeMap.ToDbType"/> has no name for,
    /// a dictionary and a sequence included. That is the parameter half of an <c>ANY</c>, and it is
    /// <see cref="CalciteValues.ToJava"/>'s: a value handed to the <c>DataContext</c> as it stood would
    /// be a .NET object loose in a plan whose row types are Java classes, and the first thing to compare
    /// it against something would fail.</para>
    /// </remarks>
    internal static class ParameterBinder
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        /// <summary>
        /// Converts a list of <see cref="CalciteParameterValue"/> instances into the Java/Calcite-native
        /// representations passed to the <c>DataContext</c> at execution time.
        /// </summary>
        /// <param name="parameters">The parameter values to bind. May be <see langword="null"/> or empty.</param>
        /// <returns>
        /// An array of converted values in positional order, or an empty array when <paramref name="parameters"/> is
        /// <see langword="null"/> or contains no elements.
        /// </returns>
        public static IReadOnlyList<object?> Bind(IReadOnlyList<CalciteParameterValue> parameters)
        {
            if (parameters is null || parameters.Count == 0)
                return Array.Empty<object?>();

            var result = new object?[parameters.Count];
            for (var i = 0; i < parameters.Count; i++)
                result[i] = Convert(parameters[i]);

            return result;
        }

        /// <summary>
        /// Converts a single <see cref="CalciteParameterValue"/> to the Java/Calcite-native representation
        /// appropriate for its <see cref="CalciteParameterValue.DbType"/>, falling back to CLR-type inference
        /// when the type is <see cref="DbType.Object"/> or unrecognized.
        /// </summary>
        /// <param name="p">The parameter value to convert.</param>
        /// <returns>The converted value, or <see langword="null"/> when the parameter value is <see langword="null"/> or <see cref="DBNull"/>.</returns>
        static object? Convert(CalciteParameterValue p)
        {
            var value = p.Value;
            if (value is null || value is DBNull)
                return null;

            return p.DbType switch
            {
                DbType.Boolean => java.lang.Boolean.valueOf((bool)value),
                DbType.Byte => org.joou.UByte.valueOf((byte)value),
                DbType.SByte => java.lang.Byte.valueOf(unchecked((byte)(sbyte)value)),
                DbType.Int16 => java.lang.Short.valueOf((short)value),
                DbType.UInt16 => org.joou.UShort.valueOf((ushort)value),
                DbType.Int32 => java.lang.Integer.valueOf((int)value),
                DbType.UInt32 => org.joou.UInteger.valueOf((uint)value),
                DbType.Int64 => java.lang.Long.valueOf((long)value),
                DbType.UInt64 => org.joou.ULong.valueOf(unchecked((long)(ulong)value)),
                DbType.Single => java.lang.Float.valueOf((float)value),
                DbType.Double => java.lang.Double.valueOf((double)value),
                DbType.Decimal or DbType.Currency or DbType.VarNumeric => JavaDecimals.ToBigDecimal((decimal)value),
                DbType.String or DbType.AnsiString or DbType.StringFixedLength or DbType.AnsiStringFixedLength => ConvertString(value),
                DbType.Guid => JavaUuids.ToUuid((Guid)value),
                DbType.Date => ConvertDate(value),
                DbType.DateTime or DbType.DateTime2 => ConvertTimestamp(value),
                DbType.DateTimeOffset => ConvertTimestamp(value is DateTimeOffset dto ? dto.UtcDateTime : (DateTime)value),
                DbType.Time => ConvertTime(value),
                DbType.Binary => ConvertBinary(value),
                _ => CalciteValues.ToJava(value),
            };
        }

        /// <summary>
        /// Converts a value bound as one of the character types to the string Calcite's runtime holds one
        /// as.
        /// </summary>
        /// <param name="value">A <see cref="string"/>, or a <see cref="char"/>, which is what a
        /// one-character value infers <see cref="DbType.StringFixedLength"/> from.</param>
        /// <returns>The string.</returns>
        /// <exception cref="InvalidCastException">Where the value is neither.</exception>
        static string ConvertString(object value)
        {
            return value switch
            {
                string s => s,
                char c => c.ToString(),
                _ => throw new InvalidCastException($"Cannot bind value of type '{value.GetType()}' as a character type."),
            };
        }

        /// <summary>
        /// Converts a date value to the number of days since the Unix epoch (<c>1970-01-01</c>),
        /// which is the representation Calcite uses for the SQL <c>DATE</c> type.
        /// </summary>
        /// <param name="value">A <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="DateOnly"/>, or any value convertible to <see cref="DateTime"/>.</param>
        /// <returns>A boxed <c>int</c> representing the number of days since the Unix epoch.</returns>
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

        /// <summary>
        /// Converts a date/time value to the number of milliseconds since the Unix epoch (<c>1970-01-01T00:00:00Z</c>),
        /// which is the representation Calcite uses for the SQL <c>TIMESTAMP</c> type.
        /// </summary>
        /// <param name="value">A <see cref="DateTime"/>, <see cref="DateTimeOffset"/>, <see cref="DateOnly"/>, or any value convertible to <see cref="DateTime"/>.</param>
        /// <returns>A boxed <c>long</c> representing the number of milliseconds since the Unix epoch.</returns>
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

        /// <summary>
        /// Converts a time-of-day value to the number of milliseconds since midnight,
        /// which is the representation Calcite uses for the SQL <c>TIME</c> type.
        /// </summary>
        /// <param name="value">A <see cref="TimeSpan"/>, <see cref="DateTime"/>, <see cref="TimeOnly"/>, or any value convertible to a time string.</param>
        /// <returns>A boxed <c>int</c> representing the number of milliseconds since midnight.</returns>
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

        /// <summary>
        /// Converts a binary value to a Calcite <see cref="ByteString"/>.
        /// </summary>
        /// <param name="value">A <see cref="T:byte[]"/> or a <see cref="string"/> whose UTF-8 encoding is used.</param>
        /// <returns>A <see cref="ByteString"/> wrapping the binary content.</returns>
        /// <exception cref="InvalidCastException">Thrown when <paramref name="value"/> is neither a byte array nor a string.</exception>
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
