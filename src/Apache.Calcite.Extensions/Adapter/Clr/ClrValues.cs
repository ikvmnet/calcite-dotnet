using System;
using System.Reflection;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// The .NET values Calcite has no representation for, as the values it does.
    /// </summary>
    /// <remarks>
    /// <b>An adapter, in the sense the whole port means by one.</b> A <see cref="decimal"/> is not a
    /// <c>BigDecimal</c> and a <see cref="DateTime"/> is not a number of milliseconds; handing either across
    /// as it stands leaves two representations of one value in a plan, and Calcite's own comparators fail on
    /// them. Every method here is one direction of one boundary, and the reverse of each is
    /// <see cref="ClrValues"/>'s to add when something needs to read one back.
    ///
    /// <para>What each answers is not a choice: it is what <c>JavaTypeFactoryImpl.getJavaClass</c> says a
    /// column of that SQL type holds. TIMESTAMP is a <see cref="long"/> of milliseconds, DATE an
    /// <see cref="int"/> of days, TIME an <see cref="int"/> of milliseconds in the day. Reading that method
    /// is what settles each of these, not SQL and not what the value ought to be.</para>
    /// </remarks>
    public static class ClrValues
    {

        /// <summary>
        /// The scale a <see cref="decimal"/> column is declared and normalised to.
        /// </summary>
        /// <remarks>
        /// <b>A policy, and the only one here.</b> A .NET <see cref="decimal"/> carries its scale per value
        /// rather than per type — <c>1.10m</c> and <c>1.1m</c> are different values of the same type — and a
        /// SQL column has one scale for all its rows, so a number has to be chosen. Six covers money and most
        /// measures inside the thirteen integral digits that leaves.
        ///
        /// <para>The normalisation matters more than the number. <c>BigDecimal.equals</c> is scale sensitive
        /// where <c>compareTo</c> is not, and a group key is compared by <c>equals</c> — so <c>1.10m</c> and
        /// <c>1.1m</c> would group apart if each kept the scale it arrived with. Rounding every value to one
        /// scale is what makes GROUP BY and DISTINCT mean what they say.</para>
        /// </remarks>
        public const int DecimalScale = 6;

        /// <summary>
        /// The precision a <see cref="decimal"/> column is declared with.
        /// </summary>
        /// <remarks>
        /// <c>RelDataTypeSystem.getMaxPrecision(DECIMAL)</c> is 19 and a .NET <see cref="decimal"/> holds up
        /// to 29 significant digits, so the ceiling is Calcite's rather than ours and a value wider than it is
        /// outside what the column can describe.
        /// </remarks>
        public const int DecimalPrecision = 19;

        static readonly java.math.RoundingMode HalfUp = java.math.RoundingMode.HALF_UP;

        /// <summary>
        /// Returns a <see cref="decimal"/> as the <c>BigDecimal</c> a DECIMAL column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// Through the invariant-culture string rather than through a <see cref="double"/>, which would round
        /// at the fifteenth digit and is the whole reason a decimal type exists.
        /// </remarks>
        public static java.math.BigDecimal ToBigDecimal(decimal value)
        {
            return new java.math.BigDecimal(value.ToString(System.Globalization.CultureInfo.InvariantCulture))
                .setScale(DecimalScale, HalfUp);
        }

        /// <summary>
        /// Returns a <see cref="DateTime"/> as the milliseconds a TIMESTAMP column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// The wall clock reading, whatever the <see cref="DateTimeKind"/>. A Calcite TIMESTAMP has no time
        /// zone — the type that does is TIMESTAMP_TZ — so a local time and a UTC time that read the same are
        /// the same TIMESTAMP, and shifting one of them would be inventing a zone the column does not have.
        /// </remarks>
        public static long ToTimestamp(DateTime value)
        {
            return (value.Ticks - DateTime.UnixEpoch.Ticks) / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Returns a <see cref="DateOnly"/> as the days a DATE column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int ToDate(DateOnly value)
        {
            return value.DayNumber - UnixEpochDay;
        }

        static readonly int UnixEpochDay = new DateOnly(1970, 1, 1).DayNumber;

        /// <summary>
        /// Returns a <see cref="TimeOnly"/> as the milliseconds a TIME column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int ToTime(TimeOnly value)
        {
            return (int)(value.Ticks / TimeSpan.TicksPerMillisecond);
        }

        /// <summary>
        /// Returns a <see cref="TimeSpan"/> as the milliseconds a day-to-second interval holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static long ToInterval(TimeSpan value)
        {
            return value.Ticks / TimeSpan.TicksPerMillisecond;
        }

        /// <summary>
        /// Returns a <see cref="Guid"/> as the 36 character string a CHAR(36) column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// The hyphenated form, which is what <c>AdoTable</c> already maps a <c>DbType.Guid</c> to and what
        /// every database that has no native type for one stores.
        /// </remarks>
        public static string ToChars(Guid value)
        {
            return value.ToString("D", System.Globalization.CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns a <see cref="char"/> as the one character string a CHAR(1) column holds.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string ToChars(char value)
        {
            return value.ToString();
        }

        /// <summary>
        /// The methods above, by the CLR type each converts from.
        /// </summary>
        internal static MethodInfo? Converter(Type type)
        {
            return type switch
            {
                _ when type == typeof(decimal) => Method(nameof(ToBigDecimal), typeof(decimal)),
                _ when type == typeof(DateTime) => Method(nameof(ToTimestamp), typeof(DateTime)),
                _ when type == typeof(DateOnly) => Method(nameof(ToDate), typeof(DateOnly)),
                _ when type == typeof(TimeOnly) => Method(nameof(ToTime), typeof(TimeOnly)),
                _ when type == typeof(TimeSpan) => Method(nameof(ToInterval), typeof(TimeSpan)),
                _ when type == typeof(Guid) => Method(nameof(ToChars), typeof(Guid)),
                _ when type == typeof(char) => Method(nameof(ToChars), typeof(char)),
                _ => null
            };
        }

        static MethodInfo Method(string name, Type parameter)
        {
            return typeof(ClrValues).GetMethod(name, BindingFlags.Public | BindingFlags.Static, [parameter])
                ?? throw new InvalidOperationException($"'{name}({parameter})' is missing.");
        }

    }

}
