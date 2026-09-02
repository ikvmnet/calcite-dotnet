using System;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Thin wrapper over an object returned by Calcite. Provides the final conversion methods to coerce the type to and from various CLR
    /// types.
    /// </summary>
    /// <remarks>
    /// Two things decide what a value is. The column's <see cref="RelDataType"/> decides wherever it can:
    /// Calcite stores a <c>DATE</c> as a count of days and a <c>TIMESTAMP</c> as a count of milliseconds,
    /// so nothing about the runtime value says which one an integer is. <see cref="SqlTypeName.ANY"/> is
    /// where it cannot — the type is <c>java.lang.Object</c> and the value is whatever a table, a
    /// user-defined function or a schema put there — and there <b>the value's own class stands in for the
    /// declared type</b>.
    ///
    /// <para>Standing in for it is all it does. <c>ANY</c> does not make an accessor lenient: a
    /// <c>java.lang.Integer</c> in an <c>ANY</c> column is an <c>INTEGER</c>, so it reads through
    /// <see cref="GetInt32"/> and <see cref="GetInt64"/> refuses it exactly as it refuses an
    /// <c>INTEGER</c> column. What the <c>ANY</c> arms add is the case the SQL type used to be the only
    /// route to: a <c>java.sql.Timestamp</c> or a <c>java.time.LocalDate</c> says what it is by being
    /// what it is, and before this there was no column type to say it, because <c>ANY</c> is not
    /// <c>TIMESTAMP</c> or <c>DATE</c>. Each such arm takes exactly the type its accessor returns —
    /// a date reads through <see cref="GetDateOnly"/> and not through <see cref="GetDateTime"/> with a
    /// zero time bolted on — and a column whose type does say what it holds is untouched by any of it.</para>
    ///
    /// <para><see cref="CalciteValues"/> holds the conversion itself, in both directions and recursively,
    /// so that a collection of an <c>ANY</c> is read the same way the <c>ANY</c> is.</para>
    /// </remarks>
    internal readonly struct CalciteResultValue
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        readonly RelDataType _type;
        readonly SqlTypeName _sqlType;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="value"></param>
        public CalciteResultValue(RelDataType type, object? value)
        {
            _type = type ?? throw new ArgumentNullException(nameof(type));
            _sqlType = type.getSqlTypeName();
            _value = value;
        }

        /// <summary>
        /// Gets whether the column's type says nothing about what the value is, which is the case in which
        /// the accessors read the value's own type instead.
        /// </summary>
        /// <remarks>
        /// Two types leave it unsaid, and they are the same problem written two ways. An <c>ANY</c> is
        /// <c>java.lang.Object</c> and carries nothing; a <c>VARIANT</c> carries its payload's type with
        /// the payload. Either way the column does not say, and the value does.
        /// </remarks>
        bool IsUntyped => _sqlType == SqlTypeName.ANY || _sqlType == SqlTypeName.VARIANT;

        /// <summary>
        /// Returns the exception an accessor throws where the value is not the thing asked for.
        /// </summary>
        /// <param name="target"></param>
        /// <returns></returns>
        InvalidCastException Cannot(string target)
        {
            return new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to '{target}'");
        }

        /// <summary>
        /// Returns the value converted by its own type, which is what an accessor over an untyped column
        /// reads. Null everywhere else, so an arm written against it cannot fire for a column that does
        /// say what it holds.
        /// </summary>
        /// <returns></returns>
        object? Untyped()
        {
            return IsUntyped ? CalciteValues.ToClr(_value, _type) : null;
        }

        /// <summary>
        /// Returns <c>true</c> if the value is DBNull.
        /// </summary>
        /// <returns></returns>
        public bool IsDbNull()
        {
            return _value is null || CalciteVariants.IsNull(_value);
        }

        /// <summary>
        /// Implements the GetFieldValue operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <remarks>
        /// The .NET value is tried first, so <c>GetFieldValue&lt;object&gt;()</c> answers what
        /// <see cref="GetValue"/> answers rather than the Java object behind it, and
        /// <c>GetFieldValue&lt;IDictionary&gt;()</c> or <c>GetFieldValue&lt;int[]&gt;()</c> answers a
        /// <c>MAP</c> or an <c>ARRAY</c> without the caller naming element types. A caller that does name
        /// them gets the conversion built to them instead. The Java object itself is last and reached
        /// only by naming its class, which is the escape hatch for a value nothing corresponds to.
        /// </remarks>
        public T GetFieldValue<T>()
        {
            if (_value is null)
            {
                // For value types, DBNull is not assignable; for reference types, return null.
                if (default(T) is null)
                    return default!;

                throw new InvalidCastException($"Cannot convert null/DB value to {typeof(T).Name}");
            }

            var target = typeof(T);

            // the value as an ADO.NET caller reads it, which is what nearly every ask is for
            if (CalciteValues.ToClr(_value, _type) is T converted)
                return converted;

            // Handle common ADO.NET types
            if (target == typeof(string))
                return (T)(object)GetString();
            if (target == typeof(char))
                return (T)(object)GetChar();
            if (target == typeof(byte[]))
                return (T)(object)(GetValue() as byte[] ?? throw Cannot("Byte[]"));
            if (target == typeof(DateTime))
                return (T)(object)GetDateTime();
            if (target == typeof(DateTimeOffset))
                return (T)(object)GetDateTimeOffset();
            if (target == typeof(TimeSpan))
                return (T)(object)GetTimeSpan();
            if (target == typeof(DateOnly))
                return (T)(object)GetDateOnly();
            if (target == typeof(TimeOnly))
                return (T)(object)GetTimeOnly();
            if (target == typeof(decimal))
                return (T)(object)GetDecimal();
            if (target == typeof(double))
                return (T)(object)GetDouble();
            if (target == typeof(float))
                return (T)(object)GetFloat();
            if (target == typeof(Guid))
                return (T)(object)GetGuid();
            if (target == typeof(short))
                return (T)(object)GetInt16();
            if (target == typeof(int))
                return (T)(object)GetInt32();
            if (target == typeof(long))
                return (T)(object)GetInt64();
            if (target == typeof(sbyte))
                return (T)(object)GetSByte();
            if (target == typeof(byte))
                return (T)(object)GetByte();
            if (target == typeof(ushort))
                return (T)(object)GetUInt16();
            if (target == typeof(uint))
                return (T)(object)GetUInt32();
            if (target == typeof(ulong))
                return (T)(object)GetUInt64();

            // a collection whose element types the caller named rather than the ones the values measured
            if (CalciteValues.TryConvertTo(_value, _type, target, out var shaped) && shaped is T reshaped)
                return reshaped;

            // last, the object Calcite produced, for a caller that asked for it by its own class
            if (target.IsInstanceOfType(_value))
                return (T)_value;

            throw Cannot(typeof(T).Name);
        }

        /// <summary>
        /// Implements the GetValue operation.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            // a variant holding a null converts to one, so the coalesce is reachable and not a formality
            return _value is null ? DBNull.Value : CalciteValues.ToClr(_value, _type) ?? DBNull.Value;
        }

        /// <summary>
        /// Implements the GetBoolean operation.
        /// </summary>
        public bool GetBoolean()
        {
            return _value switch
            {
                java.lang.Boolean b => b.booleanValue(),
                _ when Untyped() is bool clr => clr,
                _ => throw Cannot("Boolean"),
            };
        }

        /// <summary>
        /// Implements the GetString operation.
        /// </summary>
        /// <returns></returns>
        public string GetString()
        {
            return _value switch
            {
                string s => s,
                _ when Untyped() is string text => text,
                _ => throw Cannot("String"),
            };
        }

        /// <summary>
        /// Implements the GetChar operation. A <c>CHAR</c> column means a character: Calcite's
        /// runtime representation of the character family is a string, so the value converts
        /// when the SQL type is <c>CHAR</c> and the string holds exactly one character. Any
        /// other SQL type or length is not a character and does not convert.
        /// </summary>
        /// <returns></returns>
        public char GetChar()
        {
            return _value switch
            {
                java.lang.Character c => c.charValue(),
                string s when _sqlType == SqlTypeName.CHAR && s.Length == 1 => s[0],
                _ when Untyped() is char clr => clr,
                _ => throw Cannot("Char"),
            };
        }

        /// <summary>
        /// Implements the GetBytes operation to a destination buffer.
        /// </summary>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        public long GetBytes(long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (_value is null)
                return 0;

            // a BINARY arrives as a ByteString and an ANY holding binary may be either
            var bytes = GetValue() as byte[] ?? throw Cannot("Byte[]");

            if (buffer is null)
                return bytes.LongLength;

            var available = bytes.LongLength - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <summary>
        /// Implements the GetChars operation to a destination buffer.
        /// </summary>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        public long GetChars(long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var s = GetString();
            if (buffer is null)
                return s.Length;

            var available = s.Length - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            s.CopyTo((int)dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <summary>
        /// Implements the GetDateTime operation. Only valid for DATE and TIMESTAMP columns.
        /// </summary>
        /// <returns></returns>
        public DateTime GetDateTime()
        {
            return _value switch
            {
                java.lang.Integer i when _sqlType == SqlTypeName.DATE => UnixEpoch.AddDays(i.intValue()),
                java.sql.Date d when _sqlType == SqlTypeName.DATE => UnixEpoch.AddMilliseconds(d.getTime()),
                java.lang.Long l when _sqlType == SqlTypeName.TIMESTAMP => UnixEpoch.AddMilliseconds(l.longValue()),
                java.sql.Timestamp ts when _sqlType == SqlTypeName.TIMESTAMP => UnixEpoch.AddMilliseconds(ts.getTime()),
                // a java.sql.Timestamp, a java.util.Date or a java.time.LocalDateTime says it is a moment
                // whatever column it came out of, and under ANY that is the only thing saying so
                _ when Untyped() is DateTime dt => dt,
                _ => throw Cannot("DateTime"),
            };
        }

        /// <summary>
        /// Implements the GetDateTimeOffset operation. Only valid for zoned TIMESTAMP / TIME columns.
        /// </summary>
        /// <returns></returns>
        public DateTimeOffset GetDateTimeOffset()
        {
            return _value switch
            {
                java.lang.Long l when (_sqlType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE || _sqlType == SqlTypeName.TIMESTAMP_TZ) => new DateTimeOffset(UnixEpoch.AddMilliseconds(l.longValue()), TimeSpan.Zero),
                java.sql.Timestamp ts when (_sqlType == SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE || _sqlType == SqlTypeName.TIMESTAMP_TZ) => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                java.lang.Integer i when (_sqlType == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE || _sqlType == SqlTypeName.TIME_TZ) => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(i.intValue())),
                java.sql.Time t when (_sqlType == SqlTypeName.TIME_WITH_LOCAL_TIME_ZONE || _sqlType == SqlTypeName.TIME_TZ) => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(t.getTime())),
                _ when Untyped() is DateTimeOffset dto => dto,
                _ => throw Cannot("DateTimeOffset"),
            };
        }

        /// <summary>
        /// Implements the GetTimeSpan operation. Only valid for TIME columns.
        /// </summary>
        /// <returns></returns>
        public TimeSpan GetTimeSpan()
        {
            return _value switch
            {
                java.lang.Integer i when _sqlType == SqlTypeName.TIME => TimeSpan.FromMilliseconds(i.intValue()),
                java.sql.Time t when _sqlType == SqlTypeName.TIME => TimeSpan.FromMilliseconds(t.getTime()),
                _ when Untyped() is TimeSpan ts => ts,
                _ => throw Cannot("TimeSpan"),
            };
        }

        /// <summary>
        /// Implements the GetDecimal operation.
        /// </summary>
        /// <returns></returns>
        public decimal GetDecimal()
        {
            return _value switch
            {
                java.math.BigDecimal bd => JavaDecimals.ToDecimal(bd),
                _ when Untyped() is decimal clr => clr,
                _ => throw Cannot("Decimal"),
            };
        }

        /// <summary>
        /// Implements the GetDouble operation.
        /// </summary>
        /// <returns></returns>
        public double GetDouble()
        {
            return _value switch
            {
                java.lang.Double d => d.doubleValue(),
                _ when Untyped() is double clr => clr,
                _ => throw Cannot("Double"),
            };
        }

        /// <summary>
        /// Implements the GetFloat operation.
        /// </summary>
        /// <returns></returns>
        public float GetFloat()
        {
            return _value switch
            {
                java.lang.Float f => f.floatValue(),
                _ when Untyped() is float clr => clr,
                _ => throw Cannot("Single"),
            };
        }

        /// <summary>
        /// Implements the GetGuid operation. Calcite's runtime representation of <c>UUID</c> is a
        /// <see cref="java.util.UUID"/>, and that is the only thing this reads: a character column
        /// holding text in canonical GUID form is a character column, and parsing it here would be
        /// <see cref="GetGuid"/> answering for a type the column does not have. <c>CAST(x AS UUID)</c> is
        /// how a caller says it means one.
        /// </summary>
        /// <returns></returns>
        public Guid GetGuid()
        {
            return _value switch
            {
                java.util.UUID u => JavaUuids.ToGuid(u),
                _ when Untyped() is Guid clr => clr,
                _ => throw Cannot("Guid"),
            };
        }

        /// <summary>
        /// Implements the GetInt16 operation.
        /// </summary>
        /// <returns></returns>
        public short GetInt16()
        {
            return _value switch
            {
                java.lang.Short s => s.shortValue(),
                _ when Untyped() is short clr => clr,
                _ => throw Cannot("Int16"),
            };
        }

        /// <summary>
        /// Implements the GetInt32 operation.
        /// </summary>
        /// <returns></returns>
        public int GetInt32()
        {
            return _value switch
            {
                java.lang.Integer i => i.intValue(),
                _ when Untyped() is int clr => clr,
                _ => throw Cannot("Int32"),
            };
        }

        /// <summary>
        /// Implements the GetInt64 operation.
        /// </summary>
        /// <returns></returns>
        public long GetInt64()
        {
            return _value switch
            {
                java.lang.Long l => l.longValue(),
                _ when Untyped() is long clr => clr,
                _ => throw Cannot("Int64"),
            };
        }

        /// <summary>
        /// Implements the GetByte operation. A <see cref="byte"/> is a <c>TINYINT UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.UByte</c>; a signed <c>TINYINT</c> is not one.
        /// </summary>
        public byte GetByte()
        {
            return _value switch
            {
                org.joou.UByte ub => (byte)ub.byteValue(),
                _ when Untyped() is byte clr => clr,
                _ => throw Cannot("Byte"),
            };
        }

        /// <summary>
        /// Implements the GetSByte operation. An <see cref="sbyte"/> is a <c>TINYINT</c>, which Java
        /// signs and Calcite holds as a <c>java.lang.Byte</c>.
        /// </summary>
        public sbyte GetSByte()
        {
            return _value switch
            {
                java.lang.Byte by => (sbyte)by.byteValue(),
                _ when Untyped() is sbyte clr => clr,
                _ => throw Cannot("SByte"),
            };
        }

        /// <summary>
        /// Implements the GetUInt16 operation. A <see cref="ushort"/> is a <c>SMALLINT UNSIGNED</c>,
        /// which Calcite's runtime holds as an <c>org.joou.UShort</c>.
        /// </summary>
        public ushort GetUInt16()
        {
            return _value switch
            {
                org.joou.UShort us => (ushort)us.shortValue(),
                _ when Untyped() is ushort clr => clr,
                _ => throw Cannot("UInt16"),
            };
        }

        /// <summary>
        /// Implements the GetUInt32 operation. A <see cref="uint"/> is an <c>INTEGER UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.UInteger</c>.
        /// </summary>
        public uint GetUInt32()
        {
            return _value switch
            {
                org.joou.UInteger ui => (uint)ui.intValue(),
                _ when Untyped() is uint clr => clr,
                _ => throw Cannot("UInt32"),
            };
        }

        /// <summary>
        /// Implements the GetUInt64 operation. A <see cref="ulong"/> is a <c>BIGINT UNSIGNED</c>, which
        /// Calcite's runtime holds as an <c>org.joou.ULong</c>; a <c>DECIMAL</c> wide enough to hold the
        /// same number is still a <c>DECIMAL</c>.
        /// </summary>
        public ulong GetUInt64()
        {
            return _value switch
            {
                org.joou.ULong ul => (ulong)ul.longValue(),
                _ when Untyped() is ulong clr => clr,
                _ => throw Cannot("UInt64"),
            };
        }

        /// <summary>
        /// Implements the GetDateOnly operation. Only valid for DATE columns.
        /// </summary>
        /// <returns></returns>
        public DateOnly GetDateOnly()
        {
            return _value switch
            {
                java.lang.Integer i when _sqlType == SqlTypeName.DATE => DateOnly.FromDateTime(UnixEpoch.AddDays(i.intValue())),
                java.sql.Date d when _sqlType == SqlTypeName.DATE => DateOnly.FromDateTime(UnixEpoch.AddMilliseconds(d.getTime())),
                _ when Untyped() is DateOnly dd => dd,
                _ => throw Cannot("DateOnly"),
            };
        }

        /// <summary>
        /// Implements the GetTimeOnly operation. Only valid for TIME columns.
        /// </summary>
        /// <returns></returns>
        public TimeOnly GetTimeOnly()
        {
            return _value switch
            {
                java.lang.Integer i when _sqlType == SqlTypeName.TIME => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(i.intValue())),
                java.sql.Time t when _sqlType == SqlTypeName.TIME => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(t.getTime())),
                _ when Untyped() is TimeOnly to => to,
                _ => throw Cannot("TimeOnly"),
            };
        }

    }

}
