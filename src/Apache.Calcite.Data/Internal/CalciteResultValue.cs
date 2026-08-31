using System;

using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Thin wrapper over an object returned by Calcite. Provides the final conversion methods to coerce the type to and from various CLR
    /// types.
    /// </summary>
    internal readonly struct CalciteResultValue
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        readonly ClrTypeRegistry _registry;
        readonly RelDataType _relType;
        readonly SqlTypeName _sqlType;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="registry">The connection's type mapping.</param>
        /// <param name="relType">The Calcite type of the column the value came from.</param>
        /// <param name="value"></param>
        public CalciteResultValue(ClrTypeRegistry registry, RelDataType relType, object? value)
        {
            _registry = registry;
            _relType = relType;
            _sqlType = relType.getSqlTypeName();
            _value = value;
        }

        /// <summary>
        /// Returns <c>true</c> if the value is DBNull.
        /// </summary>
        /// <returns></returns>
        public bool IsDbNull()
        {
            return _value is null;
        }

        /// <summary>
        /// Implements the GetFieldValue operation.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
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

            // Fast path for direct assignable
            if (target.IsInstanceOfType(_value))
                return (T)_value;

            // a conversion the mapping states for this pair of types, which is also how a caller reaches a
            // type of their own. The getters below remain for the pairs no mapping names, and are strict on
            // purpose: a BIGINT is not read as an int merely because both are integers
            if (_registry.GetMapping(target, _relType) is ClrTypeMapping mapping && mapping.FromCalcite(_value) is T mapped)
                return mapped;

            // Handle common ADO.NET types
            if (target == typeof(string))
                return (T)(object)GetString();
            if (target == typeof(char))
                return (T)(object)GetChar();
            if (target == typeof(byte[]))
                return (T)(object)(GetValue() as byte[] ?? throw new InvalidCastException($"Cannot convert value of type '{_value.GetType().Name}' (SQL type: {_sqlType}) to 'Byte[]'"));
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

            // Fallback: try to convert via GetValue if possible
            var val = GetValue();
            if (val is T tval)
                return tval;

            throw new InvalidCastException($"Cannot convert value of type '{_value.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to '{typeof(T).Name}'");
        }

        /// <summary>
        /// Implements the GetValue operation.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            if (_value is null)
                return DBNull.Value;

            return _registry.FromCalcite(null, _relType, _value) ?? DBNull.Value;
        }

        /// <summary>
        /// Implements the GetBoolean operation.
        /// </summary>
        public bool GetBoolean()
        {
            return _value switch
            {
                java.lang.Boolean b => b.booleanValue(),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Boolean'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'String'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Char'"),
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
            if (_value is not null && _value is not byte[])
                throw new InvalidCastException($"Cannot convert value of type '{_value.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Byte[]'");

            if (_value is null)
                return 0;

            var bytes = (byte[])_value;
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
        /// Implements the GetObject operation.
        /// </summary>
        /// <returns></returns>
        public object? GetObject()
        {
            return _value;
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'DateTime'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'DateTimeOffset'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'TimeSpan'"),
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
                java.math.BigDecimal bd => BigDecimalConverter.ToDecimal(bd),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Decimal'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Double'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Single'"),
            };
        }

        /// <summary>
        /// Implements the GetGuid operation. Calcite has no native UUID type; only valid when the
        /// underlying value is a string in canonical GUID form.
        /// </summary>
        /// <returns></returns>
        public Guid GetGuid()
        {
            return _value switch
            {
                string s when Guid.TryParse(s, out _) => Guid.Parse(s),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Guid'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Int16'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Int32'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Int64'"),
            };
        }

        /// <summary>
        /// Implements the GetByte operation. Accepts any numeric value that fits in a <see cref="byte"/>.
        /// </summary>
        public byte GetByte()
        {
            return _value switch
            {
                org.joou.UByte ub => (byte)ub.byteValue(),
                java.lang.Number n => checked((byte)n.longValue()),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'Byte'"),
            };
        }

        /// <summary>
        /// Implements the GetSByte operation. Accepts any numeric value that fits in a <see cref="sbyte"/>.
        /// </summary>
        public sbyte GetSByte()
        {
            return _value switch
            {
                java.lang.Byte by => (sbyte)by.byteValue(),
                java.lang.Number n => checked((sbyte)n.longValue()),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'SByte'"),
            };
        }

        /// <summary>
        /// Implements the GetUInt16 operation. Accepts any numeric value that fits in a <see cref="ushort"/>.
        /// </summary>
        public ushort GetUInt16()
        {
            return _value switch
            {
                org.joou.UShort us => (ushort)us.shortValue(),
                java.lang.Number n => checked((ushort)n.longValue()),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'UInt16'"),
            };
        }

        /// <summary>
        /// Implements the GetUInt32 operation. Accepts any numeric value that fits in a <see cref="uint"/>.
        /// </summary>
        public uint GetUInt32()
        {
            return _value switch
            {
                org.joou.UInteger ui => (uint)ui.intValue(),
                java.lang.Number n => checked((uint)n.longValue()),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'UInt32'"),
            };
        }

        /// <summary>
        /// Implements the GetUInt64 operation. Accepts any numeric value representable as a <see cref="ulong"/>.
        /// </summary>
        public ulong GetUInt64()
        {
            return _value switch
            {
                org.joou.ULong ul => (ulong)ul.longValue(),
                java.math.BigDecimal bd => (ulong)BigDecimalConverter.ToDecimal(bd),
                java.lang.Number n => (ulong)n.longValue(),
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'UInt64'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'DateOnly'"),
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
                _ => throw new InvalidCastException($"Cannot convert value of type '{_value?.GetType().Name}' with value '{_value}' (SQL type: {_sqlType}) to 'TimeOnly'"),
            };
        }

    }

}
