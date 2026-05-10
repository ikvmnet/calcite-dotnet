using System;

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

        readonly SqlTypeName.__Enum _sqlType;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="sqlType"></param>
        /// <param name="value"></param>
        public CalciteResultValue(SqlTypeName.__Enum sqlType, object? value)
        {
            _sqlType = sqlType;
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

            // Handle common ADO.NET types
            if (target == typeof(string))
                return (T)(object)GetString();
            if (target == typeof(char))
                return (T)(object)GetChar();
            if (target == typeof(byte[]))
                return (T)(object)(GetValue() as byte[] ?? throw new InvalidCastException());
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

            // Fallback: try to convert via GetValue if possible
            var val = GetValue();
            if (val is T tval)
                return tval;

            throw new InvalidCastException($"Cannot convert value of type '{_value.GetType().Name}' to '{typeof(T).Name}'");
        }

        /// <summary>
        /// Implements the GetValue operation.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            if (_value is null)
                return DBNull.Value;

            switch (_sqlType)
            {
                case SqlTypeName.__Enum.DATE:
                    {
                        return _value switch
                        {
                            java.lang.Integer i => UnixEpoch.AddDays(i.intValue()),
                            java.lang.Number n => UnixEpoch.AddDays(n.longValue()),
                            java.sql.Date d => UnixEpoch.AddMilliseconds(d.getTime()),
                            _ => _value,
                        };
                    }
                case SqlTypeName.__Enum.TIME:
                    {
                        return _value switch
                        {
                            java.lang.Integer i => TimeSpan.FromMilliseconds(i.intValue()),
                            java.lang.Number n => TimeSpan.FromMilliseconds(n.longValue()),
                            java.sql.Time t => TimeSpan.FromMilliseconds(t.getTime()),
                            _ => _value,
                        };
                    }
                case SqlTypeName.__Enum.TIME_WITH_LOCAL_TIME_ZONE:
                case SqlTypeName.__Enum.TIME_TZ:
                    {
                        // Calcite's wire form is a count of milliseconds-since-midnight; the offset is
                        // not carried per-row, so surface as DateTimeOffset at the epoch date with UTC offset
                        // (matches IKVM.Jdbc's OffsetTime path which anchors at 0001-01-01).
                        return _value switch
                        {
                            java.lang.Integer i => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(i.intValue())),
                            java.lang.Number n => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(n.longValue())),
                            java.sql.Time t => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(t.getTime())),
                            _ => _value,
                        };
                    }
                case SqlTypeName.__Enum.TIMESTAMP:
                    {
                        return _value switch
                        {
                            java.lang.Number n => UnixEpoch.AddMilliseconds(n.longValue()),
                            java.sql.Timestamp ts => UnixEpoch.AddMilliseconds(ts.getTime()),
                            _ => _value,
                        };
                    }
                case SqlTypeName.__Enum.TIMESTAMP_WITH_LOCAL_TIME_ZONE:
                case SqlTypeName.__Enum.TIMESTAMP_TZ:
                    {
                        // UTC instant; surface as DateTimeOffset with UTC offset.
                        return _value switch
                        {
                            java.lang.Number n => new DateTimeOffset(UnixEpoch.AddMilliseconds(n.longValue()), TimeSpan.Zero),
                            java.sql.Timestamp ts => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                            _ => _value,
                        };
                    }
                case SqlTypeName.__Enum.BINARY:
                case SqlTypeName.__Enum.VARBINARY:
                    {
                        return _value switch
                        {
                            org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
                            byte[] b => b,
                            _ => _value,
                        };
                    }
            }

            {
                if (_value is null) return DBNull.Value;
                if (_value is string) return _value;
                if (_value is java.math.BigDecimal bd) return BigDecimalConverter.ToDecimal(bd);
                if (_value is java.lang.Boolean b) return b.booleanValue();
                if (_value is java.lang.Byte by) return (sbyte)by.byteValue();
                if (_value is java.lang.Short sh) return sh.shortValue();
                if (_value is java.lang.Integer i) return i.intValue();
                if (_value is java.lang.Long l) return l.longValue();
                if (_value is java.lang.Float f) return f.floatValue();
                if (_value is java.lang.Double d) return d.doubleValue();
                if (_value is java.lang.Character c) return c.charValue();
                if (_value is java.sql.Timestamp ts) return UnixEpoch.AddMilliseconds(ts.getTime());
                if (_value is java.sql.Date dt) return UnixEpoch.AddMilliseconds(dt.getTime());
                if (_value is java.sql.Time tm) return TimeSpan.FromMilliseconds(tm.getTime());
                if (_value is org.apache.calcite.avatica.util.ByteString bs) return bs.getBytes();
            }

            return _value;
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
                _ => throw new InvalidCastException(),
            };
        }

        /// <summary>
        /// Implements the GetChar operation.
        /// </summary>
        /// <returns></returns>
        public char GetChar()
        {
            return _value switch
            {
                java.lang.Character c => c.charValue(),
                _ => throw new InvalidCastException(),
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
                throw new InvalidCastException();

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
                java.lang.Integer i when _sqlType == SqlTypeName.__Enum.DATE => UnixEpoch.AddDays(i.intValue()),
                java.sql.Date d when _sqlType == SqlTypeName.__Enum.DATE => UnixEpoch.AddMilliseconds(d.getTime()),
                java.lang.Long l when _sqlType == SqlTypeName.__Enum.TIMESTAMP => UnixEpoch.AddMilliseconds(l.longValue()),
                java.sql.Timestamp ts when _sqlType == SqlTypeName.__Enum.TIMESTAMP => UnixEpoch.AddMilliseconds(ts.getTime()),
                _ => throw new InvalidCastException(),
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
                java.lang.Long l when _sqlType is SqlTypeName.__Enum.TIMESTAMP_WITH_LOCAL_TIME_ZONE or SqlTypeName.__Enum.TIMESTAMP_TZ => new DateTimeOffset(UnixEpoch.AddMilliseconds(l.longValue()), TimeSpan.Zero),
                java.sql.Timestamp ts when _sqlType is SqlTypeName.__Enum.TIMESTAMP_WITH_LOCAL_TIME_ZONE or SqlTypeName.__Enum.TIMESTAMP_TZ => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                java.lang.Integer i when _sqlType is SqlTypeName.__Enum.TIME_WITH_LOCAL_TIME_ZONE or SqlTypeName.__Enum.TIME_TZ => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(i.intValue())),
                java.sql.Time t when _sqlType is SqlTypeName.__Enum.TIME_WITH_LOCAL_TIME_ZONE or SqlTypeName.__Enum.TIME_TZ => new DateTimeOffset(1, 1, 1, 0, 0, 0, TimeSpan.Zero).Add(TimeSpan.FromMilliseconds(t.getTime())),
                _ => throw new InvalidCastException(),
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
                java.lang.Integer i when _sqlType == SqlTypeName.__Enum.TIME => TimeSpan.FromMilliseconds(i.intValue()),
                java.sql.Time t when _sqlType == SqlTypeName.__Enum.TIME => TimeSpan.FromMilliseconds(t.getTime()),
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                _ => throw new InvalidCastException(),
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
                java.lang.Integer i when _sqlType == SqlTypeName.__Enum.DATE => DateOnly.FromDateTime(UnixEpoch.AddDays(i.intValue())),
                java.sql.Date d when _sqlType == SqlTypeName.__Enum.DATE => DateOnly.FromDateTime(UnixEpoch.AddMilliseconds(d.getTime())),
                _ => throw new InvalidCastException(),
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
                java.lang.Integer i when _sqlType == SqlTypeName.__Enum.TIME => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(i.intValue())),
                java.sql.Time t when _sqlType == SqlTypeName.__Enum.TIME => TimeOnly.FromTimeSpan(TimeSpan.FromMilliseconds(t.getTime())),
                _ => throw new InvalidCastException(),
            };
        }

    }

}
