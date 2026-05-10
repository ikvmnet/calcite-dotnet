using System;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Thin wrapper over an object returned by Calcite. Provides the final conversion methods to coerce the type to and from various CLR
    /// types.
    /// </summary>
    internal readonly struct CalciteResultValue
    {

        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);

        readonly string _providerTypeName;
        readonly object? _value;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="providerTypeName"></param>
        /// <param name="value"></param>
        public CalciteResultValue(string providerTypeName, object? value)
        {
            _providerTypeName = providerTypeName ?? throw new ArgumentNullException(nameof(providerTypeName));
            _value = value;
        }

        /// <summary>
        /// Gets the value of this column as the primary CLR type. This includes the DBNull result.
        /// </summary>
        /// <returns></returns>
        public object GetValue()
        {
            if (_value is null)
                return DBNull.Value;

            switch (_providerTypeName)
            {
                case "DATE":
                    {
                        return _value switch
                        {
                            java.lang.Integer i => UnixEpoch.AddDays(i.intValue()),
                            java.lang.Number n => UnixEpoch.AddDays(n.longValue()),
                            java.sql.Date d => UnixEpoch.AddMilliseconds(d.getTime()),
                            _ => _value,
                        };
                    }
                case "TIME":
                    {
                        return _value switch
                        {
                            java.lang.Integer i => TimeSpan.FromMilliseconds(i.intValue()),
                            java.lang.Number n => TimeSpan.FromMilliseconds(n.longValue()),
                            java.sql.Time t => TimeSpan.FromMilliseconds(t.getTime()),
                            _ => _value,
                        };
                    }
                case "TIME WITH LOCAL TIME ZONE":
                case "TIME WITH TIME ZONE":
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
                case "TIMESTAMP":
                    {
                        return _value switch
                        {
                            java.lang.Number n => UnixEpoch.AddMilliseconds(n.longValue()),
                            java.sql.Timestamp ts => UnixEpoch.AddMilliseconds(ts.getTime()),
                            _ => _value,
                        };
                    }
                case "TIMESTAMP WITH LOCAL TIME ZONE":
                case "TIMESTAMP WITH TIME ZONE":
                    {
                        // UTC instant; surface as DateTimeOffset with UTC offset.
                        return _value switch
                        {
                            java.lang.Number n => new DateTimeOffset(UnixEpoch.AddMilliseconds(n.longValue()), TimeSpan.Zero),
                            java.sql.Timestamp ts => new DateTimeOffset(UnixEpoch.AddMilliseconds(ts.getTime()), TimeSpan.Zero),
                            _ => _value,
                        };
                    }
                case "BINARY":
                case "VARBINARY":
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
            if (_value is string)
                return (string)_value;
            else
                throw new InvalidCastException();
        }

        /// <summary>
        /// Implements the GetChar operation.
        /// </summary>
        /// <returns></returns>
        public char GetChar()
        {
            if (_value is java.lang.Character c)
                return c.charValue();
            else
                throw new InvalidCastException();
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
        /// Implements the GetDateTime operation.
        /// </summary>
        /// <returns></returns>
        public DateTime GetDateTime()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetDateTimeOffset operation.
        /// </summary>
        /// <returns></returns>
        public DateTimeOffset GetDateTimeOffset()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetTimeSpan operation.
        /// </summary>
        /// <returns></returns>
        public TimeSpan GetTimeSpan()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetDecimal operation.
        /// </summary>
        /// <returns></returns>
        public decimal GetDecimal()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetDouble operation.
        /// </summary>
        /// <returns></returns>
        public double GetDouble()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetFloat operation.
        /// </summary>
        /// <returns></returns>
        public float GetFloat()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetGuid operation.
        /// </summary>
        /// <returns></returns>
        public Guid GetGuid()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetInt16 operation.
        /// </summary>
        /// <returns></returns>
        public short GetInt16()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetInt32 operation.
        /// </summary>
        /// <returns></returns>
        public int GetInt32()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetInt64 operation.
        /// </summary>
        /// <returns></returns>
        public long GetInt64()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetDateOnly operation.
        /// </summary>
        /// <returns></returns>
        public DateOnly GetDateOnly()
        {
            throw new NotImplementedException();
        }

        /// <summary>
        /// Implements the GetTimeOnly operation.
        /// </summary>
        /// <returns></returns>
        public TimeOnly GetTimeOnly()
        {
            throw new NotImplementedException();
        }

    }

}
