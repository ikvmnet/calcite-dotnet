using System;
using System.Buffers;
using System.Collections;
using System.Collections.Generic;
using System.Data.Common;
using System.Data.SqlTypes;
using System.Globalization;
using System.IO;
using System.Xml.Linq;

using java.sql;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public abstract class CalciteDataReaderBase : DbDataReader
    {

        const int DEFAULT_BUFFER_SIZE = 1024;

        readonly CalciteCommand _command;
        readonly object _statement;

        IEnumerator<object> _iter;
        int _updateCount;
        ResultSet? _resultSet;
        bool _hasRows;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="statement"></param>
        internal CalciteDataReaderBase(CalciteCommand command, object statement)
        {
            _command = command ?? throw new ArgumentNullException(nameof(command));
            _statement = statement ?? throw new ArgumentNullException(nameof(statement));
            _iter = GetResultIter().GetEnumerator();

            if (AdvanceNextResultSet() == false)
                throw new InvalidOperationException("No active result set.");
        }

        /// <summary>
        /// Advances the iter to the next result set.
        /// </summary>
        bool AdvanceNextResultSet()
        {
            try
            {
                // close existing result set
                if (_resultSet is not null)
                {
                    _resultSet.close();
                    _resultSet = null;
                }

                // invoke move next until we arrive at a ResultSet
                // updating _updateCount each step to preserve affected records
                do
                {
                    // move to next record
                    if (_iter.MoveNext() == false)
                        return false;

                    switch (_iter.Current)
                    {
                        case int updateCount:
                            _updateCount = updateCount;
                            break;
                        case ResultSet resultSet:
                            _resultSet = resultSet;
                            _hasRows = _resultSet.isBeforeFirst();
                            break;
                        default:
                            throw new InvalidOperationException();
                    }
                }
                while (_resultSet is null);

                // we advanced to a result set, eventually
                return _resultSet is not null;
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the active result set.
        /// </summary>
        ResultSet ResultSet => _resultSet ?? throw new InvalidOperationException("No active result set.");

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override object this[int ordinal] => GetValue(ordinal);

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override object this[string name] => GetValue(GetOrdinal(name));

        /// <summary>
        /// Gets a value indicating the depth of nesting for the current row.
        /// </summary>
        public override int Depth => 0;

        /// <summary>
        /// Gets the number of columns in the current row.
        /// </summary>
        public override int FieldCount => ResultSet.getMetaData().getColumnCount();

        /// <summary>
        /// Gets a value that indiciates whether this <see cref="CalciteDataReaderBase"/> has one or more rows.
        /// </summary>
        public override bool HasRows => _hasRows;

        /// <summary>
        /// Gets a value indicating whether this <see cref="CalciteDataReaderBase"/> is closed.
        /// </summary>
        public override bool IsClosed => ResultSet.isClosed();

        /// <summary>
        /// Gets the number of rows changed, inserted or deleted by the SQL statement.
        /// </summary>
        public override int RecordsAffected => _updateCount;

        /// <summary>
        /// Gets an <see cref="IEnumerator"/> that can be used to iterate through the rows in the <see cref="CalciteDataReaderBase"/>.
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            return new CalciteEnumerator(this);
        }

        /// <summary>
        /// Gets the data type of the specified column.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        public override Type GetFieldType(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return ResultSet.getMetaData().getColumnType(column) switch
                {
                    Types.ARRAY => typeof(System.Array),
                    Types.BIGINT => typeof(long),
                    Types.BINARY => typeof(byte[]),
                    Types.BIT => typeof(bool),
                    Types.BLOB => typeof(byte[]),
                    Types.BOOLEAN => typeof(bool),
                    Types.CHAR => typeof(string),
                    Types.CLOB => typeof(string),
                    Types.DATALINK => throw new NotSupportedException(),
#if NETFRAMEWORK
                    Types.DATE => typeof(DateTime),
#else
                    Types.DATE => typeof(DateOnly),
#endif
                    Types.DECIMAL => typeof(decimal),
                    Types.DISTINCT => throw new NotImplementedException(),
                    Types.DOUBLE => typeof(double),
                    Types.FLOAT => typeof(float),
                    Types.INTEGER => typeof(int),
                    Types.JAVA_OBJECT => typeof(object),
                    Types.LONGNVARCHAR => typeof(string),
                    Types.LONGVARBINARY => typeof(byte[]),
                    Types.LONGVARCHAR => typeof(string),
                    Types.NCHAR => typeof(string),
                    Types.NCLOB => typeof(string),
                    Types.NULL => typeof(object),
                    Types.NUMERIC => throw new NotImplementedException(),
                    Types.NVARCHAR => typeof(string),
                    Types.OTHER => throw new NotSupportedException(),
                    Types.REAL => typeof(float),
                    Types.REF => throw new NotSupportedException(),
                    Types.REF_CURSOR => throw new NotSupportedException(),
                    Types.ROWID => throw new NotImplementedException(),
                    Types.SMALLINT => typeof(short),
                    Types.SQLXML => typeof(XDocument),
                    Types.STRUCT => throw new NotSupportedException(),
                    Types.TIME => typeof(TimeSpan),
                    Types.TIMESTAMP => typeof(DateTime),
                    Types.TIMESTAMP_WITH_TIMEZONE => typeof(DateTimeOffset),
                    Types.TIME_WITH_TIMEZONE => typeof(DateTimeOffset),
                    Types.TINYINT => typeof(byte),
                    Types.VARBINARY => typeof(byte[]),
                    Types.VARCHAR => typeof(string),
                    _ => throw new NotSupportedException(),
                };
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the name of the data type of the specified column.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override string GetDataTypeName(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return ResultSet.getMetaData().getColumnTypeName(column);
            }
            catch (java.lang.Exception e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the name of the column, given the zero based column ordinal.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override string GetName(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                return ResultSet.getMetaData().getColumnName(column);
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the column ordinal given the name of the column.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public override int GetOrdinal(string name)
        {
            if (name is null)
                throw new ArgumentNullException(nameof(name));

            try
            {
                return ResultSet.findColumn(name) is int i && i > 0 ? i - 1 : -1;
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="column"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public override object GetValue(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.ARRAY:
                        var array_ = ResultSet.getArray(column);
                        return ResultSet.wasNull() ? DBNull.Value : array_.getArray();
                    case Types.BIGINT:
                        var long_ = ResultSet.getLong(column);
                        return ResultSet.wasNull() ? DBNull.Value : long_;
                    case Types.BINARY:
                        var binary_ = ResultSet.getBytes(column);
                        return ResultSet.wasNull() ? DBNull.Value : binary_;
                    case Types.BIT:
                        var bit_ = ResultSet.getBoolean(column);
                        return ResultSet.wasNull() ? DBNull.Value : bit_;
                    case Types.BLOB:
                        var blob_ = ResultSet.getBytes(column);
                        return ResultSet.wasNull() ? DBNull.Value : blob_;
                    case Types.BOOLEAN:
                        var bool_ = ResultSet.getBoolean(column);
                        return ResultSet.wasNull() ? DBNull.Value : bool_;
                    case Types.CHAR:
                        var char_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : char_;
                    case Types.CLOB:
                        var clob_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : clob_;
                    case Types.DATALINK:
                        throw new NotSupportedException();
                    case Types.DATE:
#if NETFRAMEWORK
                        var date_ = ResultSet.getDate(column);
                        return ResultSet.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).Date;
#else
                        var date_ = ResultSet.getDate(column);
                        return ResultSet.wasNull() ? DBNull.Value : DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).Date);
#endif
                    case Types.DECIMAL:
                        var decimal_ = ResultSet.getBigDecimal(column);
                        return ResultSet.wasNull() ? DBNull.Value : decimal.Parse(decimal_.toString());
                    case Types.DISTINCT:
                        throw new NotImplementedException();
                    case Types.DOUBLE:
                        var double_ = ResultSet.getDouble(column);
                        return ResultSet.wasNull() ? DBNull.Value : double_;
                    case Types.FLOAT:
                        var float_ = ResultSet.getFloat(column);
                        return ResultSet.wasNull() ? DBNull.Value : float_;
                    case Types.INTEGER:
                        var integer_ = ResultSet.getInt(column);
                        return ResultSet.wasNull() ? DBNull.Value : integer_;
                    case Types.JAVA_OBJECT:
                        var object_ = ResultSet.getObject(column);
                        return ResultSet.wasNull() ? DBNull.Value : object_;
                    case Types.LONGNVARCHAR:
                        var longnvarchar_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : longnvarchar_;
                    case Types.LONGVARBINARY:
                        var longvarbinary_ = ResultSet.getBytes(column);
                        return ResultSet.wasNull() ? DBNull.Value : longvarbinary_;
                    case Types.LONGVARCHAR:
                        var longvarchar_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : longvarchar_;
                    case Types.NCHAR:
                        var nchar_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : nchar_;
                    case Types.NCLOB:
                        var nclob_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : nclob_;
                    case Types.NULL:
                        return DBNull.Value;
                    case Types.NUMERIC:
                        throw new NotImplementedException();
                    case Types.NVARCHAR:
                        var nvarchar_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : nvarchar_;
                    case Types.OTHER:
                        throw new NotSupportedException();
                    case Types.REAL:
                        var real_ = ResultSet.getFloat(column);
                        return ResultSet.wasNull() ? DBNull.Value : real_;
                    case Types.REF:
                        throw new NotSupportedException();
                    case Types.REF_CURSOR:
                        throw new NotSupportedException();
                    case Types.ROWID:
                        throw new NotImplementedException();
                    case Types.SMALLINT:
                        var smallint_ = ResultSet.getShort(column);
                        return ResultSet.wasNull() ? DBNull.Value : smallint_;
                    case Types.SQLXML:
                        var sqlxml_ = ResultSet.getSQLXML(column);
                        return ResultSet.wasNull() ? DBNull.Value : XDocument.Parse(sqlxml_.getString());
                    case Types.STRUCT:
                        throw new NotSupportedException();
                    case Types.TIME:
#if NETFRAMEWORK
                        var time_ = ResultSet.getTime(column);
                        return ResultSet.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(time_.getTime()).TimeOfDay;
#else
                        var time_ = ResultSet.getTime(column);
                        return ResultSet.wasNull() ? DBNull.Value : TimeOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(time_.getTime()).DateTime);
#endif
                    case Types.TIMESTAMP:
                        var timestamp_ = ResultSet.getTimestamp(column);
                        return ResultSet.wasNull() ? DBNull.Value : DateTimeOffset.FromUnixTimeMilliseconds(timestamp_.getTime()).DateTime;
                    case Types.TIMESTAMP_WITH_TIMEZONE:
                        throw new NotImplementedException();
                    case Types.TIME_WITH_TIMEZONE:
                        throw new NotImplementedException();
                    case Types.TINYINT:
                        var tinyint_ = ResultSet.getByte(column);
                        return ResultSet.wasNull() ? DBNull.Value : tinyint_;
                    case Types.VARBINARY:
                        var varbinary_ = ResultSet.getBytes(column);
                        return ResultSet.wasNull() ? DBNull.Value : varbinary_;
                    case Types.VARCHAR:
                        var varchar_ = ResultSet.getString(column);
                        return ResultSet.wasNull() ? DBNull.Value : varchar_;
                    default:
                        throw new NotSupportedException();
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
#pragma warning disable CS8603
        public override T GetFieldValue<T>(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                if (typeof(T) == typeof(bool))
                {
                    var value = GetBoolean(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(bool?))
                {
                    var value = GetNullableBoolean(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(byte))
                {
                    var value = GetByte(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(byte?))
                {
                    var value = GetNullableByte(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(short))
                {
                    var value = GetInt16(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(short?))
                {
                    var value = GetNullableInt16(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(int))
                {
                    var value = GetInt32(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(int?))
                {
                    var value = GetNullableInt32(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(long))
                {
                    var value = GetInt64(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(long?))
                {
                    var value = GetNullableInt64(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(float))
                {
                    var value = GetFloat(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(float?))
                {
                    var value = GetNullableFloat(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(double))
                {
                    var value = GetDouble(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(double?))
                {
                    var value = GetNullableDouble(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(decimal))
                {
                    var value = GetDecimal(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(decimal?))
                {
                    var value = GetNullableDecimal(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(string))
                {
                    var value = GetString(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(DateTime))
                {
                    var value = GetDateTime(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(DateTime?))
                {
                    var value = GetNullableDateTime(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(TimeSpan))
                {
                    var value = GetTimeSpan(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(TimeSpan?))
                {
                    var value = GetNullableTimeSpan(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
#if NET
                else if (typeof(T) == typeof(DateOnly))
                {
                    var value = GetDateOnly(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(DateOnly?))
                {
                    var value = GetNullableDateOnly(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(TimeOnly))
                {
                    var value = GetTimeOnly(ordinal);
                    return (T)(object)value;
                }
                else if (typeof(T) == typeof(TimeOnly?))
                {
                    var value = GetNullableTimeOnly(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
#endif
                else if (typeof(T) == typeof(byte[]))
                {
                    var value = GetBytes(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else if (typeof(T) == typeof(char[]))
                {
                    var value = GetChars(ordinal);
                    return value is not null ? (T)(object)value : default;
                }
                else
                {
                    return (T)GetValue(ordinal);
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }
#pragma warning restore CS8603

        /// <summary>
        /// Populates an array of <see cref="object"/> with the column values of the current row.
        /// </summary>
        /// <param name="values"></param>
        /// <returns></returns>
        public override int GetValues(object[] values)
        {
            if (values is null)
                throw new ArgumentNullException(nameof(values));

            try
            {
                var n = ResultSet.getMetaData().getColumnCount();
                for (int i = 0; i < n; i++)
                    values[i] = GetValue(i);

                return n;
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets a value that indicates whether the column contains non-existent or null values.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool IsDBNull(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                GetValue(ordinal);
                return ResultSet.wasNull();
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override bool GetBoolean(int ordinal)
        {
            return GetNullableBoolean(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool?"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        bool? GetNullableBoolean(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BOOLEAN:
                        var bool_ = ResultSet.getBoolean(column);
                        if (ResultSet.wasNull())
                            return null;

                        return bool_;
                    case Types.BIT:
                        var bit_ = ResultSet.getBoolean(column);
                        if (ResultSet.wasNull())
                            return null;

                        return bit_;
                    case Types.TINYINT:
                        var byte_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            return null;

                        return byte_ != 0;
                    case Types.SMALLINT:
                        var short_ = ResultSet.getShort(column);
                        if (ResultSet.wasNull())
                            return null;

                        return short_ != 0;
                    case Types.INTEGER:
                        var int_ = ResultSet.getInt(column);
                        if (ResultSet.wasNull())
                            return null;

                        return int_ != 0;
                    case Types.BIGINT:
                        var long_ = ResultSet.getLong(column);
                        if (ResultSet.wasNull())
                            return null;

                        return long_ != 0;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Boolean.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            return GetNullableByte(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        byte? GetNullableByte(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BOOLEAN:
                        var bool_ = ResultSet.getBoolean(column);
                        if (ResultSet.wasNull())
                            return null;

                        return bool_ ? (byte)1 : (byte)0;
                    case Types.BIT:
                        var bit_ = ResultSet.getBoolean(column);
                        if (ResultSet.wasNull())
                            return null;

                        return bit_ ? (byte)1 : (byte)0;
                    case Types.TINYINT:
                        var byte_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            return null;

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Byte.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a byte array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public byte[]? GetBytes(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var b = ResultSet.getBytes(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return b;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Byte[].");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// buffer, starting at the location indicated by <paramref name="bufferOffset"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var stream = ResultSet.getBinaryStream(column);
                        if (ResultSet.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            int n = 0; // total read
                            int i = 0; // current read

                            // read up to buffer size, from buffer offset, or remaining space available, until end
                            while ((i = stream.read(buffer, n + bufferOffset, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                n += i;

                            return n;
                        }
                    default:
                        throw new CalciteDbException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// <see cref="Span{Byte}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public long GetBytes(int ordinal, long dataOffset, Span<byte> buffer)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var stream = ResultSet.getBinaryStream(column);
                        if (ResultSet.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            var b = ArrayPool<byte>.Shared.Rent(DEFAULT_BUFFER_SIZE);

                            try
                            {
                                int n = 0; // total read
                                int i = 0; // current read

                                // read up to buffer size, or remaining space available, until end
                                while ((i = stream.read(b, 0, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                {
                                    b.AsSpan().Slice(0, i).CopyTo(buffer.Slice(n));
                                    n += i;
                                }

                                return n;
                            }
                            finally
                            {
                                if (b is not null)
                                    ArrayPool<byte>.Shared.Return(b);
                            }
                        }
                    default:
                        throw new CalciteDbException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column as a single <see cref="char"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public override char GetChar(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                        var char_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return (char)char_;
                    case Types.NCHAR:
                        var nchar_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return (char)nchar_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Char.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Gets the value of the specified column ordinal as a char array.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        public char[]? GetChars(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                        var b = ResultSet.getString(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return b.ToCharArray();
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Byte[].");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Reads a stream of characters from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// buffer, starting at the location indicated by <paramref name="bufferOffset"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <param name="bufferOffset"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        /// <exception cref="CalciteDbException"></exception>
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));
            if (buffer is null)
                throw new ArgumentNullException(nameof(buffer));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        var stream = ResultSet.getCharacterStream(column);
                        if (ResultSet.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            int n = 0; // total read
                            int i = 0; // current read

                            // read up to buffer size, from buffer offset, or remaining space available, until end
                            while ((i = stream.read(buffer, n + bufferOffset, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                n += i;

                            return n;
                        }
                    default:
                        throw new CalciteDbException($"Could not retrieve chars.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <summary>
        /// Reads a stream of bytes from the specified column, starting at location indicated by <paramref name="dataOffset"/>, into the
        /// <see cref="Span{Char}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="dataOffset"></param>
        /// <param name="buffer"></param>
        /// <returns></returns>
        public long GetChars(int ordinal, long dataOffset, Span<char> buffer)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (dataOffset < 0)
                throw new ArgumentOutOfRangeException(nameof(dataOffset));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                    case Types.VARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.CLOB:
                        var stream = ResultSet.getCharacterStream(column);
                        if (ResultSet.wasNull())
                        {
                            return 0;
                        }
                        else
                        {
                            // skip until we consume offset bytes
                            while (dataOffset > 0)
                                dataOffset -= stream.skip(dataOffset);

                            var b = ArrayPool<char>.Shared.Rent(DEFAULT_BUFFER_SIZE);

                            try
                            {
                                int n = 0; // total read
                                int i = 0; // current read

                                // read up to buffer size, or remaining space available, until end
                                while ((i = stream.read(b, 0, Math.Min(DEFAULT_BUFFER_SIZE, buffer.Length - n))) != -1)
                                {
                                    b.AsSpan().Slice(0, i).CopyTo(buffer.Slice(n));
                                    n += i;
                                }

                                return n;
                            }
                            finally
                            {
                                if (b is not null)
                                    ArrayPool<char>.Shared.Return(b);
                            }
                        }
                    default:
                        throw new CalciteDbException($"Could not retrieve bytes.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
            catch (java.io.IOException e)
            {
                throw new System.IO.IOException(e.getMessage(), e);
            }
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            return GetNullableDateTime(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        DateTime? GetNullableDateTime(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.DATE:
                        var date_ = ResultSet.getDate(column);
                        return ResultSet.wasNull() ? null : DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).DateTime;
                    case Types.TIME:
                        var time_ = ResultSet.getTime(column);
                        return ResultSet.wasNull() ? null : DateTimeOffset.FromUnixTimeMilliseconds(time_.getTime()).DateTime;
                    case Types.TIMESTAMP:
                        var timestamp_ = ResultSet.getTimestamp(column);
                        return ResultSet.wasNull() ? null : DateTimeOffset.FromUnixTimeMilliseconds(timestamp_.getTime()).DateTime;
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        var string_ = ResultSet.getString(column);
                        if (ResultSet.wasNull())
                            return null;

                        // default to assuming the local timezone
                        var styles = DateTimeStyles.AssumeLocal;
                        if (_command.Connection?.ConnectionStringBuilder?.AssumeLocalTimeZone == false)
                            styles = DateTimeStyles.AssumeUniversal;

                        // parse as datetimeoffset, and then return dateonly value
                        if (DateTimeOffset.TryParse(string_, null, styles, out var d))
                            return d.DateTime;

                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into DateTime.");
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into DateTime.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        TimeSpan GetTimeSpan(int ordinal)
        {
            return GetNullableTimeSpan(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        TimeSpan? GetNullableTimeSpan(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.TIME:
                        var date_ = ResultSet.getDate(column);
                        if (ResultSet.wasNull())
                            return null;

                        return TimeSpan.FromMilliseconds(date_.getTime());
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into TimeSpan.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

#if NET

        /// <inheritdoc />
        DateOnly GetDateOnly(int ordinal)
        {
            return GetNullableDateOnly(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        DateOnly? GetNullableDateOnly(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.DATE:
                        var date_ = ResultSet.getDate(column);
                        if (ResultSet.wasNull())
                            return null;

                        return DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).DateTime);
                    case Types.TIMESTAMP:
                        var timestamp_ = ResultSet.getTimestamp(column);
                        if (ResultSet.wasNull())
                            return null;

                        return DateOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(timestamp_.getTime()).DateTime);
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        var string_ = ResultSet.getString(column);
                        if (ResultSet.wasNull())
                            return null;

                        // default to assuming the local timezone
                        var styles = DateTimeStyles.AssumeLocal;
                        if (_command.Connection?.ConnectionStringBuilder?.AssumeLocalTimeZone == false)
                            styles = DateTimeStyles.AssumeUniversal;

                        // parse as datetimeoffset, and then return dateonly value
                        if (DateTimeOffset.TryParse(string_, null, styles, out var d))
                            return DateOnly.FromDateTime(d.DateTime);

                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into DateOnly.");
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into DateOnly.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        TimeOnly GetTimeOnly(int ordinal)
        {
            return GetNullableTimeOnly(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        TimeOnly? GetNullableTimeOnly(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.TIME:
                        var date_ = ResultSet.getDate(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return TimeOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(date_.getTime()).DateTime);
                    case Types.TIMESTAMP:
                        var timestamp_ = ResultSet.getTimestamp(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return TimeOnly.FromDateTime(DateTimeOffset.FromUnixTimeMilliseconds(timestamp_.getTime()).DateTime);
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into TimeOnly.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

#endif

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            return GetNullableDecimal(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the <see cref="decimal?"/> value at the specified <paramref name="ordinal"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="SqlTypeException"></exception>
        /// <exception cref="CalciteDbException"></exception>
        decimal? GetNullableDecimal(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.DECIMAL:
                        var decimal_ = ResultSet.getBigDecimal(column);
                        if (ResultSet.wasNull())
                            return null;

                        return decimal.Parse(decimal_.ToString());
                    case Types.DOUBLE:
                        var double_ = ResultSet.getDouble(column);
                        if (ResultSet.wasNull())
                            return null;

                        return (decimal)double_;
                    case Types.FLOAT:
                        var float_ = ResultSet.getFloat(column);
                        if (ResultSet.wasNull())
                            return null;

                        return (decimal)float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Decimal.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            return GetNullableDouble(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        double? GetNullableDouble(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.DOUBLE:
                        var double_ = ResultSet.getDouble(column);
                        if (ResultSet.wasNull())
                            return null;

                        return double_;
                    case Types.FLOAT:
                        var float_ = ResultSet.getFloat(column);
                        if (ResultSet.wasNull())
                            return null;

                        return float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Double.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            return GetNullableFloat(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        float? GetNullableFloat(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.FLOAT:
                        var float_ = ResultSet.getFloat(column);
                        if (ResultSet.wasNull())
                            return null;

                        return float_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Single.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            return Guid.Parse((string)GetValue(ordinal));
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            return GetNullableInt16(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        short? GetNullableInt16(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.SMALLINT:
                        var short_ = ResultSet.getShort(column);
                        if (ResultSet.wasNull())
                            return null;

                        return short_;
                    case Types.TINYINT:
                        var byte_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            return null;

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Int16.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            return GetNullableInt32(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        int? GetNullableInt32(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.INTEGER:
                        var int_ = ResultSet.getInt(column);
                        if (ResultSet.wasNull())
                            return null;

                        return int_;
                    case Types.SMALLINT:
                        var short_ = ResultSet.getShort(column);
                        if (ResultSet.wasNull())
                            return null;

                        return short_;
                    case Types.TINYINT:
                        var byte_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            return null;

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Int32.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            return GetNullableInt64(ordinal) ?? throw new SqlNullValueException();
        }

        /// <inheritdoc />
        long? GetNullableInt64(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BIGINT:
                        var long_ = ResultSet.getLong(column);
                        if (ResultSet.wasNull())
                            return null;

                        return long_;
                    case Types.INTEGER:
                        var int_ = ResultSet.getInt(column);
                        if (ResultSet.wasNull())
                            return null;

                        return int_;
                    case Types.SMALLINT:
                        var short_ = ResultSet.getShort(column);
                        if (ResultSet.wasNull())
                            return null;

                        return short_;
                    case Types.TINYINT:
                        var byte_ = ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            return null;

                        return byte_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into Int64.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                        var char_ = (char)ResultSet.getByte(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return char_.ToString();
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                    case Types.CLOB:
                    case Types.NCLOB:
                        var string_ = ResultSet.getString(column);
                        if (ResultSet.wasNull())
                            throw new SqlNullValueException();

                        return string_;
                    default:
                        throw new SqlTypeException($"Could not convert column type {ResultSet.getMetaData().getColumnTypeName(column)} into String.");
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override Stream GetStream(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.BINARY:
                    case Types.VARBINARY:
                    case Types.LONGVARBINARY:
                    case Types.BLOB:
                        var binaryStream = ResultSet.getBinaryStream(column);
                        return ResultSet.wasNull() ? Stream.Null : new CalciteInputStream(binaryStream, binaryStream.available());
                    default:
                        return base.GetStream(ordinal);
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override TextReader GetTextReader(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));
            if (ordinal >= ResultSet.getMetaData().getColumnCount())
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            try
            {
                var column = ordinal + 1;
                switch (ResultSet.getMetaData().getColumnType(column))
                {
                    case Types.CHAR:
                    case Types.NCHAR:
                        var char_ = ResultSet.getByte(column);
                        return ResultSet.wasNull() ? TextReader.Null : new StringReader(((char)char_).ToString());
                    case Types.VARCHAR:
                    case Types.NVARCHAR:
                    case Types.LONGVARCHAR:
                    case Types.LONGNVARCHAR:
                        var reader_ = ResultSet.getCharacterStream(column);
                        return ResultSet.wasNull() ? TextReader.Null : new CalciteTextReader(reader_);
                    case Types.CLOB:
                        var clob = ResultSet.getClob(column);
                        return ResultSet.wasNull() ? TextReader.Null : new CalciteClobTextReader(ResultSet.getClob(column));
                    case Types.NCLOB:
                        var nclob = ResultSet.getNClob(column);
                        return ResultSet.wasNull() ? TextReader.Null : new CalciteClobTextReader(ResultSet.getClob(column));
                    default:
                        return base.GetTextReader(ordinal);
                }
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <inheritdoc />
        public override bool Read()
        {
            try
            {
                return _resultSet is not null && _resultSet.next();
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

        /// <summary>
        /// Returns an <see cref="IEnumerable{ResultSet}"/> over the standard results available on a statement.
        /// </summary>
        /// <returns></returns>
        IEnumerable<object> GetStatementResultIter()
        {
            throw new NotImplementedException();

            //// first result
            //var updateCount = _statement.getUpdateCount();

            //do
            //{
            //    if (updateCount == -1)
            //        yield return _statement.getResultSet();
            //    else
            //        yield return updateCount;

            //    // clear update count
            //    updateCount = -1;
            //}
            //while (_statement.getMoreResults() || (updateCount = _statement.getUpdateCount()) != -1);
        }

        /// <summary>
        /// Returns an <see cref="IEnumerable{ResultSet}"/> over the complete results available on a statement, including any appended results.
        /// </summary>
        /// <returns></returns>
        IEnumerable<object> GetResultIter()
        {
            foreach (var i in GetStatementResultIter())
                yield return i;
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            return AdvanceNextResultSet();
        }

        /// <inheritdoc />
        public override void Close()
        {
            try
            {
                _resultSet?.close();
                _hasRows = false;
                base.Close();
            }
            catch (SQLException e)
            {
                throw new CalciteDbException(e);
            }
        }

    }

}
