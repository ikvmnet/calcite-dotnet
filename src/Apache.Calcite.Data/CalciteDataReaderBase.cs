using System;
using System.Collections;
using System.Data.Common;
using System.Data.SqlTypes;
using System.IO;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public abstract class CalciteDataReaderBase : DbDataReader
    {

        readonly CalciteCommand _command;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="command"></param>
        internal CalciteDataReaderBase(CalciteCommand command)
        {
            throw new NotImplementedException();
        }

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
        public override int FieldCount => throw new NotImplementedException();

        /// <summary>
        /// Gets a value that indiciates whether this <see cref="CalciteDataReaderBase"/> has one or more rows.
        /// </summary>
        public override bool HasRows => throw new NotImplementedException();

        /// <summary>
        /// Gets a value indicating whether this <see cref="CalciteDataReaderBase"/> is closed.
        /// </summary>
        public override bool IsClosed => throw new NotImplementedException();

        /// <summary>
        /// Gets the number of rows changed, inserted or deleted by the SQL statement.
        /// </summary>
        public override int RecordsAffected => throw new NotImplementedException();

        /// <summary>
        /// Gets an <see cref="IEnumerator"/> that can be used to iterate through the rows in the <see cref="CalciteDataReaderBase"/>.
        /// </summary>
        /// <returns></returns>
        public override IEnumerator GetEnumerator()
        {
            return new CalciteDbEnumerator(this);
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
        }

        /// <summary>
        /// Gets the value of the specified column as an instance of <see cref="object"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public override object GetValue(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
#pragma warning disable CS8603
        public override T GetFieldValue<T>(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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
        /// Gets the value of the specified column as a <see cref="Nullable{Boolean}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        bool? GetNullableBoolean(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            return GetNullableByte(ordinal) ?? throw new SqlNullValueException();
        }

        /// Gets the value of the specified column as a <see cref="Nullable{Byte}"/>.
        byte? GetNullableByte(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
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

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            return GetNullableDateTime(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{DateTime}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        DateTime? GetNullableDateTime(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            return GetNullableDecimal(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{decimal}"/>.
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

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            return GetNullableDouble(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{double}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        double? GetNullableDouble(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            return GetNullableFloat(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{float}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        float? GetNullableFloat(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            return GetNullableInt16(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{short}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        short? GetNullableInt16(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            return GetNullableInt32(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{int}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        int? GetNullableInt32(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            return GetNullableInt64(ordinal) ?? throw new SqlNullValueException();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Nullable{long}"/>.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentOutOfRangeException"></exception>
        /// <exception cref="NotImplementedException"></exception>
        long? GetNullableInt64(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override Stream GetStream(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override TextReader GetTextReader(int ordinal)
        {
            if (ordinal < 0)
                throw new ArgumentOutOfRangeException(nameof(ordinal));

            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override bool Read()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            throw new NotImplementedException();
        }

        /// <inheritdoc />
        public override void Close()
        {
            throw new NotImplementedException();
        }

    }

}
