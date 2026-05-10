using System;
using System.Collections;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Data.Internal;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Provides a way of reading a forward-only stream of rows produced by executing a
    /// <see cref="CalciteCommand"/>. This class cannot be inherited.
    /// </summary>
    public sealed class CalciteDataReader : DbDataReader
    {

        readonly CalciteResult _result;
        readonly CommandBehavior _behavior;
        bool _closed;
        bool _hasRow;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="behavior"></param>
        /// <exception cref="ArgumentNullException"></exception>
        internal CalciteDataReader(CalciteResult result, CommandBehavior behavior)
        {
            _result = result ?? throw new ArgumentNullException(nameof(result));
            _behavior = behavior;
        }

        /// <inheritdoc />
        public override object this[int ordinal] => GetValue(ordinal);

        /// <inheritdoc />
        public override object this[string name] => GetValue(GetOrdinal(name));

        /// <inheritdoc />
        public override int Depth => 0;

        /// <inheritdoc />
        public override int FieldCount
        {
            get
            {
                ThrowIfClosed();
                return _result.Columns.Count;
            }
        }

        /// <inheritdoc />
        public override bool HasRows => _hasRow;

        /// <inheritdoc />
        public override bool IsClosed => _closed;

        /// <inheritdoc />
        public override int RecordsAffected
        {
            get
            {
                var v = _result.RecordsAffected;
                if (v > int.MaxValue) return int.MaxValue;
                if (v < int.MinValue) return int.MinValue;
                return (int)v;
            }
        }

        /// <inheritdoc />
        public override bool IsDBNull(int ordinal)
        {
            ThrowIfNoRow();
            return _result.Current.GetValue(ordinal) is null;
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            return false;
        }

        /// <inheritdoc />
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(false);
        }

        /// <inheritdoc />
        public override bool Read() => ReadAsync(CancellationToken.None).GetAwaiter().GetResult();

        /// <inheritdoc />
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfClosed();
            _hasRow = await _result.ReadAsync(cancellationToken).ConfigureAwait(false);
            return _hasRow;
        }

        /// <inheritdoc />
        public override DataTable GetSchemaTable()
        {
            ThrowIfClosed();

            var dt = new DataTable("SchemaTable");
            dt.Columns.Add(SchemaTableColumn.ColumnName, typeof(string));
            dt.Columns.Add(SchemaTableColumn.ColumnOrdinal, typeof(int));
            dt.Columns.Add(SchemaTableColumn.DataType, typeof(Type));
            dt.Columns.Add(SchemaTableColumn.ProviderType, typeof(string));
            dt.Columns.Add(SchemaTableColumn.AllowDBNull, typeof(bool));

            for (var i = 0; i < _result.Columns.Count; i++)
            {
                var row = dt.NewRow();
                row[SchemaTableColumn.ColumnName] = _result.Columns.GetName(i);
                row[SchemaTableColumn.ColumnOrdinal] = i;
                row[SchemaTableColumn.DataType] = _result.Columns.GetClrType(i);
                row[SchemaTableColumn.ProviderType] = _result.Columns.GetProviderTypeName(i);
                row[SchemaTableColumn.AllowDBNull] = _result.Columns.GetIsNullable(i);
                dt.Rows.Add(row);
            }

            return dt;
        }

        /// <inheritdoc />
        public override void Close()
        {
            if (_closed)
                return;

            _closed = true;
            _result.Dispose();
        }

        /// <inheritdoc />
        protected override void Dispose(bool disposing)
        {
            if (disposing)
                Close();

            base.Dispose(disposing);
        }

        void ThrowIfClosed()
        {
            if (_closed)
                throw new InvalidOperationException("The data reader is closed.");
        }

        void ThrowIfNoRow()
        {
            ThrowIfClosed();
            if (_hasRow == false)
                throw new InvalidOperationException("No row is currently available. Call Read() first.");
        }

        /// <inheritdoc />
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, (_behavior & CommandBehavior.CloseConnection) != 0);

        /// <inheritdoc />
        public override int GetOrdinal(string name)
        {
            ThrowIfClosed();

            for (var i = 0; i < _result.Columns.Count; i++)
                if (string.Equals(_result.Columns.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return i;

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        /// <inheritdoc />
        public override string GetName(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns.GetName(ordinal);
        }

        /// <inheritdoc />
        public override string GetDataTypeName(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns.GetProviderTypeName(ordinal);
        }

        /// <inheritdoc />
        public override int GetValues(object[] values)
        {
            ArgumentNullException.ThrowIfNull(values);
            ThrowIfNoRow();

            var count = Math.Min(values.Length, _result.Columns.Count);
            for (var i = 0; i < count; i++)
                values[i] = GetValue(i);

            return count;
        }

        /// <inheritdoc />
        public override T GetFieldValue<T>(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetFieldValue<T>();
        }

        /// <inheritdoc />
        public override object GetValue(int ordinal)
        {
            ThrowIfNoRow();
            return _result.Current.GetValue(ordinal).GetValue();
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            ThrowIfNoRow();
            return _result.Current.GetValue(ordinal).GetString();
        }

        /// <inheritdoc />
        public override Type GetFieldType(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns.GetClrType(ordinal);
        }

        /// <inheritdoc />
        public override bool GetBoolean(int ordinal)
        {
            ThrowIfClosed();
            return Convert.ToBoolean(GetValue(ordinal));
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            ThrowIfClosed();
            return Convert.ToByte(GetValue(ordinal));
        }

        /// <inheritdoc />
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetBytes(dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc />
        public override char GetChar(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetChar();
        }

        /// <inheritdoc />
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetChars(dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetDateTime();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="DateTimeOffset"/>.
        /// </summary>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetDateTimeOffset();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="TimeSpan"/>.
        /// </summary>
        public TimeSpan GetTimeSpan(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetTimeSpan();
        }

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetDecimal();
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetDouble();
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetFloat();
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetGuid();
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetInt16();
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetInt32();
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetInt64();
        }

        /// <summary>
        /// Retrieves the value of the specified column as a DateOnly object.
        /// </summary>
        /// <remarks>This method throws an exception if the data reader is closed or if the specified
        /// column does not contain a valid date value.</remarks>
        /// <param name="ordinal">The zero-based column ordinal indicating which column's value to retrieve.</param>
        /// <returns>A DateOnly value representing the date stored in the specified column.</returns>
        public DateOnly GetDateOnly(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetDateOnly();
        }

        /// <summary>
        /// Retrieves the value of the specified column as a TimeOnly object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public TimeOnly GetTimeOnly(int ordinal)
        {
            ThrowIfClosed();
            return _result.Current.GetValue(ordinal).GetTimeOnly();
        }

    }

}
