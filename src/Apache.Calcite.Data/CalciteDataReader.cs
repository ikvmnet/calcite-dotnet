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
    /// <see cref="CalciteCommand"/> or <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// When the reader is produced by a <see cref="CalciteBatch"/> that contains multiple commands,
    /// each command produces an independent result set. After exhausting the rows of one result set,
    /// call <see cref="NextResult"/> or <see cref="NextResultAsync"/> to advance to the next.
    /// </remarks>
    public sealed class CalciteDataReader : DbDataReader
    {

        readonly CalciteResult[] _results;
        readonly CommandBehavior _behavior;
        int _resultIndex;
        bool _closed;
        bool _hasRow;

        /// <summary>
        /// Initializes a new instance with a single result set.
        /// </summary>
        internal CalciteDataReader(CalciteResult result, CommandBehavior behavior) :
            this([result ?? throw new ArgumentNullException(nameof(result))], behavior)
        {

        }

        /// <summary>
        /// Initializes a new instance with multiple result sets, supporting <see cref="NextResult"/>.
        /// </summary>
        internal CalciteDataReader(CalciteResult[] results, CommandBehavior behavior)
        {
            if (results is null || results.Length == 0)
                throw new ArgumentException("At least one result is required.", nameof(results));

            _results = results;
            _behavior = behavior;
        }

        CalciteResult ActiveResult => _results[_resultIndex];

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
                return ActiveResult.Columns.Count;
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
                var v = ActiveResult.RecordsAffected;
                if (v > int.MaxValue) return int.MaxValue;
                if (v < int.MinValue) return int.MinValue;
                return (int)v;
            }
        }

        /// <inheritdoc />
        public override bool NextResult()
        {
            ThrowIfClosed();
            if (_resultIndex >= _results.Length - 1)
                return false;
            _results[_resultIndex].Dispose();
            _resultIndex++;
            _hasRow = false;
            return true;
        }

        /// <inheritdoc />
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(NextResult());
        }

        /// <inheritdoc />
        public override bool Read()
        {
            return ReadAsync(CancellationToken.None).GetAwaiter().GetResult();
        }

        /// <inheritdoc />
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfClosed();
            _hasRow = await ActiveResult.ReadAsync(cancellationToken).ConfigureAwait(false);
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

            for (var i = 0; i < ActiveResult.Columns.Count; i++)
            {
                var row = dt.NewRow();
                row[SchemaTableColumn.ColumnName] = ActiveResult.Columns.GetName(i);
                row[SchemaTableColumn.ColumnOrdinal] = i;
                row[SchemaTableColumn.DataType] = ActiveResult.Columns.GetClrType(i);
                row[SchemaTableColumn.ProviderType] = ActiveResult.Columns.GetProviderTypeName(i);
                row[SchemaTableColumn.AllowDBNull] = ActiveResult.Columns.GetIsNullable(i);
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
            for (var i = _resultIndex; i < _results.Length; i++)
                _results[i].Dispose();
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

            for (var i = 0; i < ActiveResult.Columns.Count; i++)
                if (string.Equals(ActiveResult.Columns.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return i;

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        /// <inheritdoc />
        public override string GetName(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetName(ordinal);
        }

        /// <inheritdoc />
        public override string GetDataTypeName(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetProviderTypeName(ordinal);
        }

        /// <inheritdoc />
        public override bool IsDBNull(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).IsDbNull();
        }

        /// <inheritdoc />
        public override int GetValues(object[] values)
        {
            ArgumentNullException.ThrowIfNull(values);
            ThrowIfNoRow();

            var count = Math.Min(values.Length, ActiveResult.Columns.Count);
            for (var i = 0; i < count; i++)
                values[i] = GetValue(i);

            return count;
        }

        /// <inheritdoc />
        public override T GetFieldValue<T>(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetFieldValue<T>();
        }

        /// <inheritdoc />
        public override object GetValue(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetValue();
        }

        /// <inheritdoc />
        public override string GetString(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetString();
        }

        /// <inheritdoc />
        public override Type GetFieldType(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetClrType(ordinal);
        }

        /// <inheritdoc />
        public override bool GetBoolean(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetBoolean();
        }

        /// <inheritdoc />
        public override byte GetByte(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetByte();
        }

        /// <summary>
        /// Returns the value of the specified column as an <see cref="sbyte"/> (signed 8-bit integer).
        /// </summary>
        public sbyte GetSByte(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetSByte();
        }

        /// <inheritdoc />
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetBytes(dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc />
        public override char GetChar(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetChar();
        }

        /// <inheritdoc />
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetChars(dataOffset, buffer, bufferOffset, length);
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDateTime();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="DateTimeOffset"/>.
        /// </summary>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDateTimeOffset();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="TimeSpan"/>.
        /// </summary>
        public TimeSpan GetTimeSpan(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetTimeSpan();
        }

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDecimal();
        }

        /// <inheritdoc />
        public override double GetDouble(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDouble();
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetFloat();
        }

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetGuid();
        }

        /// <inheritdoc />
        public override short GetInt16(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt16();
        }

        /// <inheritdoc />
        public override int GetInt32(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt32();
        }

        /// <inheritdoc />
        public override long GetInt64(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt64();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="ushort"/> (unsigned 16-bit integer).
        /// </summary>
        public ushort GetUInt16(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt16();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="uint"/> (unsigned 32-bit integer).
        /// </summary>
        public uint GetUInt32(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt32();
        }

        /// <summary>
        /// Returns the value of the specified column as a <see cref="ulong"/> (unsigned 64-bit integer).
        /// </summary>
        public ulong GetUInt64(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt64();
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
            return ActiveResult.Current.GetValue(ordinal).GetDateOnly();
        }

        /// <summary>
        /// Retrieves the value of the specified column as a TimeOnly object.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        public TimeOnly GetTimeOnly(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetTimeOnly();
        }

    }

}
