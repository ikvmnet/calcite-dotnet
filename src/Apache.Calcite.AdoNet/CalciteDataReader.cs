using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.AdoNet.Protocol;
using Apache.Calcite.AdoNet.TypeMapping;

namespace Apache.Calcite.AdoNet
{

    /// <summary>
    /// Provides a forward-only reader over the rows produced by a <see cref="CalciteCommand"/>.
    /// </summary>
    public sealed class CalciteDataReader : DbDataReader
    {

        readonly ICalciteResult _result;
        readonly CommandBehavior _behavior;
        bool _closed;
        bool _hasRow;

        internal CalciteDataReader(ICalciteResult result, CommandBehavior behavior)
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
        public override bool GetBoolean(int ordinal) => Convert.ToBoolean(GetValue(ordinal));

        /// <inheritdoc />
        public override byte GetByte(int ordinal) => Convert.ToByte(GetValue(ordinal));

        /// <inheritdoc />
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            var bytes = (byte[])GetValue(ordinal);
            if (buffer is null)
                return bytes.LongLength;

            var available = bytes.LongLength - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            Array.Copy(bytes, dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <inheritdoc />
        public override char GetChar(int ordinal) => Convert.ToChar(GetValue(ordinal));

        /// <inheritdoc />
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            var s = GetString(ordinal);
            if (buffer is null)
                return s.Length;

            var available = s.Length - dataOffset;
            if (available <= 0)
                return 0;

            var copy = (int)Math.Min(length, available);
            s.CopyTo((int)dataOffset, buffer, bufferOffset, copy);
            return copy;
        }

        /// <inheritdoc />
        public override string GetDataTypeName(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns[ordinal].ProviderTypeName;
        }

        /// <inheritdoc />
        public override DateTime GetDateTime(int ordinal) => Convert.ToDateTime(GetValue(ordinal));

        /// <inheritdoc />
        public override decimal GetDecimal(int ordinal) => Convert.ToDecimal(GetValue(ordinal));

        /// <inheritdoc />
        public override double GetDouble(int ordinal) => Convert.ToDouble(GetValue(ordinal));

        /// <inheritdoc />
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, (_behavior & CommandBehavior.CloseConnection) != 0);

        /// <inheritdoc />
        public override Type GetFieldType(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns[ordinal].ClrType;
        }

        /// <inheritdoc />
        public override float GetFloat(int ordinal) => Convert.ToSingle(GetValue(ordinal));

        /// <inheritdoc />
        public override Guid GetGuid(int ordinal) => (Guid)GetValue(ordinal);

        /// <inheritdoc />
        public override short GetInt16(int ordinal) => Convert.ToInt16(GetValue(ordinal));

        /// <inheritdoc />
        public override int GetInt32(int ordinal) => Convert.ToInt32(GetValue(ordinal));

        /// <inheritdoc />
        public override long GetInt64(int ordinal) => Convert.ToInt64(GetValue(ordinal));

        /// <inheritdoc />
        public override string GetName(int ordinal)
        {
            ThrowIfClosed();
            return _result.Columns[ordinal].Name;
        }

        /// <inheritdoc />
        public override int GetOrdinal(string name)
        {
            ThrowIfClosed();
            for (var i = 0; i < _result.Columns.Count; i++)
                if (string.Equals(_result.Columns[i].Name, name, StringComparison.OrdinalIgnoreCase))
                    return i;

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        /// <inheritdoc />
        public override string GetString(int ordinal) => Convert.ToString(GetValue(ordinal)) ?? string.Empty;

        /// <inheritdoc />
        public override object GetValue(int ordinal)
        {
            ThrowIfNoRow();
            var v = _result.Current![ordinal];
            return v ?? DBNull.Value;
        }

        /// <inheritdoc />
        public override int GetValues(object[] values)
        {
            if (values is null)
                throw new ArgumentNullException(nameof(values));

            ThrowIfNoRow();
            var count = Math.Min(values.Length, _result.Columns.Count);
            for (var i = 0; i < count; i++)
                values[i] = _result.Current![i] ?? DBNull.Value;

            return count;
        }

        /// <inheritdoc />
        public override bool IsDBNull(int ordinal)
        {
            ThrowIfNoRow();
            return _result.Current![ordinal] is null;
        }

        /// <inheritdoc />
        public override bool NextResult() => false;

        /// <inheritdoc />
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken) => Task.FromResult(false);

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
                var c = _result.Columns[i];
                var row = dt.NewRow();
                row[SchemaTableColumn.ColumnName] = c.Name;
                row[SchemaTableColumn.ColumnOrdinal] = i;
                row[SchemaTableColumn.DataType] = c.ClrType;
                row[SchemaTableColumn.ProviderType] = c.ProviderTypeName;
                row[SchemaTableColumn.AllowDBNull] = c.AllowDbNull;
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
            if (_hasRow == false || _result.Current is null)
                throw new InvalidOperationException("No row is currently available. Call Read() first.");
        }

        /// <summary>
        /// Workaround for unused symbol warnings on the type map in Phase 1; the reader will use it
        /// once full type-aware materialization paths land.
        /// </summary>
        internal static IReadOnlyDictionary<Type, DbType> KnownTypes => _knownTypes;

        static readonly IReadOnlyDictionary<Type, DbType> _knownTypes = new Dictionary<Type, DbType>
        {
            { typeof(int), CalciteTypeMap.ToDbType(typeof(int)) },
        };

    }

}
