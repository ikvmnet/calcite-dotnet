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
    /// Provides a forward-only, read-only stream of rows from a result set produced by a
    /// <see cref="CalciteCommand"/> or <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Rows may be read with <see cref="Read"/> or <see cref="ReadAsync"/>, and the two may be mixed, whichever
    /// execute method produced the reader. <see cref="ReadAsync"/> awaits only where a table produces rows
    /// asynchronously and otherwise completes synchronously; <see cref="Read"/> blocks only where a table can
    /// produce rows asynchronously and no other way.
    /// </para>
    /// <para>
    /// Where <c>Microsoft.Data.SqlClient</c> sets the precedent for a choice ADO.NET leaves open, this reader
    /// follows it. A typed getter such as <see cref="GetInt32"/> reads a column whose type corresponds to the
    /// getter's type, and throws <see cref="InvalidCastException"/> otherwise rather than converting:
    /// <see cref="GetInt64"/> over an <c>INTEGER</c> column throws, and <see cref="GetGuid"/> does not parse a
    /// string. Typed getters also throw <see cref="InvalidCastException"/> for a null value; test with
    /// <see cref="IsDBNull"/> first. Columns of type <c>ANY</c>, <c>OTHER</c> or <c>VARIANT</c> behave as
    /// <c>sql_variant</c> does: the value's own type decides which getter reads it.
    /// </para>
    /// <para>
    /// Values are always .NET values: Calcite's Java representations are converted, including inside arrays,
    /// maps and rows. <see cref="GetCalciteValue"/> returns the unconverted value.
    /// </para>
    /// <para>
    /// A reader produced by a <see cref="CalciteBatch"/> holds one result set per command; call
    /// <see cref="NextResult"/> to move to the next.
    /// </para>
    /// </remarks>
    public sealed class CalciteDataReader : DbDataReader
    {

        readonly CalciteResult[] _results;
        readonly CommandBehavior _behavior;
        int _resultIndex;
        bool _closed;
        bool _hasRow;

        /// <summary>
        /// Initializes a new instance over a single result set.
        /// </summary>
        internal CalciteDataReader(CalciteResult result, CommandBehavior behavior) :
            this([result ?? throw new ArgumentNullException(nameof(result))], behavior)
        {

        }

        /// <summary>
        /// Initializes a new instance over one result set per batch command, read in order with
        /// <see cref="NextResult"/>.
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
        /// <remarks>
        /// Always 0.
        /// </remarks>
        public override int Depth => 0;

        /// <inheritdoc />
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        public override int FieldCount
        {
            get
            {
                ThrowIfClosed();
                return ActiveResult.Columns.Count;
            }
        }

        /// <summary>
        /// Gets whether the most recent call to <see cref="Read"/> or <see cref="ReadAsync"/> returned a row.
        /// </summary>
        /// <remarks>
        /// This reader does not look ahead: <see cref="HasRows"/> is <see langword="false"/> until a row has been
        /// read, and becomes <see langword="false"/> again once reading reaches the end of the result set.
        /// </remarks>
        public override bool HasRows => _hasRow;

        /// <inheritdoc />
        public override bool IsClosed => _closed;

        /// <summary>
        /// Gets the number of rows affected by the statement that produced the current result set.
        /// </summary>
        /// <remarks>
        /// A reader reports 0 for every statement. For the number of rows a data modification affected, use
        /// <see cref="DbCommand.ExecuteNonQuery"/>, or read the single <c>ROWCOUNT</c> column the statement
        /// returns as its result set.
        /// </remarks>
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

        /// <summary>
        /// Advances to the next result set of a batch, releasing the current one.
        /// </summary>
        /// <returns><see langword="true"/> if there is another result set; otherwise <see langword="false"/>.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
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

        /// <summary>
        /// Advances to the next result set of a batch, releasing the current one.
        /// </summary>
        /// <param name="cancellationToken">Not used.</param>
        /// <returns>A completed task whose result is <see langword="true"/> if there is another result set.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        /// <remarks>
        /// Runs <see cref="NextResult"/> synchronously. Every command of a batch has already been executed when
        /// the reader is returned.
        /// </remarks>
        public override Task<bool> NextResultAsync(CancellationToken cancellationToken)
        {
            return Task.FromResult(NextResult());
        }

        /// <summary>
        /// Advances the reader to the next row.
        /// </summary>
        /// <returns><see langword="true"/> if there is another row; otherwise <see langword="false"/>.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        /// <exception cref="CalciteException">The statement failed while producing the row.</exception>
        /// <remarks>
        /// Blocks only where a table can produce rows asynchronously and no other way. It does so with the
        /// current synchronization context suppressed, so calling it from a thread that has one does not
        /// deadlock.
        /// </remarks>
        public override bool Read()
        {
            ThrowIfClosed();

            _hasRow = ActiveResult.Read();
            return _hasRow;
        }

        /// <summary>
        /// Advances the reader to the next row.
        /// </summary>
        /// <param name="cancellationToken">The token to monitor for cancellation requests. It is passed to every
        /// operator of the plan for this call.</param>
        /// <returns>A task whose result is <see langword="true"/> if there is another row.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
        /// <remarks>
        /// Where no table in the plan reads asynchronously, the task is already complete when returned.
        /// Cancelling <paramref name="cancellationToken"/> during the call, or passing one that is already
        /// cancelled, cancels the whole statement, as <c>SqlDataReader.ReadAsync</c> does; later reads fail.
        /// </remarks>
        public override async Task<bool> ReadAsync(CancellationToken cancellationToken)
        {
            ThrowIfClosed();

            _hasRow = await ActiveResult.ReadAsync(cancellationToken).ConfigureAwait(false);
            return _hasRow;
        }

        /// <summary>
        /// Returns a <see cref="DataTable"/> that describes the columns of the current result set.
        /// </summary>
        /// <returns>A table with one row per column and the columns <c>ColumnName</c>, <c>ColumnOrdinal</c>,
        /// <c>DataType</c>, <c>ProviderType</c> (the Calcite type name) and <c>AllowDBNull</c>.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
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

        /// <summary>
        /// Closes the reader and releases the result sets it has not yet released.
        /// </summary>
        /// <remarks>
        /// Closing the reader does not close the connection, even where the command was executed with
        /// <see cref="CommandBehavior.CloseConnection"/>.
        /// </remarks>
        public override void Close()
        {
            if (_closed)
                return;

            _closed = true;
            for (var i = _resultIndex; i < _results.Length; i++)
                _results[i].Dispose();
        }

        /// <summary>
        /// Closes the reader and releases the result sets it has not yet released, awaiting where a table
        /// releases its resources asynchronously.
        /// </summary>
        /// <returns>A task that completes when the reader is closed.</returns>
        /// <remarks>
        /// <see cref="DbDataReader.CloseAsync"/> and <see cref="DbDataReader.DisposeAsync"/> would otherwise run
        /// <see cref="Close"/>, which blocks where a table's release must be awaited. <c>await using</c> on the
        /// reader uses this method.
        /// </remarks>
        public override async Task CloseAsync()
        {
            if (_closed)
                return;

            _closed = true;

            for (var i = _resultIndex; i < _results.Length; i++)
                await _results[i].DisposeAsync().ConfigureAwait(false);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Closes the reader with <see cref="CloseAsync"/>.
        /// </remarks>
        public override async ValueTask DisposeAsync()
        {
            await CloseAsync().ConfigureAwait(false);

            // the base call reaches Dispose and then Close, which returns at once on a closed reader; it is made
            // for whatever else DbDataReader does on disposal
            await base.DisposeAsync().ConfigureAwait(false);
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
        /// <remarks>
        /// Where the command was executed with <see cref="CommandBehavior.CloseConnection"/>, the enumerator
        /// closes the connection when it finishes.
        /// </remarks>
        public override IEnumerator GetEnumerator() => new DbEnumerator(this, (_behavior & CommandBehavior.CloseConnection) != 0);

        /// <summary>
        /// Gets the ordinal of the column with the specified name.
        /// </summary>
        /// <param name="name">The column name, compared ignoring case.</param>
        /// <returns>The zero-based ordinal of the first column with that name.</returns>
        /// <exception cref="IndexOutOfRangeException">No column has that name.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        public override int GetOrdinal(string name)
        {
            ThrowIfClosed();

            for (var i = 0; i < ActiveResult.Columns.Count; i++)
                if (string.Equals(ActiveResult.Columns.GetName(i), name, StringComparison.OrdinalIgnoreCase))
                    return i;

            throw new IndexOutOfRangeException($"Column '{name}' was not found.");
        }

        /// <summary>
        /// Gets the name of the specified column: its alias where the query gives one.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The column name.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        public override string GetName(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetName(ordinal);
        }

        /// <summary>
        /// Gets the Calcite SQL type name of the specified column, such as <c>INTEGER</c> or <c>VARCHAR</c>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The type name.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        /// <remarks>
        /// For the full type, including the element type of an array or the fields of a row, use
        /// <see cref="GetRelDataType"/>.
        /// </remarks>
        public override string GetDataTypeName(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetProviderTypeName(ordinal);
        }

        /// <summary>
        /// Gets whether the specified column of the current row is SQL null.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns><see langword="true"/> if the value is null; otherwise <see langword="false"/>.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// A <c>VARIANT</c> holding a null, whether a typed SQL null or a JSON <c>null</c>, is null.
        /// </remarks>
        public override bool IsDBNull(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).IsDbNull();
        }

        /// <summary>
        /// Copies the values of the current row into an array.
        /// </summary>
        /// <param name="values">The array to copy into.</param>
        /// <returns>The number of values copied: the smaller of the array's length and <see cref="FieldCount"/>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="values"/> is <see langword="null"/>.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Each value is converted as <see cref="GetValue"/> converts it.
        /// </remarks>
        public override int GetValues(object[] values)
        {
            ArgumentNullException.ThrowIfNull(values);
            ThrowIfNoRow();

            var count = Math.Min(values.Length, ActiveResult.Columns.Count);
            for (var i = 0; i < count; i++)
                values[i] = GetValue(i);

            return count;
        }

        /// <summary>
        /// Gets the value of the specified column as the requested type.
        /// </summary>
        /// <typeparam name="T">The type to read the value as.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The value cannot be read as <typeparamref name="T"/>, or is null
        /// and <typeparamref name="T"/> is a non-nullable value type.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// <para>
        /// Accepts the type <see cref="GetFieldType"/> reports, any type that value is assignable to (including
        /// <see cref="object"/>), and any other .NET type the column's Calcite type has a mapping for: a
        /// <c>DATE</c> column reads as <see cref="DateTime"/> by default and as <see cref="DateOnly"/> when
        /// asked. For an array or map column, the element, key and value types may be named, such as
        /// <c>object[]</c> for an <c>INTEGER ARRAY</c> or <c>DateOnly[]</c> for a <c>DATE ARRAY</c>; naming an
        /// element type the elements do not have, such as <c>long[]</c> for an <c>INTEGER ARRAY</c>, throws.
        /// </para>
        /// <para>
        /// A null value is returned as <see langword="null"/> where <typeparamref name="T"/> is a reference or
        /// nullable type. The Java class Calcite holds the value in is not accepted; use
        /// <see cref="GetCalciteValue"/> for that.
        /// </para>
        /// </remarks>
        public override T GetFieldValue<T>(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetFieldValue<T>();
        }

        /// <summary>
        /// Gets the value of the specified column in its default .NET representation.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value, of the type <see cref="GetFieldType"/> reports, or <see cref="DBNull.Value"/> for a
        /// null value.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// An <c>ARRAY</c> or <c>MULTISET</c> is returned as an array whose element type is the type its elements
        /// share (for example <c>int[]</c>, or <c>int?[]</c> where one is null, or <c>object[]</c> where they
        /// differ), a <c>MAP</c> as a <see cref="System.Collections.Generic.Dictionary{TKey, TValue}"/>, and a
        /// <c>ROW</c> as an <c>object[]</c>. A map with a null key is returned as an array of
        /// <see cref="System.Collections.Generic.KeyValuePair{TKey, TValue}"/>.
        /// </remarks>
        public override object GetValue(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetValue();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="string"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column's type has no <see cref="string"/> reading, or the
        /// value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Reads <c>CHAR</c> and <c>VARCHAR</c> columns, and <c>GEOMETRY</c> columns as well-known text. Other
        /// types are not converted to text.
        /// </remarks>
        public override string GetString(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetString();
        }

        /// <summary>
        /// Gets the .NET type of the specified column's values, as <see cref="GetValue"/> returns them.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The type. For an <c>ANY</c>, <c>OTHER</c> or <c>VARIANT</c> column it is <see cref="object"/>.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed.</exception>
        /// <exception cref="Common.ClrTypeMappingException">No type mapping covers the column's Calcite type.</exception>
        public override Type GetFieldType(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetClrType(ordinal);
        }

        /// <summary>
        /// Gets the value of an <c>ARRAY</c> or <c>MULTISET</c> column as an array.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value as an array.</returns>
        /// <exception cref="InvalidCastException">The column is not a collection, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// <para>
        /// ADO.NET has no accessor for a collection; this is the counterpart of JDBC's <c>getArray</c>. A
        /// <c>MAP</c> column is not a collection and is refused.
        /// </para>
        /// <para>
        /// The element type is the one the column's elements read back as: an <c>INTEGER ARRAY</c> is an
        /// <c>int[]</c>, an <c>INTEGER ARRAY ARRAY</c> an <c>int[][]</c>, and an array whose elements may be null
        /// an <c>int?[]</c>. <see cref="GetArray{T}"/> names the element type instead. An <c>ANY</c> or
        /// <c>VARIANT</c> column is read where its value is a collection.
        /// </para>
        /// </remarks>
        public Array GetArray(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetArray();
        }

        /// <summary>
        /// Gets the value of an <c>ARRAY</c> or <c>MULTISET</c> column as an array of the specified element type.
        /// </summary>
        /// <typeparam name="T">The element type.</typeparam>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value as an array of <typeparamref name="T"/>.</returns>
        /// <exception cref="InvalidCastException">The column is not a collection, the value is null, an element
        /// cannot be read as <typeparamref name="T"/>, or an element is null and <typeparamref name="T"/> is a
        /// non-nullable value type.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Naming the element type selects the element conversion rather than casting the default result, so a
        /// <c>DATE ARRAY</c> read as <see cref="DateOnly"/> converts each element to <see cref="DateOnly"/>. The
        /// element type must be one the elements can be read as by the rules of <see cref="GetFieldValue{T}"/>:
        /// <c>GetArray&lt;long&gt;</c> over an <c>INTEGER ARRAY</c> throws, as <see cref="GetInt64"/> does over an
        /// <c>INTEGER</c> column. Use <see cref="object"/> for elements of mixed types and
        /// <c>GetArray&lt;int?&gt;</c> where elements may be null.
        /// </remarks>
        public T[] GetArray<T>(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).GetArray<T>();
        }

        /// <summary>
        /// Gets the value of the specified column exactly as Calcite's runtime holds it, without conversion.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value as Calcite holds it, or <see langword="null"/> where it is null.</returns>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// <para>
        /// This is the only accessor that returns Java objects. An <c>INTEGER</c> is a <c>java.lang.Integer</c>,
        /// a <c>DATE</c> the <c>java.lang.Integer</c> count of days since 1970-01-01, an <c>ARRAY</c> a
        /// <c>java.util.List</c>, a <c>UUID</c> a <c>UuidValue</c>, a <c>GEOMETRY</c> a JTS <c>Geometry</c>, and
        /// a <c>VARIANT</c> a <c>VariantValue</c>.
        /// </para>
        /// <para>
        /// Use it to work with Calcite's representation directly, or to read a type this provider has no .NET
        /// mapping for. A null value is returned as <see langword="null"/>, not <see cref="DBNull.Value"/>.
        /// </para>
        /// </remarks>
        public object? GetCalciteValue(int ordinal)
        {
            ThrowIfNoRow();
            return ActiveResult.Current.GetValue(ordinal).CalciteValue;
        }

        /// <summary>
        /// Gets the Calcite type of the specified column.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The type.</returns>
        /// <remarks>
        /// <see cref="GetFieldType"/> and <see cref="GetDataTypeName"/> cannot describe a nested type such as an
        /// <c>INTEGER ARRAY ARRAY</c>, a <c>MAP</c>'s key and value types, or a <c>ROW</c>'s fields. The
        /// <c>RelDataType</c> returned here carries them, along with precision, scale and nullability.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The reader is closed, or the statement has no result
        /// columns (DDL).</exception>
        public org.apache.calcite.rel.type.RelDataType GetRelDataType(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Columns.GetRelType(ordinal);
        }

        /// <summary>
        /// Gets the Calcite type of the specified column as a <see cref="Common.CalciteDbType"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The type, or <see cref="Common.CalciteDbType.Unknown"/> where it has no member for it.</returns>
        /// <remarks>
        /// A collection is a flag combined with its element type, so an <c>INTEGER ARRAY</c> is
        /// <c>Array | Integer</c>. A nested collection such as <c>INTEGER ARRAY ARRAY</c> is <c>Array</c> over
        /// <see cref="Common.CalciteDbType.Unknown"/>; use <see cref="GetRelDataType"/> for the exact type.
        /// </remarks>
        /// <exception cref="InvalidOperationException">The reader is closed, or the statement has no result
        /// columns (DDL).</exception>
        public Common.CalciteDbType GetCalciteDbType(int ordinal)
        {
            ThrowIfClosed();
            return Common.CalciteDbTypes.Of(ActiveResult.Columns.GetRelType(ordinal));
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="bool"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>BOOLEAN</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override bool GetBoolean(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetBoolean();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="byte"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>TINYINT UNSIGNED</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// A <see cref="byte"/> is unsigned, so it corresponds to <c>TINYINT UNSIGNED</c>. Calcite's
        /// <c>TINYINT</c> is signed; read it with <see cref="GetSByte"/>.
        /// </remarks>
        public override byte GetByte(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetByte();
        }

        /// <summary>
        /// Gets the value of the specified column as an <see cref="sbyte"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>TINYINT</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public sbyte GetSByte(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetSByte();
        }

        /// <summary>
        /// Reads bytes from a binary column into a buffer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The offset in the value to start reading from.</param>
        /// <param name="buffer">The buffer to copy into, or <see langword="null"/> to return the value's length.</param>
        /// <param name="bufferOffset">The offset in <paramref name="buffer"/> to start writing at.</param>
        /// <param name="length">The maximum number of bytes to copy.</param>
        /// <returns>The number of bytes copied, or the value's length in bytes where <paramref name="buffer"/> is
        /// <see langword="null"/>. Returns 0 for a null value.</returns>
        /// <exception cref="InvalidCastException">The value is not binary.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override long GetBytes(int ordinal, long dataOffset, byte[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetBytes(dataOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="char"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>CHAR</c>, the value is not exactly one
        /// character long, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override char GetChar(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetChar();
        }

        /// <summary>
        /// Reads characters from a character column into a buffer.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <param name="dataOffset">The offset in the value to start reading from.</param>
        /// <param name="buffer">The buffer to copy into, or <see langword="null"/> to return the value's length.</param>
        /// <param name="bufferOffset">The offset in <paramref name="buffer"/> to start writing at.</param>
        /// <param name="length">The maximum number of characters to copy.</param>
        /// <returns>The number of characters copied, or the value's length where <paramref name="buffer"/> is
        /// <see langword="null"/>.</returns>
        /// <exception cref="InvalidCastException">The column is not a character type, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override long GetChars(int ordinal, long dataOffset, char[]? buffer, int bufferOffset, int length)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetChars(dataOffset, buffer, bufferOffset, length);
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTime"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column's type has no <see cref="DateTime"/> reading, or the
        /// value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Reads <c>DATE</c> and <c>TIMESTAMP</c> columns, and <c>TIMESTAMP WITH TIME ZONE</c> as UTC.
        /// </remarks>
        public override DateTime GetDateTime(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDateTime();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateTimeOffset"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column's type has no <see cref="DateTimeOffset"/> reading,
        /// or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Reads the zoned types: <c>TIMESTAMP WITH TIME ZONE</c>, <c>TIMESTAMP WITH LOCAL TIME ZONE</c>,
        /// <c>TIME WITH TIME ZONE</c> and <c>TIME WITH LOCAL TIME ZONE</c> (on the date 0001-01-01). Also reads a
        /// <c>TIMESTAMP</c> column at offset zero.
        /// </remarks>
        public DateTimeOffset GetDateTimeOffset(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDateTimeOffset();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="TimeSpan"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column's type has no <see cref="TimeSpan"/> reading, or the
        /// value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Reads <c>TIME</c> and the day-time interval types.
        /// </remarks>
        public TimeSpan GetTimeSpan(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetTimeSpan();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="decimal"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>DECIMAL</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override decimal GetDecimal(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDecimal();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="double"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>DOUBLE</c> or <c>FLOAT</c>, or the value
        /// is null. A <c>REAL</c> column is not widened.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override double GetDouble(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDouble();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="float"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>REAL</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override float GetFloat(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetFloat();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="Guid"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>UUID</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// A character column holding GUID text is not read; convert it in SQL with <c>CAST(x AS UUID)</c>.
        /// </remarks>
        public override Guid GetGuid(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetGuid();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="short"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>SMALLINT</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public override short GetInt16(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt16();
        }

        /// <summary>
        /// Gets the value of the specified column as an <see cref="int"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column's type has no <see cref="int"/> reading, or the value
        /// is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// Reads <c>INTEGER</c> columns, and year-month interval columns as a number of months. Narrower and
        /// wider integer columns are not converted.
        /// </remarks>
        public override int GetInt32(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt32();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="long"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>BIGINT</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// An <c>INTEGER</c> column is not widened; read it with <see cref="GetInt32"/>.
        /// </remarks>
        public override long GetInt64(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetInt64();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="ushort"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>SMALLINT UNSIGNED</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public ushort GetUInt16(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt16();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="uint"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>INTEGER UNSIGNED</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public uint GetUInt32(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt32();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="ulong"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>BIGINT UNSIGNED</c>, or the value is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        public ulong GetUInt64(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetUInt64();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="DateOnly"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>DATE</c> or <c>TIMESTAMP</c>, or the value
        /// is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// A <c>TIMESTAMP</c> is read as its date part.
        /// </remarks>
        public DateOnly GetDateOnly(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetDateOnly();
        }

        /// <summary>
        /// Gets the value of the specified column as a <see cref="TimeOnly"/>.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidCastException">The column is not <c>TIME</c> or <c>TIMESTAMP</c>, or the value
        /// is null.</exception>
        /// <exception cref="InvalidOperationException">The reader is closed, or there is no current row.</exception>
        /// <remarks>
        /// A <c>TIMESTAMP</c> is read as its time part.
        /// </remarks>
        public TimeOnly GetTimeOnly(int ordinal)
        {
            ThrowIfClosed();
            return ActiveResult.Current.GetValue(ordinal).GetTimeOnly();
        }

    }

}
