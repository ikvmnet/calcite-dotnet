using System;

using java.util.concurrent.atomic;

using org.apache.calcite.adapter.java;
using org.apache.calcite.jdbc;
using org.apache.calcite.rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// Runs a <see cref="RelNode"/> that was built rather than parsed.
    /// </summary>
    /// <remarks>
    /// <c>CalciteConnectionImpl</c>, which is itself the <c>RelRunner</c> its context hands out. There is no
    /// connection here, so the two halves are separate classes; what they do is the same.
    ///
    /// <para>Calcite reaches this from one place: <c>ServerDdlExecutor.populate</c>, which is how
    /// <c>CREATE MATERIALIZED VIEW</c> and <c>CREATE TABLE ... AS SELECT</c> load their rows. It builds an
    /// <c>INSERT</c> through <c>Frameworks.getPlanner</c> and then asks the context to run the plan. Without
    /// a runner both statements fail on the store, after the table has already been added to the
    /// schema.</para>
    /// </remarks>
    sealed class ClrRelRunner : RelRunner
    {

        readonly CalcitePrepare.Context _context;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        public ClrRelRunner(CalcitePrepare.Context context)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
        }

        /// <inheritdoc />
        public java.sql.PreparedStatement prepareStatement(RelNode rel)
        {
            return new ClrRelPreparedStatement(_context, rel);
        }

    }

    /// <summary>
    /// The <c>java.sql.PreparedStatement</c> a <see cref="ClrRelRunner"/> answers with.
    /// </summary>
    /// <remarks>
    /// <b>This is a shim, not a JDBC statement.</b> <c>RelRunner.prepareStatement</c> is declared to return
    /// a <c>PreparedStatement</c> and its one caller here — <c>ServerDdlExecutor.populate</c> — uses exactly
    /// two members of it, <c>executeUpdate()</c> and <c>close()</c>. Those two are implemented; the other
    /// hundred-odd are the interface's, and throw.
    ///
    /// <para>Calcite's own answer is an <c>AvaticaPreparedStatement</c> built by the connection's statement
    /// factory. There is no Avatica connection on this path and one cannot be conjured, so the alternative
    /// to a shim is that these two DDL statements do not run. A caller that wants a real ADO.NET command
    /// has <c>CalciteCommand</c>; nothing outside Calcite is meant to see this class.</para>
    /// </remarks>
    sealed class ClrRelPreparedStatement : java.sql.PreparedStatement
    {

        readonly CalcitePrepare.Context _context;
        readonly RelNode _rel;
        bool _closed;
        int _updateCount = -1;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The schema, type factory and configuration to plan against.</param>
        /// <param name="rel">The plan to run.</param>
        public ClrRelPreparedStatement(CalcitePrepare.Context context, RelNode rel)
        {
            _context = context ?? throw new ArgumentNullException(nameof(context));
            _rel = rel ?? throw new ArgumentNullException(nameof(rel));
        }

        /// <summary>
        /// Plans, compiles and runs the statement, returning the rows it wrote.
        /// </summary>
        /// <remarks>
        /// <c>CalciteSession.ExecuteNonQuery</c>'s DML branch, against a plan rather than a statement: the
        /// row count is a value the plan produces, so the enumerator has to be drained for the write to
        /// happen at all. The cursor factory is <c>ARRAY</c>, and a one-column result is the value rather
        /// than a one-element row, so both shapes are read.
        /// </remarks>
        public int executeUpdate()
        {
            ThrowIfClosed();

            var signature = new ClrPrepareImpl().PrepareRel(_context, _rel, -1);
            var dataContext = new StatementDataContext(
                _context.getRootSchema().plus(),
                (JavaTypeFactory)_context.getTypeFactory(),
                new AtomicBoolean(false),
                0L,
                [],
                signature.InternalParameters);

            long count = 0;
            using (var e = signature.Bind(dataContext).GetEnumerator())
            {
                if (e.MoveNext())
                {
                    var cur = e.Current;
                    if (cur is object[] row && row.Length > 0)
                        count = ToInt64(row[0]);
                    else if (cur is not null)
                        count = ToInt64(cur);
                }
            }

            _updateCount = count > int.MaxValue ? int.MaxValue : (int)count;
            return _updateCount;
        }

        /// <inheritdoc />
        public int getUpdateCount() => _updateCount;

        /// <inheritdoc />
        public void close() => _closed = true;

        /// <inheritdoc />
        public bool isClosed() => _closed;

        /// <inheritdoc />
        public void Dispose() => close();

        /// <inheritdoc />
        public bool isWrapperFor(java.lang.Class iface) => iface.isInstance(this);

        /// <inheritdoc />
        public object unwrap(java.lang.Class iface)
        {
            if (iface.isInstance(this))
                return this;

            throw new java.sql.SQLException("not a " + iface.getName());
        }

        void ThrowIfClosed()
        {
            if (_closed)
                throw new java.sql.SQLException("statement is closed");
        }

        /// <summary>Converts a Calcite row-count value (Java boxed number or CLR primitive) to <see cref="long"/>.</summary>
        static long ToInt64(object? value) => value switch
        {
            null => 0,
            java.lang.Long l => l.longValue(),
            java.lang.Integer i => i.intValue(),
            java.lang.Number n => n.longValue(),
            IConvertible c => c.ToInt64(null),
            _ => Convert.ToInt64(value.ToString()),
        };

        static java.sql.SQLFeatureNotSupportedException Unsupported(string member)
        {
            return new java.sql.SQLFeatureNotSupportedException(
                member + " is not implemented: this statement exists only so that ServerDdlExecutor.populate "
                + "can run a plan, and it uses executeUpdate() and close().");
        }

        // The rest of java.sql.PreparedStatement. Generated from the interface; none of it is reachable
        // from populate, and none of it has a meaning here that a caller could rely on.
        public void addBatch() => throw Unsupported(nameof(addBatch));
        public void clearParameters() => throw Unsupported(nameof(clearParameters));
        public bool execute() => throw Unsupported(nameof(execute));
        public long executeLargeUpdate() => throw Unsupported(nameof(executeLargeUpdate));
        public java.sql.ResultSet executeQuery() => throw Unsupported(nameof(executeQuery));
        public java.sql.ResultSetMetaData getMetaData() => throw Unsupported(nameof(getMetaData));
        public java.sql.ParameterMetaData getParameterMetaData() => throw Unsupported(nameof(getParameterMetaData));
        public void setArray(int parameterIndex, java.sql.Array x) => throw Unsupported(nameof(setArray));
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, long length) => throw Unsupported(nameof(setAsciiStream));
        public void setAsciiStream(int parameterIndex, java.io.InputStream x, int length) => throw Unsupported(nameof(setAsciiStream));
        public void setAsciiStream(int parameterIndex, java.io.InputStream x) => throw Unsupported(nameof(setAsciiStream));
        public void setBigDecimal(int parameterIndex, java.math.BigDecimal x) => throw Unsupported(nameof(setBigDecimal));
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, int length) => throw Unsupported(nameof(setBinaryStream));
        public void setBinaryStream(int parameterIndex, java.io.InputStream x) => throw Unsupported(nameof(setBinaryStream));
        public void setBinaryStream(int parameterIndex, java.io.InputStream x, long length) => throw Unsupported(nameof(setBinaryStream));
        public void setBlob(int parameterIndex, java.io.InputStream inputStream, long length) => throw Unsupported(nameof(setBlob));
        public void setBlob(int parameterIndex, java.io.InputStream inputStream) => throw Unsupported(nameof(setBlob));
        public void setBlob(int parameterIndex, java.sql.Blob x) => throw Unsupported(nameof(setBlob));
        public void setBoolean(int parameterIndex, bool x) => throw Unsupported(nameof(setBoolean));
        public void setByte(int parameterIndex, byte x) => throw Unsupported(nameof(setByte));
        public void setBytes(int parameterIndex, byte[] x) => throw Unsupported(nameof(setBytes));
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, int length) => throw Unsupported(nameof(setCharacterStream));
        public void setCharacterStream(int parameterIndex, java.io.Reader reader) => throw Unsupported(nameof(setCharacterStream));
        public void setCharacterStream(int parameterIndex, java.io.Reader reader, long length) => throw Unsupported(nameof(setCharacterStream));
        public void setClob(int parameterIndex, java.io.Reader reader) => throw Unsupported(nameof(setClob));
        public void setClob(int parameterIndex, java.io.Reader reader, long length) => throw Unsupported(nameof(setClob));
        public void setClob(int parameterIndex, java.sql.Clob x) => throw Unsupported(nameof(setClob));
        public void setDate(int parameterIndex, java.sql.Date x) => throw Unsupported(nameof(setDate));
        public void setDate(int parameterIndex, java.sql.Date x, java.util.Calendar cal) => throw Unsupported(nameof(setDate));
        public void setDouble(int parameterIndex, double x) => throw Unsupported(nameof(setDouble));
        public void setFloat(int parameterIndex, float x) => throw Unsupported(nameof(setFloat));
        public void setInt(int parameterIndex, int x) => throw Unsupported(nameof(setInt));
        public void setLong(int parameterIndex, long x) => throw Unsupported(nameof(setLong));
        public void setNCharacterStream(int parameterIndex, java.io.Reader value, long length) => throw Unsupported(nameof(setNCharacterStream));
        public void setNCharacterStream(int parameterIndex, java.io.Reader value) => throw Unsupported(nameof(setNCharacterStream));
        public void setNClob(int parameterIndex, java.sql.NClob value) => throw Unsupported(nameof(setNClob));
        public void setNClob(int parameterIndex, java.io.Reader reader, long length) => throw Unsupported(nameof(setNClob));
        public void setNClob(int parameterIndex, java.io.Reader reader) => throw Unsupported(nameof(setNClob));
        public void setNString(int parameterIndex, string value) => throw Unsupported(nameof(setNString));
        public void setNull(int parameterIndex, int sqlType, string typeName) => throw Unsupported(nameof(setNull));
        public void setNull(int parameterIndex, int sqlType) => throw Unsupported(nameof(setNull));
        public void setObject(int parameterIndex, object x, java.sql.SQLType targetSqlType) => throw Unsupported(nameof(setObject));
        public void setObject(int parameterIndex, object x, java.sql.SQLType targetSqlType, int scaleOrLength) => throw Unsupported(nameof(setObject));
        public void setObject(int parameterIndex, object x, int targetSqlType) => throw Unsupported(nameof(setObject));
        public void setObject(int parameterIndex, object x) => throw Unsupported(nameof(setObject));
        public void setObject(int parameterIndex, object x, int targetSqlType, int scaleOrLength) => throw Unsupported(nameof(setObject));
        public void setRef(int parameterIndex, java.sql.Ref x) => throw Unsupported(nameof(setRef));
        public void setRowId(int parameterIndex, java.sql.RowId x) => throw Unsupported(nameof(setRowId));
        public void setShort(int parameterIndex, short x) => throw Unsupported(nameof(setShort));
        public void setSQLXML(int parameterIndex, java.sql.SQLXML xmlObject) => throw Unsupported(nameof(setSQLXML));
        public void setString(int parameterIndex, string x) => throw Unsupported(nameof(setString));
        public void setTime(int parameterIndex, java.sql.Time x) => throw Unsupported(nameof(setTime));
        public void setTime(int parameterIndex, java.sql.Time x, java.util.Calendar cal) => throw Unsupported(nameof(setTime));
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x) => throw Unsupported(nameof(setTimestamp));
        public void setTimestamp(int parameterIndex, java.sql.Timestamp x, java.util.Calendar cal) => throw Unsupported(nameof(setTimestamp));
        public void setUnicodeStream(int parameterIndex, java.io.InputStream x, int length) => throw Unsupported(nameof(setUnicodeStream));
        public void setURL(int parameterIndex, java.net.URL x) => throw Unsupported(nameof(setURL));
        public void addBatch(string sql) => throw Unsupported(nameof(addBatch));
        public void cancel() => throw Unsupported(nameof(cancel));
        public void clearBatch() => throw Unsupported(nameof(clearBatch));
        public void clearWarnings() => throw Unsupported(nameof(clearWarnings));
        public void closeOnCompletion() => throw Unsupported(nameof(closeOnCompletion));
        public bool execute(string sql, int autoGeneratedKeys) => throw Unsupported(nameof(execute));
        public bool execute(string sql, int[] columnIndexes) => throw Unsupported(nameof(execute));
        public bool execute(string sql, string[] columnNames) => throw Unsupported(nameof(execute));
        public bool execute(string sql) => throw Unsupported(nameof(execute));
        public int[] executeBatch() => throw Unsupported(nameof(executeBatch));
        public long[] executeLargeBatch() => throw Unsupported(nameof(executeLargeBatch));
        public long executeLargeUpdate(string sql) => throw Unsupported(nameof(executeLargeUpdate));
        public long executeLargeUpdate(string sql, string[] columnNames) => throw Unsupported(nameof(executeLargeUpdate));
        public long executeLargeUpdate(string sql, int autoGeneratedKeys) => throw Unsupported(nameof(executeLargeUpdate));
        public long executeLargeUpdate(string sql, int[] columnIndexes) => throw Unsupported(nameof(executeLargeUpdate));
        public java.sql.ResultSet executeQuery(string sql) => throw Unsupported(nameof(executeQuery));
        public int executeUpdate(string sql) => throw Unsupported(nameof(executeUpdate));
        public int executeUpdate(string sql, int autoGeneratedKeys) => throw Unsupported(nameof(executeUpdate));
        public int executeUpdate(string sql, int[] columnIndexes) => throw Unsupported(nameof(executeUpdate));
        public int executeUpdate(string sql, string[] columnNames) => throw Unsupported(nameof(executeUpdate));
        public java.sql.Connection getConnection() => throw Unsupported(nameof(getConnection));
        public int getFetchDirection() => throw Unsupported(nameof(getFetchDirection));
        public int getFetchSize() => throw Unsupported(nameof(getFetchSize));
        public java.sql.ResultSet getGeneratedKeys() => throw Unsupported(nameof(getGeneratedKeys));
        public long getLargeMaxRows() => throw Unsupported(nameof(getLargeMaxRows));
        public long getLargeUpdateCount() => throw Unsupported(nameof(getLargeUpdateCount));
        public int getMaxFieldSize() => throw Unsupported(nameof(getMaxFieldSize));
        public int getMaxRows() => throw Unsupported(nameof(getMaxRows));
        public bool getMoreResults(int current) => throw Unsupported(nameof(getMoreResults));
        public bool getMoreResults() => throw Unsupported(nameof(getMoreResults));
        public int getQueryTimeout() => throw Unsupported(nameof(getQueryTimeout));
        public java.sql.ResultSet getResultSet() => throw Unsupported(nameof(getResultSet));
        public int getResultSetConcurrency() => throw Unsupported(nameof(getResultSetConcurrency));
        public int getResultSetHoldability() => throw Unsupported(nameof(getResultSetHoldability));
        public int getResultSetType() => throw Unsupported(nameof(getResultSetType));
        public java.sql.SQLWarning getWarnings() => throw Unsupported(nameof(getWarnings));
        public bool isCloseOnCompletion() => throw Unsupported(nameof(isCloseOnCompletion));
        public bool isPoolable() => throw Unsupported(nameof(isPoolable));
        public void setCursorName(string name) => throw Unsupported(nameof(setCursorName));
        public void setEscapeProcessing(bool enable) => throw Unsupported(nameof(setEscapeProcessing));
        public void setFetchDirection(int direction) => throw Unsupported(nameof(setFetchDirection));
        public void setFetchSize(int rows) => throw Unsupported(nameof(setFetchSize));
        public void setLargeMaxRows(long max) => throw Unsupported(nameof(setLargeMaxRows));
        public void setMaxFieldSize(int max) => throw Unsupported(nameof(setMaxFieldSize));
        public void setMaxRows(int max) => throw Unsupported(nameof(setMaxRows));
        public void setPoolable(bool poolable) => throw Unsupported(nameof(setPoolable));
        public void setQueryTimeout(int seconds) => throw Unsupported(nameof(setQueryTimeout));

    }

}
