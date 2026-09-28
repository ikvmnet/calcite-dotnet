using System;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Extensions;
using Apache.Calcite.Adapter.AdoNet.Metadata;
using Apache.Calcite.Extensions.Interop;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A linq4j <see cref="org.apache.calcite.linq4j.Enumerable"/> that runs one SQL statement against an
    /// <see cref="AdoDataSource"/> each time it is enumerated. Plans in Calcite's <c>EnumerableConvention</c>
    /// read a pushed-down statement through this class.
    /// </summary>
    /// <remarks>
    /// Each call to <see cref="enumerator"/> opens a connection, creates the command, applies the
    /// <see cref="DbCommandEnricher"/> if there is one, and executes the command there. A query's enumerator owns
    /// the reader, command and connection and disposes them when it is closed; an update executes, closes the
    /// connection and returns a single row holding the affected row count.
    /// </remarks>
    public abstract partial class AdoEnumerable : AbstractEnumerable
    {

        static readonly Function1 AutoRowBuilderFactory = new FuncFunction1<DbDataReader, Function0>(AutoRowBuilderFactoryFunc);

        /// <summary>
        /// Returns a row builder that reads the current row as the provider's own values: the value itself for a
        /// single column, and an <c>object[]</c> otherwise.
        /// </summary>
        /// <param name="reader">The reader the row builder reads from.</param>
        /// <returns>The row builder.</returns>
        /// <exception cref="AdoCalciteException">The reader cannot report its column count.</exception>
        static Function0 AutoRowBuilderFactoryFunc(DbDataReader reader)
        {
            int fieldCount;

            try
            {
                fieldCount = reader.FieldCount;
            }
            catch (NotSupportedException e)
            {
                throw new AdoCalciteException("The data reader does not support reading the number of columns.", e);
            }

            if (fieldCount == 1)
                return new FuncFunction0<object>(() => reader.GetValue(0));
            else
                return new FuncFunction0<object>(() => ConvertColumns(reader, fieldCount));
        }

        /// <summary>
        /// Reads every column of the reader's current row into an array.
        /// </summary>
        /// <param name="reader">The reader, positioned on a row.</param>
        /// <param name="fieldCount">The reader's column count.</param>
        /// <returns>The row's values, as the provider returns them.</returns>
        static object[] ConvertColumns(DbDataReader reader, int fieldCount)
        {
            var list = new object[fieldCount];
            reader.GetValues(list);
            return list;
        }

        /// <summary>
        /// Returns an enumerable that runs a query and yields each row as the provider's own values.
        /// </summary>
        /// <param name="dataSource">The data source to run the query against.</param>
        /// <param name="sql">The query.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql)
        {
            return CreateReader(dataSource, sql, AutoRowBuilderFactory);
        }

        /// <summary>
        /// Returns an enumerable that runs a query and builds each row with a row builder.
        /// </summary>
        /// <param name="dataSource">The data source to run the query against.</param>
        /// <param name="sql">The query.</param>
        /// <param name="rowBuilderFactory">A <see cref="Function1"/> from the <see cref="DbDataReader"/> to a
        /// <see cref="Function0"/> that returns the current row.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            return new AdoReaderEnumerable(dataSource, sql, rowBuilderFactory);
        }

        /// <summary>
        /// Returns an enumerable that runs a query, after <paramref name="dbCommandEnricher"/> has prepared the
        /// command, and builds each row with a row builder.
        /// </summary>
        /// <param name="dataSource">The data source to run the query against.</param>
        /// <param name="sql">The query.</param>
        /// <param name="rowBuilderFactory">A <see cref="Function1"/> from the <see cref="DbDataReader"/> to a
        /// <see cref="Function0"/> that returns the current row.</param>
        /// <param name="dbCommandEnricher">Called with each command before it executes.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher)
        {
            return new AdoReaderEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Returns an enumerable that runs a statement that returns no rows, and yields its affected row count.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql)
        {
            return CreateUpdate(dataSource, sql, AutoRowBuilderFactory);
        }

        /// <summary>
        /// Returns an enumerable that runs a statement that returns no rows, and yields its affected row count.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">Required, and not used: the single row is the count.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            return new AdoUpdateEnumerable(dataSource, sql, rowBuilderFactory);
        }

        /// <summary>
        /// Returns an enumerable that runs a statement that returns no rows, after
        /// <paramref name="dbCommandEnricher"/> has prepared the command, and yields its affected row count.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">Required, and not used: the single row is the count.</param>
        /// <param name="dbCommandEnricher">Called with each command before it executes.</param>
        /// <returns>The enumerable. Nothing runs until it is enumerated.</returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher)
        {
            return new AdoUpdateEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Returns an enricher that adds the statement's parameters to a command, reading each value from a
        /// <see cref="DataContext"/>. Called from the code the converters generate.
        /// </summary>
        /// <param name="dataSource">The data source, whose <see cref="IAdoSqlSyntax"/> names each
        /// parameter.</param>
        /// <param name="indexes">The <see cref="DataContext"/> variable index behind each parameter, in the order the
        /// parameters appear in the statement.</param>
        /// <param name="typeNames">The <see cref="org.apache.calcite.sql.type.SqlTypeName"/> name of each parameter, in
        /// the same order, or <see langword="null"/> where none was recorded.</param>
        /// <param name="context">The context the values are read from, as <c>?</c> followed by the index.</param>
        /// <returns>The enricher.</returns>
        /// <remarks>
        /// A statement's own dynamic parameters come from the context the statement was bound with. Correlation
        /// variables have indexes at or above <see cref="AdoCorrelationDataContext.Offset"/> and come from an
        /// <see cref="AdoCorrelationDataContext"/> built over the outer row. The type name is needed because
        /// Calcite carries a temporal value as a number: a <c>DATE</c> is a day count in a
        /// <see cref="java.lang.Integer"/>, indistinguishable by value from an <c>INTEGER</c>.
        /// </remarks>
        public static DbCommandEnricher CreateEnricher(AdoDataSource dataSource, java.util.List indexes, java.util.List typeNames, DataContext context)
        {
            return new ParameterEnricher(dataSource.Metadata.Syntax, indexes, typeNames, context);
        }

        /// <summary>
        /// Adds one parameter to the command for each index, reading its value from the context.
        /// </summary>
        /// <param name="syntax">Names each parameter by its position.</param>
        /// <param name="indexes">The <see cref="DataContext"/> variable index behind each parameter, in
        /// statement order.</param>
        /// <param name="typeNames">The <c>SqlTypeName</c> name of each parameter, in the same order; may be
        /// shorter.</param>
        /// <param name="context">The context each value is read from, under <c>?</c> followed by its
        /// index.</param>
        sealed class ParameterEnricher(IAdoSqlSyntax syntax, java.util.List indexes, java.util.List typeNames, DataContext context) : DbCommandEnricher
        {

            /// <inheritdoc />
            public void Enrich(DbCommand command)
            {
                for (int i = 0; i < indexes.size(); i++)
                    SetParameter(syntax, command, i,
                        context.get("?" + ((java.lang.Number)indexes.get(i)).intValue()),
                        i < typeNames.size() ? (string?)typeNames.get(i) : null);
            }

        }

        /// <summary>
        /// Inserts parameter <paramref name="i"/> into the command, named by <paramref name="syntax"/>, with the
        /// value converted for the provider. No <see cref="System.Data.DbType"/> is set; the provider infers one.
        /// </summary>
        /// <param name="syntax">Names the parameter.</param>
        /// <param name="command">The command to add the parameter to.</param>
        /// <param name="i">The parameter's position in the statement.</param>
        /// <param name="value">The value in Calcite's representation.</param>
        /// <param name="typeName">The parameter's <see cref="org.apache.calcite.sql.type.SqlTypeName"/> name,
        /// or <see langword="null"/>.</param>
        static void SetParameter(IAdoSqlSyntax syntax, DbCommand command, int i, object? value, string? typeName)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = syntax.GetParameterName(i);
            parameter.Value = ToProviderValue(value, typeName) ?? DBNull.Value;

            // ODBC and OLE DB bind a temporal parameter at scale zero unless told otherwise, and then reject
            // fractional seconds ("Fractional second precision exceeds the scale specified in the parameter
            // binding"). The values decode from a millisecond count, so three digits covers them; SqlClient
            // ignores the scale for an inferred datetime.
            if (parameter.Value is DateTime or TimeSpan or DateTimeOffset)
                parameter.Scale = 3;

            command.Parameters.Insert(i, parameter);
        }

        /// <summary>
        /// The instant Calcite's temporal representations count from.
        /// </summary>
        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        /// <summary>
        /// Converts a value from Calcite's representation into one a provider can bind, decoding temporal values
        /// by their SQL type.
        /// </summary>
        /// <param name="value">The value in Calcite's representation.</param>
        /// <param name="typeName">The value's <see cref="org.apache.calcite.sql.type.SqlTypeName"/> name, or
        /// <see langword="null"/>.</param>
        /// <returns>The value to bind, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Calcite carries a <c>DATE</c> as days since the epoch, a <c>TIME</c> as milliseconds since midnight and a
        /// <c>TIMESTAMP</c> as milliseconds since the epoch. A typed column does not accept the number, so it is
        /// decoded to a <see cref="DateTime"/> or <see cref="TimeSpan"/>, inverting what <see cref="AdoReaderUtil"/>
        /// encodes on the way in. The <see cref="DateTime"/> is <see cref="DateTimeKind.Unspecified"/> because the
        /// count is of the wall clock; the two zoned timestamp types become a <see cref="DateTimeOffset"/> in UTC.
        /// </remarks>
        static object? ToProviderValue(object? value, string? typeName)
        {
            if (value is null)
                return null;

            switch (typeName)
            {
                case nameof(org.apache.calcite.sql.type.SqlTypeName.DATE):
                    return UnixEpoch.AddDays(((java.lang.Number)value).intValue());
                case nameof(org.apache.calcite.sql.type.SqlTypeName.TIME):
                    return TimeSpan.FromMilliseconds(((java.lang.Number)value).intValue());
                case nameof(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP):
                    return UnixEpoch.AddMilliseconds(((java.lang.Number)value).longValue());
                case nameof(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_TZ):
                case nameof(org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE):
                    return new DateTimeOffset(DateTime.SpecifyKind(UnixEpoch, DateTimeKind.Utc).AddMilliseconds(((java.lang.Number)value).longValue()), TimeSpan.Zero);
                default:
                    return ToProviderValue(value);
            }
        }

        /// <summary>
        /// Converts a value from Calcite's representation, a boxed Java type, into the .NET value a provider
        /// can bind.
        /// </summary>
        /// <param name="value">The value in Calcite's representation.</param>
        /// <returns>The value to bind, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Each value becomes the narrowest .NET type every provider binds, which is not always the narrowest
        /// that holds it: SqlClient binds none of <c>sbyte</c>, <c>ushort</c>, <c>uint</c> and <c>ulong</c>, so
        /// those widen. The provider infers the parameter's type from the result.
        /// </remarks>
        static object? ToProviderValue(object? value)
        {
            return value switch
            {
                null => null,
                java.lang.Boolean b => b.booleanValue(),
                // IKVM's byte is unsigned, so byteValue() would turn -56 into 200; shortValue() sign-extends,
                // and SqlClient does not bind an sbyte
                java.lang.Byte b => b.shortValue(),
                java.lang.Short s => s.shortValue(),
                java.lang.Integer i => i.intValue(),
                java.lang.Long l => l.longValue(),
                java.lang.Float f => f.floatValue(),
                java.lang.Double d => d.doubleValue(),
                java.lang.Character c => c.charValue(),
                // not through toString(), which uses scientific notation for small values ("1E-7") that
                // decimal.Parse does not accept
                java.math.BigDecimal m => JavaDecimals.ToDecimal(m),
                org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
                // a provider binds a Guid against a uniqueidentifier and knows neither Java UUID type
                org.apache.calcite.util.UuidValue uv => JavaUuids.ToGuid(uv),
                java.util.UUID u => JavaUuids.ToGuid(u),
                // Calcite's unsigned types are joou values. SqlClient binds a byte but not ushort, uint or
                // ulong, so those go to the signed type that holds their range, and ULong to decimal
                org.joou.UByte ub => (byte)ub.shortValue(),
                org.joou.UShort us => us.intValue(),
                org.joou.UInteger ui => ui.longValue(),
                org.joou.ULong ul => (decimal)unchecked((ulong)ul.longValue()),
                _ => value,
            };
        }

        readonly AdoDataSource _dataSource;
        readonly string _sql;
        readonly Function1 _rowBuilderFactory;
        readonly DbCommandEnricher? _dbCommandEnricher;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">A <see cref="Function1"/> from the <see cref="DbDataReader"/> to a
        /// <see cref="Function0"/> that returns the current row.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        protected AdoEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _sql = sql ?? throw new ArgumentNullException(nameof(sql));
            _rowBuilderFactory = rowBuilderFactory ?? throw new ArgumentNullException(nameof(rowBuilderFactory));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to run the statement against.</param>
        /// <param name="sql">The statement.</param>
        /// <param name="rowBuilderFactory">A <see cref="Function1"/> from the <see cref="DbDataReader"/> to a
        /// <see cref="Function0"/> that returns the current row.</param>
        /// <param name="dbCommandEnricher">Called with each command before it executes.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        protected AdoEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher) :
            this(dataSource, sql, rowBuilderFactory)
        {
            _dbCommandEnricher = dbCommandEnricher ?? throw new ArgumentNullException(nameof(dbCommandEnricher));
        }

        /// <summary>
        /// Gets the factory that makes a row builder for a reader.
        /// </summary>
        protected Function1 RowBuilderFactory => _rowBuilderFactory;

        /// <summary>
        /// Opens a connection, prepares the command and executes it.
        /// </summary>
        /// <returns>An enumerator over the statement's result.</returns>
        /// <exception cref="AdoCalciteException">The provider raised a <see cref="DbException"/>; the message includes
        /// the SQL.</exception>
        public override Enumerator enumerator()
        {
            try
            {
                var cnn = _dataSource.OpenConnection();
                var cmd = cnn.CreateCommand();
                cmd.CommandText = _sql;
                _dbCommandEnricher?.Enrich(cmd);
                return CreateEnumerator(cnn, cmd);
            }
            catch (DbException e)
            {
                throw new AdoCalciteException($"Exception while enumerating query: {_sql}", e);
            }
        }

        /// <summary>
        /// Executes the command and returns an enumerator over its result.
        /// </summary>
        /// <param name="connection">The open connection. The implementation takes ownership of it.</param>
        /// <param name="command">The prepared command. The implementation takes ownership of it.</param>
        /// <returns>The enumerator.</returns>
        protected abstract Enumerator CreateEnumerator(DbConnection connection, DbCommand command);

    }

}
