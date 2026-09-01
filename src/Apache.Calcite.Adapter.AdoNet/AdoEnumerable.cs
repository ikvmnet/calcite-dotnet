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
    /// Enumerable implementation.
    /// </summary>
    public abstract partial class AdoEnumerable : AbstractEnumerable
    {

        static readonly Function1 AutoRowBuilderFactory = new FuncFunction1<DbDataReader, Function0>(AutoRowBuilderFactoryFunc);

        /// <summary>
        /// Provides a row builder factory function that automatically converts if required.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        /// <exception cref="AdoCalciteException"></exception>
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
        /// Converts the columns of the current row of the <see cref="DbDataReader"/> to the appropriate ADO output type.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fieldCount"></param>
        /// <returns></returns>
        static object[] ConvertColumns(DbDataReader reader, int fieldCount)
        {
            var list = new object[fieldCount];
            reader.GetValues(list);
            return list;
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as their automatic types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql)
        {
            return CreateReader(dataSource, sql, AutoRowBuilderFactory);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            return new AdoReaderEnumerable(dataSource, sql, rowBuilderFactory);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <param name="dbCommandEnricher"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher)
        {
            return new AdoReaderEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as their automatic types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql)
        {
            return CreateUpdate(dataSource, sql, AutoRowBuilderFactory);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            return new AdoUpdateEnumerable(dataSource, sql, rowBuilderFactory);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <param name="dbCommandEnricher"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher)
        {
            return new AdoUpdateEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Creates an enricher that fills the command's parameters from the context.
        /// </summary>
        /// <param name="dataSource">The source whose syntax names a parameter for this provider.</param>
        /// <param name="indexes">The variable index behind each parameter, in parameter order.</param>
        /// <param name="typeNames">The <see cref="org.apache.calcite.sql.type.SqlTypeName"/> name behind each parameter, in the same order, or a null where none was recorded.</param>
        /// <param name="context">The context the values are read from.</param>
        /// <returns></returns>
        /// <remarks>
        /// Reached from the code the converter generates, once per execution of the pushed-down statement.
        /// The context is an <see cref="AdoCorrelationDataContext"/> closed over the outer row of a
        /// correlated join, so reading a <c>?N</c> at or above its offset yields that row's value; every
        /// lower index is a dynamic parameter of the statement itself and comes from the context the
        /// statement was bound with, which is what a <c>WHERE key = ?</c> pushed down to the provider
        /// carries. The type names travel beside the indexes because a value alone cannot be bound: a
        /// <c>DATE</c> leaves the plan as a day count in a <see cref="java.lang.Integer"/>, and only the
        /// type says it is not simply an <c>INTEGER</c>.
        /// </remarks>
        public static DbCommandEnricher CreateEnricher(AdoDataSource dataSource, java.util.List indexes, java.util.List typeNames, DataContext context)
        {
            return new ParameterEnricher(dataSource.Metadata.Syntax, indexes, typeNames, context);
        }

        /// <summary>
        /// Sets one parameter per correlation variable, reading each from the context.
        /// </summary>
        /// <param name="syntax"></param>
        /// <param name="indexes"></param>
        /// <param name="typeNames"></param>
        /// <param name="context"></param>
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
        /// Sets the given parameter to the given value.
        /// </summary>
        /// <param name="syntax"></param>
        /// <param name="command"></param>
        /// <param name="i"></param>
        /// <param name="value"></param>
        /// <param name="typeName"></param>
        static void SetParameter(IAdoSqlSyntax syntax, DbCommand command, int i, object? value, string? typeName)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = syntax.GetParameterName(i);
            parameter.Value = ToProviderValue(value, typeName) ?? DBNull.Value;

            // ODBC and OLE DB bind a temporal parameter at scale zero unless told otherwise, and then refuse
            // the fractional seconds the value itself carries: "Datetime field overflow. Fractional second
            // precision exceeds the scale specified in the parameter binding." Three digits is the honest
            // scale — the representation these values decode from is a millisecond count. Measured: scale 3
            // makes both drivers accept the value, and SqlClient ignores it for an inferred datetime.
            if (parameter.Value is DateTime or TimeSpan or DateTimeOffset)
                parameter.Scale = 3;

            command.Parameters.Insert(i, parameter);
        }

        /// <summary>
        /// The instant the temporal representations count from.
        /// </summary>
        static readonly DateTime UnixEpoch = new(1970, 1, 1, 0, 0, 0, DateTimeKind.Unspecified);

        /// <summary>
        /// Converts a value read from a <see cref="DataContext"/> into one a provider can bind, undoing the
        /// temporal encodings first.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="typeName"></param>
        /// <returns></returns>
        /// <remarks>
        /// A temporal value leaves the plan as a count — a <c>DATE</c> as whole days since the epoch, a
        /// <c>TIME</c> as milliseconds since midnight, a <c>TIMESTAMP</c> as milliseconds since the epoch —
        /// which is Calcite's internal representation and nothing a typed column accepts: SQL Server answers
        /// "Operand type clash: date is incompatible with int". Only SQLite tolerated it, and only because
        /// SQLite compares whatever it is handed. The count is decoded to the temporal value it stands for,
        /// exactly inverting what <see cref="AdoReaderUtil"/> encoded on the way in; the kind is Unspecified
        /// because the count is of the wall clock as written, no zone having entered into it.
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
        /// Converts a value read from a <see cref="DataContext"/> into one a provider can bind.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// A parameter value comes out of the plan in Calcite's representation, which is a boxed Java
        /// type. No ADO.NET provider knows what a <see cref="java.lang.Long"/> is, so it is unwrapped to
        /// the .NET value it stands for. This is the inverse of what <see cref="AdoReaderUtil"/> does on
        /// the way in.
        /// </remarks>
        static object? ToProviderValue(object? value)
        {
            return value switch
            {
                null => null,
                java.lang.Boolean b => b.booleanValue(),
                java.lang.Byte b => b.byteValue(),
                java.lang.Short s => s.shortValue(),
                java.lang.Integer i => i.intValue(),
                java.lang.Long l => l.longValue(),
                java.lang.Float f => f.floatValue(),
                java.lang.Double d => d.doubleValue(),
                java.lang.Character c => c.charValue(),
                // through the byte transfer, not through toString(): BigDecimal writes itself in scientific
                // notation once the adjusted exponent falls below -6 or the scale goes negative -- 0.0000001
                // is "1E-7" -- and decimal.Parse(string, IFormatProvider) is NumberStyles.Number, which does
                // not allow an exponent. Measured: every such value was a FormatException
                java.math.BigDecimal m => JavaDecimals.ToDecimal(m),
                org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
                // Calcite holds a UUID as a java.util.UUID, which SqlClient refuses outright: "No mapping
                // exists from object type java.util.UUID to a known managed provider native type". A Guid
                // is what a provider binds against a uniqueidentifier, and the transfer is the sixteen
                // bytes rather than the text
                java.util.UUID u => JavaUuids.ToGuid(u),
                // the unsigned types travel as joou values, and no provider knows those either. Each is
                // unwrapped to the narrowest CLR type every provider binds: SqlClient takes a byte but none
                // of ushort, uint or ulong, so the wider three go to the signed type that holds their range
                // exactly — and ULong to decimal, ulong's top half being outside long
                org.joou.UByte ub => (byte)ub.shortValue(),
                org.joou.UShort us => us.intValue(),
                org.joou.UInteger ui => ui.longValue(),
                org.joou.ULong ul => decimal.Parse(ul.toString(), System.Globalization.CultureInfo.InvariantCulture),
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
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        protected AdoEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory)
        {
            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _sql = sql ?? throw new ArgumentNullException(nameof(sql));
            _rowBuilderFactory = rowBuilderFactory ?? throw new ArgumentNullException(nameof(rowBuilderFactory));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <param name="dbCommandEnricher"></param>
        protected AdoEnumerable(AdoDataSource dataSource, string sql, Function1 rowBuilderFactory, DbCommandEnricher dbCommandEnricher) :
            this(dataSource, sql, rowBuilderFactory)
        {
            _dbCommandEnricher = dbCommandEnricher ?? throw new ArgumentNullException(nameof(dbCommandEnricher));
        }

        /// <summary>
        /// 
        /// </summary>
        protected Function1 RowBuilderFactory => _rowBuilderFactory;

        /// <summary>
        /// Returns an enumerator for the result set.
        /// </summary>
        /// <returns></returns>
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
                // with the SQL, because what a provider says about a statement it rejected is rarely enough
                // to find it: "Incorrect syntax near '='" names neither the statement nor the position
                throw new AdoCalciteException($"Exception while enumerating query: {_sql}", e);
            }
        }

        /// <summary>
        /// Creates an enumerator around the specified connection and command.
        /// </summary>
        /// <param name="connection"></param>
        /// <param name="command"></param>
        /// <returns></returns>
        protected abstract Enumerator CreateEnumerator(DbConnection connection, DbCommand command);

    }

}
