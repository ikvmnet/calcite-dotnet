using System;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Extensions;
using Apache.Calcite.Adapter.AdoNet.Metadata;

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
        /// <param name="context">The context the values are read from, one per correlation variable.</param>
        /// <returns></returns>
        /// <remarks>
        /// Reached from the code the converter generates, once per execution of the inner side of a
        /// correlated join. The context is an <see cref="AdoCorrelationDataContext"/> closed over the outer
        /// row, so reading <c>?N</c> yields that row's value.
        /// </remarks>
        public static DbCommandEnricher CreateEnricher(AdoDataSource dataSource, java.util.List indexes, DataContext context)
        {
            return new ParameterEnricher(dataSource.Metadata.Syntax, indexes, context);
        }

        /// <summary>
        /// Sets one parameter per correlation variable, reading each from the context.
        /// </summary>
        /// <param name="syntax"></param>
        /// <param name="indexes"></param>
        /// <param name="context"></param>
        sealed class ParameterEnricher(IAdoSqlSyntax syntax, java.util.List indexes, DataContext context) : DbCommandEnricher
        {

            /// <inheritdoc />
            public void Enrich(DbCommand command)
            {
                for (int i = 0; i < indexes.size(); i++)
                    SetParameter(syntax, command, i, context.get("?" + ((java.lang.Number)indexes.get(i)).intValue()));
            }

        }

        /// <summary>
        /// Sets the given parameter to the given value.
        /// </summary>
        /// <param name="syntax"></param>
        /// <param name="command"></param>
        /// <param name="i"></param>
        /// <param name="value"></param>
        static void SetParameter(IAdoSqlSyntax syntax, DbCommand command, int i, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = syntax.GetParameterName(i);
            parameter.Value = ToProviderValue(value) ?? DBNull.Value;
            command.Parameters.Insert(i, parameter);
        }

        /// <summary>
        /// Converts a value read from a <see cref="DataContext"/> into one a provider can bind.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <remarks>
        /// A correlation value comes out of the plan in Calcite's representation, which is a boxed Java
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
                java.math.BigDecimal m => decimal.Parse(m.toString(), System.Globalization.CultureInfo.InvariantCulture),
                org.apache.calcite.avatica.util.ByteString bs => bs.getBytes(),
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
