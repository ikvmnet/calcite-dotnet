using System;
using System.Data.Common;

using Apache.Calcite.Adapter.Ado.Extensions;
using Apache.Calcite.Adapter.Ado.Metadata;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.Ado
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
        /// Creates an encricher that sets parameters from the context.
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="indexes"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static Action<DbCommand> CreateEnricher(AdoDatabaseMetadata metadata, int[] indexes, DataContext context)
        {
            return command =>
            {
                for (int i = 0; i < indexes.Length; i++)
                {
                    var index = indexes[i];
                    SetParameter(metadata, command, i, context.get("?" + index));
                }
            };
        }

        /// <summary>
        /// Sets the given parameter to the given value.
        /// </summary>
        /// <param name="metadata"></param>
        /// <param name="command"></param>
        /// <param name="i"></param>
        /// <param name="value"></param>
        static void SetParameter(AdoDatabaseMetadata metadata, DbCommand command, int i, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = metadata.GetParameterName(i);
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Insert(i, parameter);
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
                Console.WriteLine(cmd.CommandText);
                Console.WriteLine();
                return CreateEnumerator(cnn, cmd);
            }
            catch (DbException e)
            {
                throw new AdoCalciteException("Exception while enumerating query.", e);
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
