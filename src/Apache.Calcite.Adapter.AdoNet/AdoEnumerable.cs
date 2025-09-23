using System;
using System.Data;
using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Extensions;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Adapter.AdoNet
{

    public abstract class AdoEnumerable : AbstractEnumerable
    {

        static readonly Function1 AutoRowBuilderFactory = new FuncFunction1<DbDataReader, Function0>(AutoRowBuilderFactoryFunc);

        /// <summary>
        /// Provides a row builder factory function that automatically converts if required.
        /// </summary>
        /// <param name="reader"></param>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        static Function0 AutoRowBuilderFactoryFunc(DbDataReader reader)
        {
            int fieldCount;

            try
            {
                fieldCount = reader.FieldCount;
            }
            catch (NotSupportedException e)
            {
                throw new AdoSchemaException("The data reader does not support reading the number of columns.", e);
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
        /// Creates a primitive row builder factory for the given set of primitives.
        /// </summary>
        /// <param name="primitives"></param>
        /// <returns></returns>
        static Function1 PrimitiveRowBuilderFactory(Primitive[] primitives)
        {
            return new FuncFunction1<DbDataReader, Function0>(reader =>
            {
                int fieldCount;

                try
                {
                    fieldCount = reader.FieldCount;
                }
                catch (NotSupportedException e)
                {
                    throw new AdoSchemaException("The data reader does not support reading the number of columns.", e);
                }

                if (fieldCount != primitives.Length)
                    throw new AdoSchemaException($"The number of primitives ({primitives.Length}) does not match the number of columns ({fieldCount}).");

                if (fieldCount == 1)
                    return new FuncFunction0<object>(() => reader.GetValue(0));
                else
                    return new FuncFunction0<object>(() => ConvertPrimitiveColumns(primitives, reader, fieldCount));
            });
        }

        /// <summary>
        /// Converts the columns of the current row of the <see cref="DbDataReader"/> to the appropriate ADO output type.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="primitives"></param>
        /// <param name="fieldCount"></param>
        /// <returns></returns>
        static object[] ConvertPrimitiveColumns(Primitive[] primitives, DbDataReader reader, int fieldCount)
        {
            var list = new object[fieldCount];
            reader.GetValues(list);

            for (int i = 0; i < list.Length; i++)
                list[i] = primitives[i].arrayItem(list, i);

            return list;
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as their automatic types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(DbDataSource dataSource, string sql)
        {
            return CreateReader(dataSource, sql, AutoRowBuilderFactory);
        }
        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as specific Java types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="primitives"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(DbDataSource dataSource, string sql, Primitive[] primitives)
        {
            return CreateReader(dataSource, sql, PrimitiveRowBuilderFactory(primitives));
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateReader(DbDataSource dataSource, string sql, Function1 rowBuilderFactory)
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
        public static AdoEnumerable CreateReader(DbDataSource dataSource, string sql, Function1 rowBuilderFactory, Action<DbCommand> dbCommandEnricher)
        {
            return new AdoReaderEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as their automatic types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(DbDataSource dataSource, string sql)
        {
            return CreateUpdate(dataSource, sql, AutoRowBuilderFactory);
        }
        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to retrieve columns as specific Java types.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="primitives"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(DbDataSource dataSource, string sql, Primitive[] primitives)
        {
            return CreateUpdate(dataSource, sql, PrimitiveRowBuilderFactory(primitives));
        }

        /// <summary>
        /// Executes a SQL query and returns the results as an enumerator, using a row builder to convert ADO column values into rows.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        /// <returns></returns>
        public static AdoEnumerable CreateUpdate(DbDataSource dataSource, string sql, Function1 rowBuilderFactory)
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
        public static AdoEnumerable CreateUpdate(DbDataSource dataSource, string sql, Function1 rowBuilderFactory, Action<DbCommand> dbCommandEnricher)
        {
            return new AdoUpdateEnumerable(dataSource, sql, rowBuilderFactory, dbCommandEnricher);
        }

        /// <summary>
        /// Creates an encricher that sets parameters from the context.
        /// </summary>
        /// <param name="indexes"></param>
        /// <param name="context"></param>
        /// <returns></returns>
        public static Action<DbCommand> CreateEnricher(int[] indexes, DataContext context)
        {
            return command =>
            {
                for (int i = 0; i < indexes.Length; i++)
                {
                    var index = indexes[i];
                    SetParameter(command, i, context.get("?" + index));
                }
            };
        }

        /// <summary>
        /// Sets the given parameter to the given value.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="i"></param>
        /// <param name="value"></param>
        static void SetParameter(DbCommand command, int i, object? value)
        {
            var parameter = command.CreateParameter();
            parameter.ParameterName = "@p" + i;
            parameter.Value = value ?? DBNull.Value;
            command.Parameters.Insert(i, parameter);
        }

        readonly DbDataSource _dataSource;
        readonly string _sql;
        readonly Function1 _rowBuilderFactory;
        readonly Action<DbCommand>? _dbCommandEnricher;

        int _timeout;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="sql"></param>
        /// <param name="rowBuilderFactory"></param>
        protected AdoEnumerable(DbDataSource dataSource, string sql, Function1 rowBuilderFactory)
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
        protected AdoEnumerable(DbDataSource dataSource, string sql, Function1 rowBuilderFactory, Action<DbCommand> dbCommandEnricher) :
            this(dataSource, sql, rowBuilderFactory)
        {
            _dbCommandEnricher = dbCommandEnricher ?? throw new ArgumentNullException(nameof(dbCommandEnricher));
        }

        /// <summary>
        /// 
        /// </summary>
        protected Function1 RowBuilderFactory => _rowBuilderFactory;

        /// <summary>
        /// Sets the command timeout from the context.
        /// </summary>
        /// <param name="context"></param>
        public void SetTimeout(DataContext context)
        {
            _timeout = context.get(DataContext.Variable.TIMEOUT.camelName) switch
            {
                java.lang.Long @long => (int)@long.longValue(),
                java.lang.Integer @int => @int.intValue(),
                _ => 0,
            };
        }

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
                cmd.CommandTimeout = _timeout;
                _dbCommandEnricher?.Invoke(cmd);
                return CreateEnumerator(cnn, cmd);
            }
            catch (DataException e)
            {
                throw new AdoSchemaException("Exception while enumerating query.", e);
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
