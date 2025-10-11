using System;
using System.Data;
using System.Threading;

using Apache.Calcite.Adapter.Ado.Extensions;

using com.google.common.@base;

using java.lang;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Queryable table that gets its data from a table within a ADO connection.
    /// </summary>
    public class AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable
    {

        readonly java.util.function.Supplier protoRowTypeSupplier;

        readonly AdoDataSource _dataSource;
        readonly AdoConvention _convention;
        readonly string? _databaseName;
        readonly string? _schemaName;
        readonly string _tableName;
        readonly Schema.TableType _tableType;

        SqlIdentifier? _fullyQualifiedTableName;

        /// <summary>sssssssssss
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        /// <param name="convention"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="tableType"></param>
        /// <exception cref="ArgumentNullException"></exception>
        internal AdoTable(AdoDataSource dataSource, AdoConvention convention, string? databaseName, string? schemaName, string tableName, Schema.TableType tableType) :
            base((Class)typeof(object[]))
        {
            protoRowTypeSupplier = Suppliers.memoize(new FuncSupplier<RelProtoDataType>(GetRowProtoDataType));

            _dataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
            _convention = convention ?? throw new ArgumentNullException(nameof(convention));
            _databaseName = databaseName;
            _schemaName = schemaName;
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _tableType = tableType ?? throw new ArgumentNullException(nameof(tableType));
        }

        /// <summary>
        /// Gets the ADO data source.
        /// </summary>
        public AdoDataSource DataSource => _dataSource;

        /// <summary>
        /// Gets the ADO convention.
        /// </summary>
        public AdoConvention Convention => _convention;

        /// <summary>
        /// Gets the SQL dialect.
        /// </summary>
        public SqlDialect Dialect => _convention.Dialect;

        /// <inheritdoc />
        public override Schema.TableType getJdbcTableType() => _tableType;

        /// <summary>
        /// Gets the name of the source database for this table.
        /// </summary>
        public string? DatabaseName => _databaseName;

        /// <summary>
        /// Gets the name of the source schema for this table.
        /// </summary>
        public string? SchemaName => _schemaName;

        /// <summary>
        /// Gets the name of this table.
        /// </summary>
        public string TableName => _tableName;

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return (RelDataType)((RelProtoDataType)protoRowTypeSupplier.get()).apply(typeFactory);
        }

        /// <summary>
        /// Gets the <see cref="RelProtoDataType"/> for the row type of this table.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="AdoCalciteException"></exception>
        RelProtoDataType GetRowProtoDataType()
        {
            return GetRowProtoDataType(_databaseName, _schemaName, _tableName);
        }

        /// <summary>
        /// Gets the <see cref="RelProtoDataType"/> for a given table.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        RelProtoDataType GetRowProtoDataType(string? databaseName, string? schemaName, string tableName)
        {
            var typeFactory = new SqlTypeFactoryImpl(RelDataTypeSystem.DEFAULT);
            var types = typeFactory.builder();

            // derive a type for each field
            foreach (var field in _dataSource.Metadata.GetFields(databaseName, schemaName, tableName))
            {
                if (field.Name is null)
                    throw new AdoCalciteException("Null value encountered for field name.");

                types.add(field.Name, GetSqlType(typeFactory, field.DbType, field.Precision ?? -1, field.Scale ?? -1, field.Size ?? -1)).nullable(field.Nullable);
            }

            return RelDataTypeImpl.proto(types.build());
        }

        /// <summary>
        /// Transforms a <see cref="DbType"/> and its various additional information into a <see cref="RelDataType"/>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="dbType"></param>
        /// <param name="precision"></param>
        /// <param name="scale"></param>
        /// <param name="size"></param>
        /// <returns></returns>
        /// <exception cref="AdoCalciteException"></exception>
        RelDataType GetSqlType(RelDataTypeFactory typeFactory, DbType dbType, int precision, int scale, int size)
        {
            switch (dbType)
            {
                case DbType.AnsiString:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.Binary:
                    break;
                case DbType.Byte:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case DbType.Boolean:
                    return typeFactory.createSqlType(SqlTypeName.BOOLEAN);
                case DbType.Currency:
                    break;
                case DbType.Date:
                    return typeFactory.createSqlType(SqlTypeName.DATE);
                case DbType.DateTime:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.Decimal:
                    return typeFactory.createSqlType(SqlTypeName.DECIMAL, precision, scale);
                case DbType.Double:
                    return typeFactory.createSqlType(SqlTypeName.DOUBLE);
                case DbType.Guid:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, 36);
                case DbType.Int16:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case DbType.Int32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case DbType.Int64:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case DbType.Object:
                    break;
                case DbType.SByte:
                    return typeFactory.createSqlType(SqlTypeName.TINYINT);
                case DbType.Single:
                    break;
                case DbType.String:
                    return typeFactory.createSqlType(SqlTypeName.VARCHAR, size);
                case DbType.Time:
                    return typeFactory.createSqlType(SqlTypeName.TIME);
                case DbType.UInt16:
                    return typeFactory.createSqlType(SqlTypeName.SMALLINT);
                case DbType.UInt32:
                    return typeFactory.createSqlType(SqlTypeName.INTEGER);
                case DbType.UInt64:
                    return typeFactory.createSqlType(SqlTypeName.BIGINT);
                case DbType.VarNumeric:
                    break;
                case DbType.AnsiStringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.StringFixedLength:
                    return typeFactory.createSqlType(SqlTypeName.CHAR, size);
                case DbType.Xml:
                    break;
                case DbType.DateTime2:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP);
                case DbType.DateTimeOffset:
                    return typeFactory.createSqlType(SqlTypeName.TIMESTAMP_TZ);
            }

            throw new AdoCalciteException($"Unsupported database type: {dbType}.");
        }

        /// <summary>
        /// Returns a SQL string that selects from the table.
        /// </summary>
        /// <returns></returns>
        internal SqlString GenerateSqlString()
        {
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <summary>
        /// Returns the table name, qualified with schema name if applicable, as a parse tree node <see cref="SqlIdentifier"/>.
        /// </summary>
        /// <returns></returns>
        public SqlIdentifier FullyQualifiedTableName => GetFullyQualifiedTableName();

        /// <summary>
        /// Gets the value for <see cref="FullyQualifiedTableName"/>.
        /// </summary>
        /// <returns></returns>
        SqlIdentifier GetFullyQualifiedTableName()
        {
            if (_fullyQualifiedTableName is null)
            {
                var names = new java.util.ArrayList(3);

                if (_databaseName is not null)
                    names.add(_databaseName);

                if (_schemaName is not null)
                    names.add(_schemaName);

                names.add(_tableName);
                Interlocked.CompareExchange(ref _fullyQualifiedTableName, new SqlIdentifier(names, SqlParserPos.ZERO), null);
            }

            return _fullyQualifiedTableName;
        }

        /// <inheritdoc />
        public override Queryable asQueryable(QueryProvider queryProvider, SchemaPlus schema, string tableName)
        {
            return new AdoTableQueryable(this, queryProvider, schema, tableName);
        }

        /// <inheritdoc />
        public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable)
        {
            return new AdoTableScan(context.getCluster(), context.getTableHints(), relOptTable, this);
        }

        /// <inheritdoc />
        public Enumerable scan(DataContext root)
        {
            var typeFactory = root.getTypeFactory();
            var sql = GenerateSql();
            return AdoEnumerable.CreateReader(_dataSource, sql.getSql(), AdoUtils.CreateObjectArrayRowBuilderFactory(getRowType(typeFactory).getFieldList()));
        }

        /// <summary>
        /// Generates the SQL for the queryable.
        /// </summary>
        /// <returns></returns>
        SqlString GenerateSql()
        {
            var selectList = SqlNodeList.SINGLETON_STAR;
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <inheritdoc />
        public override object unwrap(Class aClass)
        {
            if (aClass.isInstance(_dataSource))
                return aClass.cast(_dataSource);
            else if (aClass.isInstance(_convention.Dialect))
                return aClass.cast(_convention.Dialect);
            else
                return base.unwrap(aClass);
        }

        /// <inheritdoc />
        public override string toString()
        {
            return $"AdoTable {_tableName}";
        }

    }

}
