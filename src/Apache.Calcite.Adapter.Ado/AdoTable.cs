using System;
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
using org.apache.calcite.sql.util;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Queryable table that gets its data from a table within a ADO connection.
    /// </summary>
    public class AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable
    {

        readonly java.util.function.Supplier protoRowTypeSupplier;

        readonly AdoSchema _adoSchema;
        readonly string? _databaseName;
        readonly string? _schemaName;
        readonly string _tableName;
        readonly Schema.TableType _tableType;

        SqlIdentifier? _fullyQualifiedTableName;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="tableType"></param>
        /// <exception cref="ArgumentNullException"></exception>
        internal AdoTable(AdoSchema schema, string? databaseName, string? schemaName, string tableName, Schema.TableType tableType) :
            base((Class)typeof(object[]))
        {
            protoRowTypeSupplier = Suppliers.memoize(new FuncSupplier<RelProtoDataType>(GetRowProtoDataType));

            _adoSchema = schema ?? throw new ArgumentNullException(nameof(schema));
            _databaseName = databaseName;
            _schemaName = schemaName;
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _tableType = tableType ?? throw new ArgumentNullException(nameof(tableType));
        }

        /// <summary>
        /// Gets the owning schema of this table.
        /// </summary>
        public AdoSchema Schema => _adoSchema;

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
            return _adoSchema.GetRowProtoDataType(_databaseName, _schemaName, _tableName);
        }

        /// <summary>
        /// Returns a SQL string that selects from the table.
        /// </summary>
        /// <returns></returns>
        internal SqlString GenerateSqlString()
        {
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_adoSchema.Convention.Dialect);
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

                if (_adoSchema.DatabaseName is not null)
                    names.add(_adoSchema.DatabaseName);

                if (_adoSchema.SchemaName is not null)
                    names.add(_adoSchema.SchemaName);

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
            return AdoEnumerable.CreateReader(_adoSchema.DataSource, sql.getSql(), AdoUtils.CreateObjectArrayRowBuilderFactory(getRowType(typeFactory).getFieldList()));
        }

        /// <summary>
        /// Generates the SQL for the queryable.
        /// </summary>
        /// <returns></returns>
        SqlString GenerateSql()
        {
            var selectList = SqlNodeList.SINGLETON_STAR;
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(Schema.Convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <inheritdoc />
        public override object unwrap(Class aClass)
        {
            if (aClass.isInstance(_adoSchema.DataSource))
                return aClass.cast(_adoSchema.DataSource);
            else if (aClass.isInstance(_adoSchema.Convention.Dialect))
                return aClass.cast(_adoSchema.Convention.Dialect);
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
