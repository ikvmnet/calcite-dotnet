using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Threading;

using Apache.Calcite.Adapter.AdoNet.Extensions;

using com.google.common.@base;

using java.lang;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.util;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Queryable table that gets its data from a table within a ADO connection.
    /// </summary>
    public class AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable
    {

        readonly Supplier protoRowTypeSupplier;

        readonly AdoSchema _adoSchema;
        readonly string? _catalogName;
        readonly string? _schemaName;
        readonly string _tableName;
        readonly Schema.TableType _tableType;

        SqlIdentifier? _fullyQualifiedTableName;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="catalogName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <param name="tableType"></param>
        /// <exception cref="ArgumentNullException"></exception>
        internal AdoTable(AdoSchema schema, string? catalogName, string? schemaName, string tableName, Schema.TableType tableType)
            : base((java.lang.Class)typeof(object[]))
        {
            protoRowTypeSupplier = Suppliers.memoize(new FuncSupplier<RelProtoDataType>(SupplyProto));

            _adoSchema = schema ?? throw new ArgumentNullException(nameof(schema));
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _tableType = tableType ?? throw new ArgumentNullException(nameof(tableType));
            _catalogName = catalogName;
            _schemaName = schemaName;
        }

        /// <inheritdoc />
        public override string toString()
        {
            return $"AdoTable {_tableName}";
        }

        /// <summary>
        /// Gets the owning schema of this table.
        /// </summary>
        public AdoSchema Schema => _adoSchema;

        /// <inheritdoc />
        public override Schema.TableType getJdbcTableType()
        {
            return _tableType;
        }

        /// <inheritdoc />
        public override object unwrap(java.lang.Class aClass)
        {
            if (aClass.isInstance(_adoSchema.DataSource))
                return aClass.cast(_adoSchema.DataSource);
            else if (aClass.isInstance(_adoSchema.Dialect))
                return aClass.cast(_adoSchema.Dialect);
            else
                return base.unwrap(aClass);
        }

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return (RelDataType)((RelProtoDataType)protoRowTypeSupplier.get()).apply(typeFactory);
        }

        RelProtoDataType SupplyProto()
        {
            try
            {
                return _adoSchema.GetRelDataType(_catalogName, _schemaName, _tableName);
            }
            catch (System.Exception e)
            {
                throw new AdoSchemaException($"Exception while reading definition of table '{_tableName}'.", e);
            }
        }

        /// <summary>
        /// Derives the type information for the fields of the table.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        internal IEnumerable<(ColumnMetaData.Rep, DbType)> GetFieldTypes(JavaTypeFactory typeFactory)
        {
            return getRowType(typeFactory).getFieldList().AsEnumerable<RelDataTypeField>().Select(field =>
            {
                var type = field.getType();
                var rep = (ColumnMetaData.Rep)Util.first(ColumnMetaData.Rep.of(typeFactory.getJavaClass(type)), ColumnMetaData.Rep.OBJECT);
                return (rep, type.getSqlTypeName().ToDbType());
            });
        }

        /// <summary>
        /// Returns a SQL string that selects from the table.
        /// </summary>
        /// <returns></returns>
        internal SqlString GenerateSqlString()
        {
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_adoSchema.Dialect);
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
                names.add(_adoSchema.DatabaseName);
                names.add(_adoSchema.SchemaName);
                names.add(_tableName);
                Interlocked.CompareExchange(ref _fullyQualifiedTableName, new SqlIdentifier(names, SqlParserPos.ZERO), null);
            }

            return _fullyQualifiedTableName;
        }

        /// <inheritdoc />
        public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable)
        {
            return new AdoTableScan(context.getCluster(), context.getTableHints(), relOptTable, this, _adoSchema.Convention);
        }

        /// <inheritdoc />
        public override org.apache.calcite.linq4j.Queryable asQueryable(QueryProvider queryProvider, SchemaPlus schema, string tableName)
        {
            return new AdoTableQueryable(this, queryProvider, schema, tableName);
        }

        /// <inheritdoc />
        public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
        {
            var typeFactory = root.getTypeFactory();
            var sql = GenerateSqlString();
            return AdoEnumerable.CreateReader(_adoSchema.DataSource, sql.getSql(), AdoUtils.RowBuilderFactory2(GetFieldTypes(typeFactory)));
        }

    }

}
