using System;
using System.Collections.Generic;
using System.Collections.Immutable;
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
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Queryable table that gets its data from a table within a ADO connection.
    /// </summary>
    public class AdoTable : AbstractQueryableTable, ScannableTable
    {

        readonly Supplier protoRowTypeSupplier;

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
        internal AdoTable(AdoSchema schema, string? databaseName, string? schemaName, string tableName, Schema.TableType tableType)
            : base((java.lang.Class)typeof(object[]))
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
        public override Schema.TableType getJdbcTableType()
        {
            return _tableType;
        }

        /// <inheritdoc />
        public override object unwrap(java.lang.Class aClass)
        {
            if (aClass.isInstance(_adoSchema.DataSource))
                return aClass.cast(_adoSchema.DataSource);
            else if (aClass.isInstance(_adoSchema.Convention.Dialect))
                return aClass.cast(_adoSchema.Convention.Dialect);
            else
                return base.unwrap(aClass);
        }

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return (RelDataType)((RelProtoDataType)protoRowTypeSupplier.get()).apply(typeFactory);
        }

        /// <summary>
        /// Gets the <see cref="RelProtoDataType"/> for the row type of this table.
        /// </summary>
        /// <returns></returns>
        /// <exception cref="AdoSchemaException"></exception>
        RelProtoDataType GetRowProtoDataType()
        {
            try
            {
                return _adoSchema.GetRowProtoDataType(_databaseName, _schemaName, _tableName);
            }
            catch (System.Exception e)
            {
                throw new AdoSchemaException($"Exception while reading definition of table '{_tableName}'.", e);
            }
        }

        /// <summary>
        /// For each field of the table returns the <see cref="ColumnMetaData.Rep"/> and <see cref="DbType"/>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <returns></returns>
        internal ImmutableArray<(ColumnMetaData.Rep, DbType)> GetFieldRepAndDbTypes(JavaTypeFactory typeFactory)
        {
            return getRowType(typeFactory)
                .getFieldList()
                .AsEnumerable<RelDataTypeField>()
                .Select(field =>
                {
                    var dataType = field.getType();
                    var sqlTypeName = dataType.getSqlTypeName();
                    var dbType = sqlTypeName.ToDbType();
                    var rep = GetTypeRep(typeFactory, dataType, sqlTypeName, dbType);
                    return (rep, dbType);
                })
                .ToImmutableArray();
        }

        /// <summary>
        /// Returns the appropriate Avatica Rep value for the iven <see cref="RelDataType"/>.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="dataType"></param>
        /// <param name="sqlTypeName"></param>
        /// <param name="dbType"></param>
        /// <returns></returns>
        internal ColumnMetaData.Rep GetTypeRep(JavaTypeFactory typeFactory, RelDataType dataType, SqlTypeName sqlTypeName, DbType dbType)
        {
            // GUID cannot be mapped to JDBC using UUID, so we map as String
            if (dbType == DbType.Guid)
                return ColumnMetaData.Rep.STRING;

            // check with JavaTypeFactory for built in type
            var clazz = typeFactory.getJavaClass(dataType);
            if (clazz is not null)
                return ColumnMetaData.Rep.of(clazz);

            // fall back to non translated object
            return ColumnMetaData.Rep.OBJECT;
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
            return AdoEnumerable.CreateReader(_adoSchema.DataSource, sql.getSql(), AdoUtils.RowBuilderFactory2(GetFieldRepAndDbTypes(typeFactory)));
        }

        /// <inheritdoc />
        public override string toString()
        {
            return $"AdoTable {_tableName}";
        }

    }

}
