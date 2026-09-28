using System;
using System.Data;
using System.Threading;

using Apache.Calcite.Adapter.AdoNet.Extensions;

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

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A table of an ADO.NET data source, as a Calcite table. The counterpart of Calcite's <c>JdbcTable</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The planner turns a reference to the table into an <see cref="AdoTableScan"/> in the schema's
    /// <see cref="AdoConvention"/>, from which filters, projections and the rest are pushed down. The table can
    /// also be read whole through <see cref="scan"/> or <see cref="asQueryable"/>, each of which runs
    /// <c>SELECT *</c> against it.
    /// </para>
    /// <para>
    /// <see cref="AdoSchema"/> creates the instances. The row type is read from the schema's metadata the first
    /// time it is asked for and then kept.
    /// </para>
    /// </remarks>
    public class AdoTable : AbstractQueryableTable, TranslatableTable, ScannableTable
    {

        readonly java.util.function.Supplier protoRowTypeSupplier;

        readonly AdoSchema _schema;
        readonly string? _databaseName;
        readonly string? _schemaName;
        readonly string _tableName;
        readonly Schema.TableType _tableType;

        SqlIdentifier? _fullyQualifiedTableName;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema">The schema the table belongs to.</param>
        /// <param name="databaseName">The table's database, or <see langword="null"/>.</param>
        /// <param name="schemaName">The table's schema, or <see langword="null"/>.</param>
        /// <param name="tableName">The table's name.</param>
        /// <param name="tableType">The kind of table, as JDBC names it.</param>
        /// <exception cref="ArgumentNullException"><paramref name="schema"/>, <paramref name="tableName"/> or
        /// <paramref name="tableType"/> is <see langword="null"/>.</exception>
        internal AdoTable(AdoSchema schema, string? databaseName, string? schemaName, string tableName, Schema.TableType tableType) :
            base((Class)typeof(object[]))
        {
            protoRowTypeSupplier = Suppliers.memoize(new FuncSupplier<RelProtoDataType>(SupplyProto));

            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _databaseName = databaseName;
            _schemaName = schemaName;
            _tableName = tableName ?? throw new ArgumentNullException(nameof(tableName));
            _tableType = tableType ?? throw new ArgumentNullException(nameof(tableType));
        }

        /// <summary>
        /// Gets the schema the table belongs to, through which its data source, convention and dialect are
        /// reached.
        /// </summary>
        public AdoSchema Schema => _schema;

        /// <inheritdoc />
        public override Schema.TableType getJdbcTableType() => _tableType;

        /// <summary>
        /// Gets the table's database, or <see langword="null"/>.
        /// </summary>
        public string? DatabaseName => _databaseName;

        /// <summary>
        /// Gets the table's schema, or <see langword="null"/>.
        /// </summary>
        public string? SchemaName => _schemaName;

        /// <summary>
        /// Gets the table's name.
        /// </summary>
        public string TableName => _tableName;

        /// <inheritdoc />
        /// <exception cref="AdoCalciteException">The table has a column of a type the adapter cannot map.</exception>
        public override RelDataType getRowType(RelDataTypeFactory typeFactory)
        {
            return (RelDataType)((RelProtoDataType)protoRowTypeSupplier.get()).apply(typeFactory);
        }

        /// <summary>
        /// Reads the row type from the schema. Mirrors <c>JdbcTable.supplyProto</c>; the result is memoized.
        /// </summary>
        /// <returns>A prototype of the row type.</returns>
        /// <exception cref="AdoCalciteException">A column's type cannot be mapped.</exception>
        RelProtoDataType SupplyProto()
        {
            return _schema.GetRelDataType(_databaseName, _schemaName, _tableName);
        }

        /// <summary>
        /// Returns <c>SELECT *</c> from the table, in the schema's dialect.
        /// </summary>
        /// <returns>The SQL.</returns>
        internal SqlString GenerateSqlString()
        {
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, SqlNodeList.SINGLETON_STAR, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_schema.Convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <summary>
        /// Gets the table's name as a <see cref="SqlIdentifier"/>, qualified by its database and schema where
        /// each is known.
        /// </summary>
        /// <returns>An identifier of one to three parts, built once and reused.</returns>
        public SqlIdentifier FullyQualifiedTableName => GetFullyQualifiedTableName();

        /// <summary>
        /// Builds <see cref="FullyQualifiedTableName"/> on first use.
        /// </summary>
        /// <returns>The identifier.</returns>
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

        /// <summary>
        /// Returns every row of the table, each as an <c>object[]</c>. The query runs when the result is
        /// enumerated.
        /// </summary>
        /// <param name="root">The context, which supplies the type factory.</param>
        /// <returns>The rows.</returns>
        public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
        {
            var typeFactory = root.getTypeFactory();
            var sql = GenerateSql();
            return AdoEnumerable.CreateReader(_schema.DataSource, sql.getSql(), AdoUtils.CreateObjectArrayRowBuilderFactory(getRowType(typeFactory).getFieldList()));
        }

        /// <summary>
        /// Returns <c>SELECT *</c> from the table, in the schema's dialect.
        /// </summary>
        /// <returns>The SQL.</returns>
        SqlString GenerateSql()
        {
            var selectList = SqlNodeList.SINGLETON_STAR;
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_schema.Convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <summary>
        /// Returns the schema's <see cref="AdoDataSource"/> or its dialect where either is an instance of
        /// <paramref name="aClass"/>, and otherwise defers to the base class.
        /// </summary>
        /// <param name="aClass">The class to unwrap to.</param>
        /// <returns>The object, or <see langword="null"/>.</returns>
        public override object unwrap(Class aClass)
        {
            if (aClass.isInstance(_schema.DataSource))
                return aClass.cast(_schema.DataSource);
            else if (aClass.isInstance(_schema.Convention.Dialect))
                return aClass.cast(_schema.Convention.Dialect);
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
