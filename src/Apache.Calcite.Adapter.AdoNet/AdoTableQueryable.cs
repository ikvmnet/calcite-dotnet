using System;

using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.util;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A linq4j queryable over every row of an <see cref="AdoTable"/>, returned by
    /// <see cref="AdoTable.asQueryable"/>.
    /// </summary>
    /// <remarks>
    /// Enumerating it runs <c>SELECT *</c> against the table and yields each row as an <c>object[]</c>. The query
    /// provider must be a <see cref="CalciteConnection"/>, whose type factory types the rows.
    /// </remarks>
    public class AdoTableQueryable : AbstractTableQueryable
    {

        readonly AdoTable _adoTable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="adoTable">The table.</param>
        /// <param name="queryProvider">The query provider, a <see cref="CalciteConnection"/>.</param>
        /// <param name="schema">The schema the table is registered in.</param>
        /// <param name="tableName">The name the table is registered under.</param>
        /// <exception cref="ArgumentNullException"><paramref name="adoTable"/> is <see langword="null"/>.</exception>
        public AdoTableQueryable(AdoTable adoTable, QueryProvider queryProvider, SchemaPlus schema, string tableName) :
            base(queryProvider, schema, adoTable, tableName)
        {
            _adoTable = adoTable ?? throw new ArgumentNullException(nameof(adoTable));
        }

        /// <summary>
        /// Gets the table.
        /// </summary>
        public AdoTable Table => _adoTable;

        /// <summary>
        /// Runs the query and returns an enumerator over its rows.
        /// </summary>
        /// <returns>The enumerator, which owns the connection until it is closed.</returns>
        public override Enumerator enumerator()
        {
            var typeFactory = ((CalciteConnection)queryProvider).getTypeFactory();
            var fields = _adoTable.getRowType(typeFactory).getFieldList();
            var sql = GenerateSql();
            var enumerable = AdoEnumerable.CreateReader(_adoTable.Schema.DataSource, sql.getSql(), AdoUtils.CreateObjectArrayRowBuilderFactory(fields));
            return enumerable.enumerator();
        }

        /// <summary>
        /// Returns <c>SELECT *</c> from the table, in the schema's dialect.
        /// </summary>
        /// <returns>The SQL.</returns>
        SqlString GenerateSql()
        {
            var selectList = SqlNodeList.SINGLETON_STAR;
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, _adoTable.FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_adoTable.Schema.Convention.Dialect);
            var writer = new SqlPrettyWriter(config);
            node.unparse(writer, 0, 0);
            return writer.toSqlString();
        }

        /// <inheritdoc />
        public override string toString()
        {
            return $"AdoTableQueryable {{table: {tableName}}}";
        }

    }

}
