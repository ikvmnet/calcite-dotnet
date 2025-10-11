using System;

using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql;
using org.apache.calcite.sql.parser;
using org.apache.calcite.sql.pretty;
using org.apache.calcite.sql.util;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Queryable implementation across an entire <see cref="AdoTable"/>.
    /// </summary>
    public class AdoTableQueryable : AbstractTableQueryable
    {

        readonly AdoTable _adoTable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="adoTable"></param>
        /// <param name="queryProvider"></param>
        /// <param name="schema"></param>
        /// <param name="tableName"></param>
        public AdoTableQueryable(AdoTable adoTable, QueryProvider queryProvider, SchemaPlus schema, string tableName) :
            base(queryProvider, schema, adoTable, tableName)
        {
            _adoTable = adoTable ?? throw new ArgumentNullException(nameof(adoTable));
        }

        /// <summary>
        /// Gets the ADO table being queried.
        /// </summary>
        public AdoTable Table => _adoTable;

        /// <inheritdoc />
        public override Enumerator enumerator()
        {
            var typeFactory = ((CalciteConnection)queryProvider).getTypeFactory();
            var fields = _adoTable.getRowType(typeFactory).getFieldList();
            var sql = GenerateSql();
            var enumerable = AdoEnumerable.CreateReader(_adoTable.DataSource, sql.getSql(), AdoUtils.CreateObjectArrayRowBuilderFactory(fields));
            return enumerable.enumerator();
        }

        /// <summary>
        /// Generates the SQL for the queryable.
        /// </summary>
        /// <returns></returns>
        SqlString GenerateSql()
        {
            var selectList = SqlNodeList.SINGLETON_STAR;
            var node = new SqlSelect(SqlParserPos.ZERO, SqlNodeList.EMPTY, selectList, _adoTable.FullyQualifiedTableName, null, null, null, null, null, null, null, null, null);
            var config = SqlPrettyWriter.config().withAlwaysUseParentheses(true).withDialect(_adoTable.Dialect);
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
