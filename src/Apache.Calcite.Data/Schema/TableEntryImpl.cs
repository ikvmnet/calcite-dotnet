using System;
using System.Collections.Immutable;

using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Implementation of <see cref="TableEntry"/> where all properties are held in fields.
    /// </summary>
    class TableEntryImpl : TableEntry
    {

        readonly TableRef _table;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="table"></param>
        /// <param name="sqls"></param>
        public TableEntryImpl(CalciteSchema schema, string name, TableRef table, ImmutableList<string> sqls) :
            base(schema, name, sqls)
        {
            _table = !table.IsNull ? table : throw new ArgumentNullException(nameof(table));
        }

        /// <inheritdoc />
        public override TableRef Table => _table;

    }

}
