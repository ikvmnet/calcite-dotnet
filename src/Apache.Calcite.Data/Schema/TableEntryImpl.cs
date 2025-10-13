using System;
using System.Collections.Immutable;

namespace Apache.Calcite.Data.Impl
{

    /// <summary>
    /// Implementation of <see cref="TableEntry"/> where all properties are held in fields.
    /// </summary>
    class TableEntryImpl : TableEntry
    {

        readonly org.apache.calcite.schema.Table _table;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="table"></param>
        /// <param name="sqls"></param>
        public TableEntryImpl(CalciteSchema schema, string name, org.apache.calcite.schema.Table table, ImmutableList<string> sqls) :
            base(schema, name, sqls)
        {
            _table = table ?? throw new ArgumentNullException(nameof(table));
        }

        /// <inheritdoc />
        public override org.apache.calcite.schema.Table Table => _table;

    }

}
