using System;
using System.Collections.Immutable;

using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Membership of a table in a schema.
    /// </summary>
    public abstract class TableEntry : Entry
    {

        readonly ImmutableList<string> _sqls;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="sqls"></param>
        protected TableEntry(CalciteSchema schema, string name, ImmutableList<string> sqls) :
            base(schema, name)
        {
            _sqls = sqls ?? throw new ArgumentNullException(nameof(sqls));
        }

        /// <summary>
        /// Gets the underlying table.
        /// </summary>
        public abstract TableRef Table { get; }

        /// <summary>
        /// Gets the SQLs.
        /// </summary>
        public ImmutableList<string> Sqls => _sqls;

    }

}
