using System;

using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Implementation of <see cref="TableEntry"/> where all properties are held in fields.
    /// </summary>
    class FunctionEntryImpl : FunctionEntry
    {

        readonly FunctionRef _function;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="function"></param>
        public FunctionEntryImpl(CalciteSchema schema, string name, FunctionRef function) :
            base(schema, name)
        {
            _function = !function.IsNull ? function : throw new ArgumentNullException(nameof(function));
        }

        /// <inheritdoc />
        public override FunctionRef Function => _function;

        /// <inheritdoc />
        public override bool IsMaterialization => _function.Underlying is MaterializedViewTable.MaterializedViewTableMacro;

    }

}
