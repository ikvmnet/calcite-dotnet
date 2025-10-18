using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Membership of a function in a schema.
    /// </summary>
    public abstract class FunctionEntry : Entry
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        protected FunctionEntry(CalciteSchema schema, string name) :
            base(schema, name)
        {

        }

        /// <summary>
        /// Gets the underlying function.
        /// </summary>
        public abstract FunctionRef Function { get; }

        /// <summary>
        /// Gets whether this represents a materialized view.
        /// </summary>
        public abstract bool IsMaterialization { get; }

    }

}
