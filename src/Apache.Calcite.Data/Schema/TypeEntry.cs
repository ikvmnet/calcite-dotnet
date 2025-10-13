using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Impl
{

    /// <summary>
    /// Membership of a type in a schema.
    /// </summary>
    public abstract class TypeEntry : Entry
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        protected TypeEntry(CalciteSchema schema, string name) :
            base(schema, name)
        {

        }

        /// <summary>
        /// Gets the underlying type.
        /// </summary>
        public abstract RelProtoDataType Type { get; }

    }

}
