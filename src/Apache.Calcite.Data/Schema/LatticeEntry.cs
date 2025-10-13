
using org.apache.calcite.materialize;

namespace Apache.Calcite.Data.Impl
{

    /// <summary>
    /// Membership of a lattice in a schema.
    /// </summary>
    public abstract class LatticeEntry : Entry
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        protected LatticeEntry(CalciteSchema schema, string name) :
            base(schema, name)
        {

        }

        /// <summary>
        /// Gets the underlying lattice.
        /// </summary>
        public abstract Lattice Lattice { get; }

        /// <summary>
        /// 
        /// </summary>
        public abstract TableEntry StarTable { get; }

    }

}
