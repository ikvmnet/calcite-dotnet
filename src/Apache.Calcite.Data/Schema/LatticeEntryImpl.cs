using System;

using org.apache.calcite.materialize;

namespace Apache.Calcite.Data.Schema
{

    /// <summary>
    /// Implementation of <see cref="TableEntry"/> where all properties are held in fields.
    /// </summary>
    public class LatticeEntryImpl : LatticeEntry
    {

        readonly LatticeRef _lattice;
        readonly TableEntry _starTableEntry;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="lattice"></param>
        public LatticeEntryImpl(CalciteSchema schema, string name, LatticeRef lattice) :
            base(schema, name)
        {
            _lattice = !lattice.IsNull ? lattice : throw new ArgumentNullException(nameof(lattice));
            _starTableEntry = schema.Add(name, lattice.CreateStarTable());
        }

        /// <inheritdoc />
        public override LatticeRef Lattice => _lattice;

        /// <inheritdoc />
        public override TableEntry StarTable => _starTableEntry;

    }

}
