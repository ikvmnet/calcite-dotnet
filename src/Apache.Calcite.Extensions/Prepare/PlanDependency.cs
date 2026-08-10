using System;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// One table a plan reads or writes: the qualified name it was resolved by, and the instance it
    /// resolved to.
    /// </summary>
    /// <remarks>
    /// The instance matters as much as the name. A Calcite-SPI table is looked up by name when the plan
    /// runs, so a replacement would be found — but an <c>IClrScannableTable</c> is held in the compiled
    /// tree as the instance itself, and a replacement under the same name would go silently unread.
    /// Comparing the instance the name resolves to now against the one it resolved to at plan time catches
    /// both, without knowing which kind of table it is looking at.
    /// </remarks>
    sealed class PlanDependency
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="path">The table's qualified name, from the root schema down.</param>
        /// <param name="table">The table the name resolved to at plan time.</param>
        public PlanDependency(string[] path, org.apache.calcite.schema.Table table)
        {
            Path = path ?? throw new ArgumentNullException(nameof(path));
            Table = table ?? throw new ArgumentNullException(nameof(table));
        }

        /// <summary>
        /// Gets the table's qualified name, from the root schema down.
        /// </summary>
        public string[] Path { get; }

        /// <summary>
        /// Gets the table the name resolved to at plan time.
        /// </summary>
        public org.apache.calcite.schema.Table Table { get; }

    }

}
