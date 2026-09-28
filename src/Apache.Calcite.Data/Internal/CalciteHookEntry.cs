using java.util.function;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A Calcite <see cref="Hook"/> and the consumer to attach to it while a statement executes.
    /// </summary>
    internal readonly struct CalciteHookEntry
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="hook">The hook to attach to.</param>
        /// <param name="consumer">The consumer the hook invokes.</param>
        internal CalciteHookEntry(Hook hook, Consumer consumer)
        {
            Hook = hook;
            Consumer = consumer;
        }

        /// <summary>
        /// Gets the hook to attach to.
        /// </summary>
        public Hook Hook { get; }

        /// <summary>
        /// Gets the consumer the hook invokes.
        /// </summary>
        public Consumer Consumer { get; }

    }

}
