using java.util.function;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Pairs a Calcite <see cref="Hook"/> with the value to supply to it.
    /// </summary>
    internal readonly struct CalciteHookEntry
    {

        /// <summary>
        /// Adds a hook that is a consumer.
        /// </summary>
        /// <param name="hook"></param>
        /// <param name="consumer"></param>
        internal CalciteHookEntry(Hook hook, Consumer consumer)
        {
            Hook = hook;
            Consumer = consumer;
        }

        public Hook Hook { get; }

        public Consumer Consumer { get; }

    }

}
