using org.apache.calcite.runtime;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Pairs a Calcite <see cref="Hook"/> with the value to supply to it.
    /// </summary>
    internal readonly struct CalciteHookEntry
    {

        internal CalciteHookEntry(Hook hook, object? value)
        {
            Hook = hook;
            Value = BoxValue(value);
        }

        internal Hook Hook { get; }

        internal object? Value { get; }

        /// <summary>
        /// Converts CLR primitive types to their Java boxed equivalents so that
        /// <c>Hook.propertyJ</c> receives the correct Java type at planning time.
        /// Values that are already Java objects (or <see langword="null"/>) are returned unchanged.
        /// </summary>
        static object? BoxValue(object? value) => value switch
        {
            null => null,
            bool b => java.lang.Boolean.valueOf(b),
            int i => java.lang.Integer.valueOf(i),
            long l => java.lang.Long.valueOf(l),
            double d => java.lang.Double.valueOf(d),
            float f => java.lang.Float.valueOf(f),
            short s => java.lang.Short.valueOf(s),
            byte by => java.lang.Byte.valueOf(by),
            _ => value,
        };

    }

}
