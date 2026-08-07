using System.Collections.Generic;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Linq.Tests
{

    /// <summary>
    /// Runs a compiled plan of either convention and yields its rows.
    /// </summary>
    /// <remarks>
    /// The two conventions no longer compile to the same type — this one produces an
    /// <see cref="IClrBindable"/> and Calcite's produces a <c>Bindable</c> — so a harness that runs both
    /// needs one place where that difference is absorbed. The rows are handed back untouched either way,
    /// which is what a differential comparison requires.
    /// </remarks>
    static class TestRows
    {

        /// <summary>
        /// Runs a plan of the <see cref="ClrEnumerableConvention"/> calling convention.
        /// </summary>
        /// <param name="bindable"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        public static IEnumerable<object> Of(IClrBindable bindable, DataContext root)
        {
            return bindable.Bind(root);
        }

        /// <summary>
        /// Runs a plan of <c>EnumerableConvention</c>.
        /// </summary>
        /// <param name="bindable"></param>
        /// <param name="root"></param>
        /// <returns></returns>
        public static IEnumerable<object> Of(Bindable bindable, DataContext root)
        {
            return FromJava(bindable.bind(root));
        }

        /// <summary>
        /// Reads a linq4j sequence, yielding each row exactly as the enumerator gave it.
        /// </summary>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// Not <c>JavaSequences.FromJava</c>, which is an adapter and converts. A comparison wants to see
        /// what Calcite's plan actually produced.
        /// </remarks>
        static IEnumerable<object> FromJava(Enumerable source)
        {
            var enumerator = source.enumerator();

            try
            {
                while (enumerator.moveNext())
                    yield return enumerator.current();
            }
            finally
            {
                enumerator.close();
            }
        }

    }

}
