using System.Collections.Generic;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a compiled plan of Calcite's convention and yields its rows.
    /// </summary>
    /// <remarks>
    /// The rows are handed back untouched, which is what a differential comparison against Calcite
    /// requires.
    /// </remarks>
    static class TestRows
    {

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
