using System.Collections.Generic;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Tests
{

    /// <summary>
    /// Runs a compiled plan of <c>EnumerableConvention</c> and yields its rows unconverted, as a differential
    /// comparison against Calcite requires.
    /// </summary>
    static class TestRows
    {

        /// <summary>
        /// Binds <paramref name="bindable"/> to <paramref name="root"/> and yields its rows.
        /// </summary>
        /// <param name="bindable">The compiled plan.</param>
        /// <param name="root">The data context to bind it to.</param>
        /// <returns>The rows, read lazily.</returns>
        public static IEnumerable<object> Of(Bindable bindable, DataContext root)
        {
            return FromJava(bindable.bind(root));
        }

        /// <summary>
        /// Yields each row of a linq4j sequence exactly as its enumerator returns it.
        /// </summary>
        /// <param name="source">The linq4j sequence to read.</param>
        /// <returns>The rows, each as the enumerator's <c>current()</c> returns it; the enumerator is closed at the end.</returns>
        /// <remarks>
        /// <c>JavaCursors.FromJava</c> is not used because it converts each row through <c>JavaValues.As</c>,
        /// and the comparison needs what Calcite's plan produced.
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
