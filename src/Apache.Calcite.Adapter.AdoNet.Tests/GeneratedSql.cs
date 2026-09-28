using System.Collections.Generic;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Collects the statements the adapter generates, which the converters announce on
    /// <c>Hook.QUERY_PLAN</c> as they build each one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Asserting on the generated statement shows that the server evaluated the expression; the rows alone
    /// would also be right if the adapter had declined to push it down.
    /// </para>
    /// <para>
    /// IKVM does not project a Java default method onto a CLR class that implements the interface, so
    /// <c>andThen</c> is implemented here.
    /// </para>
    /// </remarks>
    sealed class GeneratedSql : java.util.function.Consumer
    {

        /// <summary>
        /// Two handlers run in order, as <c>Consumer.andThen</c> returns in Java.
        /// </summary>
        /// <param name="first">The handler run first.</param>
        /// <param name="then">The handler run second.</param>
        sealed class Composed(java.util.function.Consumer first, java.util.function.Consumer then) : java.util.function.Consumer
        {

            /// <inheritdoc />
            public void accept(object value)
            {
                first.accept(value);
                then.accept(value);
            }

            /// <inheritdoc />
            public java.util.function.Consumer andThen(java.util.function.Consumer after)
            {
                return new Composed(this, after);
            }

        }

        readonly List<string> _statements = [];

        /// <summary>
        /// Gets the statements generated so far.
        /// </summary>
        public IReadOnlyList<string> Statements => _statements;

        /// <inheritdoc />
        public void accept(object value)
        {
            _statements.Add(value?.ToString() ?? "");
        }

        /// <inheritdoc />
        public java.util.function.Consumer andThen(java.util.function.Consumer after)
        {
            return new Composed(this, after);
        }

    }

}
