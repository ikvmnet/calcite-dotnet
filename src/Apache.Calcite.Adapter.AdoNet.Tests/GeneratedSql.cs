using System.Collections.Generic;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Collects the statements the adapter generates, which the converters announce on
    /// <c>Hook.QUERY_PLAN</c> as they build each one.
    /// </summary>
    /// <remarks>
    /// <para>
    /// What a test asserts about the rows is that the answer is right; what it asserts about these is that
    /// the server is what answered. The two are separate claims, and a query the adapter declined to push
    /// down would satisfy the first while saying nothing about the dialect.
    /// </para>
    /// <para>
    /// IKVM does not project a Java default method onto a CLR class that implements the interface, so
    /// <c>andThen</c> has to be written even though a hook handler is never composed with another.
    /// </para>
    /// </remarks>
    sealed class GeneratedSql : java.util.function.Consumer
    {

        /// <summary>
        /// Two handlers as one, which is what <c>Consumer.andThen</c> answers in Java.
        /// </summary>
        /// <param name="first"></param>
        /// <param name="then"></param>
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
