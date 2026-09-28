using System;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Linq4j
{

    /// <summary>
    /// A linq4j <see cref="AbstractEnumerable"/> backed by a delegate.
    /// </summary>
    /// <remarks>
    /// What an anonymous <c>AbstractEnumerable</c> in a Calcite-generated block becomes when translated; its
    /// one method usually returns a new <see cref="DelegateEnumerator"/>.
    /// </remarks>
    sealed class DelegateEnumerable : AbstractEnumerable
    {

        readonly Func<Enumerator> onEnumerator;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="enumerator">Called for each <c>enumerator()</c> call.</param>
        public DelegateEnumerable(Func<Enumerator> enumerator)
        {
            onEnumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
        }

        /// <inheritdoc />
        public override Enumerator enumerator() => onEnumerator();

    }

}
