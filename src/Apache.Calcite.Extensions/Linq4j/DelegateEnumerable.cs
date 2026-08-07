using System;

using org.apache.calcite.linq4j;

namespace Apache.Calcite.Extensions.Linq4j
{

    /// <summary>
    /// A linq4j <see cref="AbstractEnumerable"/> backed by a delegate.
    /// </summary>
    /// <remarks>
    /// The other half of what Calcite generates around a sub-plan: an enumerable whose one job is to hand out
    /// a fresh <see cref="DelegateEnumerator"/> each time it is asked. It is a class rather than an interface,
    /// so the adapter derives from it, but it has a single method and is otherwise an ordinary one.
    /// </remarks>
    sealed class DelegateEnumerable : AbstractEnumerable
    {

        readonly Func<Enumerator> onEnumerator;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="enumerator"></param>
        public DelegateEnumerable(Func<Enumerator> enumerator)
        {
            onEnumerator = enumerator ?? throw new ArgumentNullException(nameof(enumerator));
        }

        /// <inheritdoc />
        public override Enumerator enumerator() => onEnumerator();

    }

}
