using System;

using Apache.Calcite.Extensions.Interop;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A linq4j <see cref="Predicate1"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0"></typeparam>
    /// <param name="predicate"></param>
    /// <remarks>
    /// The counterpart of <see cref="DelegateFunction1Of{T0, TResult}"/> for the interface linq4j uses where
    /// the answer is a <see cref="bool"/> rather than a value. It is separate from the function adapters
    /// because <c>Predicate1</c> is not a <c>Function1</c> of <c>Boolean</c>: it declares its own
    /// <c>apply</c> returning a primitive, so a <c>Function1</c> put in its place does not implement it.
    /// </remarks>
    sealed class DelegatePredicate1<T0>(Func<T0, bool> predicate) : Predicate1
    {

        readonly Func<T0, bool> predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

        /// <inheritdoc />
        public bool apply(object? v0) => predicate(JavaValues.As<T0>(v0));

    }

    /// <summary>
    /// A linq4j <see cref="Predicate2"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0"></typeparam>
    /// <typeparam name="T1"></typeparam>
    /// <param name="predicate"></param>
    /// <inheritdoc cref="DelegatePredicate1{T0}" path="/remarks"/>
    sealed class DelegatePredicate2<T0, T1>(Func<T0, T1, bool> predicate) : Predicate2
    {

        readonly Func<T0, T1, bool> predicate = predicate ?? throw new ArgumentNullException(nameof(predicate));

        /// <inheritdoc />
        public bool apply(object? v0, object? v1) => predicate(JavaValues.As<T0>(v0), JavaValues.As<T1>(v1));

    }

}
