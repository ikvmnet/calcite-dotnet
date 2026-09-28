using System;

using org.apache.calcite.linq4j.function;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A linq4j <see cref="Function0"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function.</param>
    /// <remarks>
    /// Wraps a compiled lambda where Calcite takes one of linq4j's functional interfaces, such as an
    /// aggregate's lambdas passed to <c>AggregateLambdaFactory</c>. Arguments and results are converted with
    /// <see cref="JavaValues"/>.
    /// </remarks>
    sealed class DelegateFunction0<TResult>(Func<TResult> function) : Function0
    {

        readonly Func<TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object? apply() => JavaValues.From(function());

    }

    /// <summary>
    /// A linq4j <see cref="Function2"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0">The first argument type.</typeparam>
    /// <typeparam name="T1">The second argument type.</typeparam>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function.</param>
    /// <inheritdoc cref="DelegateFunction0{TResult}" path="/remarks"/>
    sealed class DelegateFunction2<T0, T1, TResult>(Func<T0, T1, TResult> function) : Function2
    {

        readonly Func<T0, T1, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object? apply(object? arg0, object? arg1) => JavaValues.From(function(JavaValues.As<T0>(arg0), JavaValues.As<T1>(arg1)));

    }

    /// <summary>
    /// A linq4j <see cref="Function1"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0">The argument type.</typeparam>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function.</param>
    /// <inheritdoc cref="DelegateFunction0{TResult}" path="/remarks"/>
    sealed class DelegateFunction1Of<T0, TResult>(Func<T0, TResult> function) : Function1
    {

        readonly Func<T0, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object? apply(object? arg0) => JavaValues.From(function(JavaValues.As<T0>(arg0)));

    }

}
