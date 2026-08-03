using System;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Linq.Runtime
{

    /// <summary>
    /// A linq4j <see cref="Function0"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="function"></param>
    /// <remarks>
    /// An aggregate's lambdas go to <c>AggregateLambdaFactory</c>, which is Calcite's and takes Calcite's
    /// functional interfaces, so the lambda a tree yields is wrapped to be handed over.
    /// </remarks>
    public sealed class DelegateFunction0<TResult>(Func<TResult> function) : Function0
    {

        readonly Func<TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object apply() => function()!;

    }

    /// <summary>
    /// A linq4j <see cref="Function2"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0"></typeparam>
    /// <typeparam name="T1"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="function"></param>
    /// <inheritdoc cref="DelegateFunction0{TResult}" path="/remarks"/>
    public sealed class DelegateFunction2<T0, T1, TResult>(Func<T0, T1, TResult> function) : Function2
    {

        readonly Func<T0, T1, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object apply(object arg0, object arg1) => function((T0)arg0, (T1)arg1)!;

    }

    /// <summary>
    /// A linq4j <see cref="Function1"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="T0"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="function"></param>
    /// <inheritdoc cref="DelegateFunction0{TResult}" path="/remarks"/>
    public sealed class DelegateFunction1Of<T0, TResult>(Func<T0, TResult> function) : Function1
    {

        readonly Func<T0, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object apply(object arg0) => function((T0)arg0)!;

    }

}
