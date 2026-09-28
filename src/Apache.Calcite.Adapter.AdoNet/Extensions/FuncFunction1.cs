using System;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet.Extensions
{

    /// <summary>
    /// A linq4j <see cref="Function1"/> over a <see cref="Func{T, TResult}"/>. The argument is cast to
    /// <typeparamref name="TArg"/>.
    /// </summary>
    class FuncFunction1<TArg, TResult> : Function1
    {

        readonly Func<TArg, TResult> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func">The delegate to call.</param>
        /// <exception cref="ArgumentNullException"><paramref name="func"/> is <see langword="null"/>.</exception>
        public FuncFunction1(Func<TArg, TResult> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public object? apply(object obj)
        {
            return _func((TArg)obj);
        }

    }

}
