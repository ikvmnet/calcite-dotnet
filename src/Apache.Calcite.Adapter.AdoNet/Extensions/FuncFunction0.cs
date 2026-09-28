using System;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet.Extensions
{

    /// <summary>
    /// A linq4j <see cref="Function0"/> over a <see cref="Func{TResult}"/>.
    /// </summary>
    class FuncFunction0<TResult> : Function0
    {

        readonly Func<TResult> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func">The delegate to call.</param>
        /// <exception cref="ArgumentNullException"><paramref name="func"/> is <see langword="null"/>.</exception>
        public FuncFunction0(Func<TResult> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public object? apply()
        {
            return _func();
        }

    }

}
