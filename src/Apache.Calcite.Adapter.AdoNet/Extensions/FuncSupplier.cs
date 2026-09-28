using System;

using com.google.common.@base;

namespace Apache.Calcite.Adapter.AdoNet.Extensions
{

    /// <summary>
    /// A Guava <see cref="Supplier"/> over a <see cref="Func{TResult}"/>.
    /// </summary>
    /// <typeparam name="T">The type the delegate returns.</typeparam>
    class FuncSupplier<T> : Supplier
    {

        readonly Func<T> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func">The delegate to call.</param>
        /// <exception cref="ArgumentNullException"><paramref name="func"/> is <see langword="null"/>.</exception>
        public FuncSupplier(Func<T> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public object? get()
        {
            return _func();
        }

    }

}
