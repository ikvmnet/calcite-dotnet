using System;

using com.google.common.@base;

namespace Apache.Calcite.Adapter.AdoNet.Extensions
{

    /// <summary>
    /// Implements the <see cref="Supplier"/> interface around a Func.
    /// </summary>
    /// <typeparam name="T"></typeparam>
    class FuncSupplier<T> : Supplier
    {

        readonly Func<T> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func"></param>
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
