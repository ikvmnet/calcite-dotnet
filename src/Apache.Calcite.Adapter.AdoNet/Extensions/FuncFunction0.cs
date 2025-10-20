using System;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.AdoNet.Extensions
{

    class FuncFunction0<TResult> : Function0
    {

        readonly Func<TResult> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public FuncFunction0(Func<TResult> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public object apply()
        {
            return _func()!;
        }

    }

}
