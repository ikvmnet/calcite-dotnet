using System;

using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.Ado.Extensions
{

    class FuncFunction1<TArg, TResult> : Function1
    {

        readonly Func<TArg, TResult> _func;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="func"></param>
        /// <exception cref="ArgumentNullException"></exception>
        public FuncFunction1(Func<TArg, TResult> func)
        {
            _func = func ?? throw new ArgumentNullException(nameof(func));
        }

        public object apply(object obj)
        {
            return _func((TArg)obj)!;
        }

    }

}
