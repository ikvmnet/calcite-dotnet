using System;

using org.apache.calcite.linq4j.function;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A linq4j <see cref="Function1"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="TArg"></typeparam>
    /// <typeparam name="TResult"></typeparam>
    /// <param name="function"></param>
    /// <remarks>
    /// Calcite passes a method reference where a Function1 is wanted; C# has no such conversion for an
    /// interface IKVM compiled, so the delegate is wrapped.
    /// </remarks>
    class DelegateFunction1<TArg, TResult>(Func<TArg, TResult> function) : Function1
    {

        readonly Func<TArg, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object apply(object arg)
        {
            return JavaValues.From(function(JavaValues.As<TArg>(arg)));
        }

    }

}
