using System;

using org.apache.calcite.linq4j.function;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Linq4j.Function
{

    /// <summary>
    /// A linq4j <see cref="Function1"/> backed by a delegate.
    /// </summary>
    /// <typeparam name="TArg">The argument type.</typeparam>
    /// <typeparam name="TResult">The result type.</typeparam>
    /// <param name="function">The function.</param>
    /// <remarks>
    /// C# cannot convert a method group to an interface IKVM compiled, so a method passed where Calcite
    /// takes a <c>Function1</c> is wrapped in this. The argument and result are converted with
    /// <see cref="JavaValues"/>.
    /// </remarks>
    class DelegateFunction1<TArg, TResult>(Func<TArg, TResult> function) : Function1
    {

        readonly Func<TArg, TResult> function = function ?? throw new ArgumentNullException(nameof(function));

        /// <inheritdoc />
        public object? apply(object? arg)
        {
            return JavaValues.From(function(JavaValues.As<TArg>(arg)));
        }

    }

}
