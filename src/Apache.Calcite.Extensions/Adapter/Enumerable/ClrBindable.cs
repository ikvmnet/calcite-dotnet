using System;
using System.Collections.Generic;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// A compiled plan, held as the delegate it compiled to.
    /// </summary>
    /// <param name="plan">The compiled plan.</param>
    /// <param name="elementType">The Java type of one row.</param>
    /// <remarks>
    /// What Calcite's Janino-generated class is, and produced at the same point. <c>toBindable</c> compiles
    /// there too — <c>compileToBindable</c> cooks the source, loads the class and calls
    /// <c>newInstance</c> — so what it hands back is already compiled, exactly as this is. The artifact is
    /// what differs: a class whose <c>bind</c> holds the plan's root block, against a delegate the same
    /// block compiled to.
    /// </remarks>
    sealed class ClrBindable(Func<DataContext, IEnumerable<object>> plan, java.lang.reflect.Type elementType) : IClrBindable
    {

        readonly Func<DataContext, IEnumerable<object>> plan = plan ?? throw new ArgumentNullException(nameof(plan));
        readonly java.lang.reflect.Type elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));

        /// <inheritdoc />
        public IEnumerable<object> Bind(DataContext root)
        {
            ArgumentNullException.ThrowIfNull(root);

            return plan(root);
        }

        /// <inheritdoc />
        public java.lang.reflect.Type ElementType => elementType;

    }

}
