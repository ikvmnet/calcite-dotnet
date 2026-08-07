using System;
using System.Collections.Generic;

using org.apache.calcite;

using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// A compiled plan, held as the delegate it compiled to.
    /// </summary>
    /// <param name="plan">The compiled plan.</param>
    /// <param name="elementType">The Java type of one row.</param>
    /// <remarks>
    /// What Calcite's Janino-generated class is: <c>bind</c> is the plan's root block, and
    /// <c>getElementType</c> returns a constant. Here the block is a lambda that has already been
    /// compiled, so this holds it and answers both.
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
