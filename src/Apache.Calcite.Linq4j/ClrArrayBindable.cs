using System;

using org.apache.calcite.linq4j;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Linq4j
{

    /// <summary>
    /// A concrete <see cref="ArrayBindable"/> backed by a CLR delegate, returned by
    /// <see cref="ExpressionClrBindableCompiler"/> in place of a Janino-compiled class.
    /// </summary>
    sealed class ClrArrayBindable : ArrayBindable
    {

        static readonly java.lang.Class ElementClass =
            ikvm.runtime.Util.getClassFromTypeHandle(typeof(object[]).TypeHandle);

        readonly Func<org.apache.calcite.DataContext, Enumerable> _bind;

        internal ClrArrayBindable(Func<org.apache.calcite.DataContext, Enumerable> bind)
        {
            ArgumentNullException.ThrowIfNull(bind);
            _bind = bind;
        }

        /// <summary>Implements <c>ArrayBindable.getElementType()</c>.</summary>
        public java.lang.Class getElementType() => ElementClass;

        /// <summary>Implements <c>Typed.getElementType()</c> (bridge).</summary>
        java.lang.reflect.Type Typed.getElementType() => ElementClass;

        /// <inheritdoc/>
        public Enumerable bind(org.apache.calcite.DataContext dataContext) => _bind(dataContext);

    }

}
