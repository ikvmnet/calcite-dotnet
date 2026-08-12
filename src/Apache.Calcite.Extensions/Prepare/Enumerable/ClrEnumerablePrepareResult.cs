using System;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

using Apache.Calcite.Extensions.Linq4j.Tree;

namespace Apache.Calcite.Extensions.Prepare.Enumerable
{

    /// <summary>
    /// A statement of the <see cref="ClrEnumerableConvention"/> calling convention, prepared and compiled.
    /// </summary>
    sealed class ClrEnumerablePrepareResult : ClrPrepare.PreparedResultImpl
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrEnumerablePrepareResult(
            RelDataType rowType,
            RelDataType parameterRowType,
            java.util.List fieldOrigins,
            java.util.List collations,
            RelNode rootRel,
            TableModify.Operation? tableModOp,
            bool isDml,
            IClrBindable bindable,
            System.Type elementType) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            Bindable = bindable ?? throw new ArgumentNullException(nameof(bindable));
            this.elementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
        }

        /// <inheritdoc />
        public override string Code => throw new java.lang.UnsupportedOperationException();

        /// <inheritdoc />
        public override Apache.Calcite.Extensions.Runtime.IClrBindableBase GetBindable(org.apache.calcite.avatica.Meta.CursorFactory cursorFactory)
        {
            return Bindable;
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        public IClrBindable Bindable { get; }

        /// <inheritdoc />
        public override System.Type ElementType => elementType;

        readonly System.Type elementType;

    }

}
