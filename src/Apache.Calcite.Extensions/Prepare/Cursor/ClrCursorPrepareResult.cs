using System;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare.Cursor
{

    /// <summary>
    /// A statement prepared and implemented in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    sealed class ClrCursorPrepareResult : ClrPrepare.PreparedResultImpl
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrCursorPrepareResult(
            RelDataType rowType,
            RelDataType parameterRowType,
            java.util.List fieldOrigins,
            java.util.List collations,
            RelNode rootRel,
            TableModify.Operation? tableModOp,
            bool isDml,
            ClrCursorFactory factory) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            Factory = factory ?? throw new ArgumentNullException(nameof(factory));
        }

        /// <inheritdoc />
        public override string Code => throw new java.lang.UnsupportedOperationException();

        /// <inheritdoc />
        public override IClrCursorFactory GetBindable(org.apache.calcite.avatica.Meta.CursorFactory cursorFactory)
        {
            return Factory;
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        public ClrCursorFactory Factory { get; }

        /// <inheritdoc />
        /// <remarks>
        /// The factory's element type.
        /// </remarks>
        public override Type ElementType => Factory.ElementType;

    }

}
