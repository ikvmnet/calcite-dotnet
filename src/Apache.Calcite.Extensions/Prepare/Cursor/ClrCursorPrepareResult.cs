using System;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Extensions.Prepare.Cursor
{

    /// <summary>
    /// A statement of the <see cref="ClrCursorConvention"/> calling convention, prepared and
    /// implemented.
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
        /// Gets the plan, which opens its cursor either way.
        /// </summary>
        public ClrCursorFactory Factory { get; }

        /// <inheritdoc />
        /// <remarks>
        /// The factory's answer, which is the type factory's for the physical row and the same whichever way
        /// the plan is opened.
        /// </remarks>
        public override Type ElementType => Factory.ElementType;

    }

}
