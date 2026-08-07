using System;

using Apache.Calcite.Linq;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Prepare
{

    /// <summary>
    /// A statement of the <see cref="ClrEnumerableConvention"/> calling convention, prepared and compiled.
    /// </summary>
    /// <remarks>
    /// What <c>CalcitePreparingStmt.implement</c> returns from its anonymous <c>PreparedResultImpl</c>,
    /// carrying an <see cref="IClrBindable"/> where that carries a <c>Bindable</c>.
    /// </remarks>
    sealed class ClrEnumerablePrepareResult : ClrPrepareResult
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
            IClrBindable bindable) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            Bindable = bindable ?? throw new ArgumentNullException(nameof(bindable));
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        public IClrBindable Bindable { get; }

        /// <inheritdoc />
        public override java.lang.reflect.Type? ElementType => Bindable.ElementType;

    }

}
