using System;

using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

using Apache.Calcite.Extensions.Linq4j.Tree;

namespace Apache.Calcite.Extensions.Prepare.AsyncEnumerable
{

    /// <summary>
    /// A statement of the <see cref="ClrAsyncEnumerableConvention"/> calling convention, prepared and compiled.
    /// </summary>
    /// <remarks>
    /// What <c>CalcitePreparingStmt.implement</c> returns from its anonymous <c>PreparedResultImpl</c>,
    /// carrying an <see cref="IClrAsyncBindable"/> where that carries a <c>Bindable</c>.
    /// </remarks>
    sealed class ClrAsyncEnumerablePrepareResult : ClrPrepareResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrAsyncEnumerablePrepareResult(
            RelDataType rowType,
            RelDataType parameterRowType,
            java.util.List fieldOrigins,
            java.util.List collations,
            RelNode rootRel,
            TableModify.Operation? tableModOp,
            bool isDml,
            IClrAsyncBindable bindable) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            Bindable = bindable ?? throw new ArgumentNullException(nameof(bindable));
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        public IClrAsyncBindable Bindable { get; }

        /// <inheritdoc />
        public override java.lang.reflect.Type? ElementType => Bindable.ElementType;

    }

}
