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
            IClrBindable bindable,
            java.lang.reflect.Type javaElementType) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            Bindable = bindable ?? throw new ArgumentNullException(nameof(bindable));
            this.javaElementType = javaElementType ?? throw new ArgumentNullException(nameof(javaElementType));
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        public IClrBindable Bindable { get; }

        /// <inheritdoc />
        /// <remarks>
        /// The type factory's own answer, kept apart from the bindable's. A compiled plan states its rows in
        /// CLR types; <c>Meta.CursorFactory.deduce</c> wants the <c>java.lang.Class</c> the row type was
        /// built from, and converting one back would be answering a question with a translation of itself.
        /// </remarks>
        public override java.lang.reflect.Type? ElementType => javaElementType;

        readonly java.lang.reflect.Type javaElementType;

    }

}
