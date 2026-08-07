using System;

using Apache.Calcite.Linq;

using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The result of preparing a statement of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <c>Prepare.implement</c> is declared to return a <c>PreparedResult</c>, and this convention's
    /// preparing statement derives from Calcite's, so this exists whether or not anything asks Calcite for
    /// it. It carries an <see cref="IClrBindable"/> where Calcite's carries a <c>Bindable</c>.
    /// </remarks>
    sealed class ClrEnumerablePreparedResult : Prepare.PreparedResultImpl
    {

        readonly IClrBindable bindable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrEnumerablePreparedResult(
            RelDataType rowType,
            RelDataType parameterRowType,
            java.util.List fieldOrigins,
            java.util.List collations,
            RelNode rootRel,
            org.apache.calcite.rel.core.TableModify.Operation tableModOp,
            bool isDml,
            IClrBindable bindable) :
            base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
        {
            this.bindable = bindable ?? throw new ArgumentNullException(nameof(bindable));
        }

        /// <summary>
        /// Gets the compiled plan.
        /// </summary>
        /// <remarks>
        /// How the driver gets the plan back, since <see cref="getBindable"/> cannot hand it over.
        /// </remarks>
        public IClrBindable Bindable => bindable;

        /// <summary>
        /// Gets the collations the result is known to carry.
        /// </summary>
        /// <remarks>
        /// <c>PreparedResultImpl.collations</c> is protected, and Calcite's driver reads it by testing for
        /// <c>PreparedResultImpl</c> and casting — which reaches it only because the driver is in the same
        /// package. This is the reachable route.
        /// </remarks>
        public java.util.List Collations => collations;

        /// <inheritdoc />
        /// <remarks>
        /// There is no source text: the plan is an expression tree, which is the whole point of this
        /// convention. Calcite's own throws here too, for the opposite reason — its code is generated but
        /// not kept.
        /// </remarks>
        public override string getCode()
        {
            throw new java.lang.UnsupportedOperationException();
        }

        /// <inheritdoc />
        /// <remarks>
        /// Refused rather than adapted. A <c>Bindable</c> hands back a linq4j <c>Enumerable</c>, and
        /// producing one would mean wrapping the compiled delegate only for the caller to unwrap it a row
        /// at a time — the round trip this pipeline exists to remove. Nothing in this project asks:
        /// <see cref="Bindable"/> is the way through, and <c>ClrPrepare</c> is the only caller.
        /// </remarks>
        public override Bindable getBindable(org.apache.calcite.avatica.Meta.CursorFactory cursorFactory)
        {
            throw new java.lang.UnsupportedOperationException(
                $"A plan of {nameof(ClrEnumerableConvention)} is a compiled delegate, not a Bindable. Read {nameof(Bindable)}.");
        }

        /// <inheritdoc />
        public override java.lang.reflect.Type getElementType()
        {
            return bindable.ElementType;
        }

    }

}
