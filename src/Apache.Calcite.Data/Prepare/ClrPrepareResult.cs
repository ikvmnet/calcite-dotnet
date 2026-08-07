using System;

using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Prepare
{

    /// <summary>
    /// What preparing a statement produces, before it is described for a caller.
    /// </summary>
    /// <remarks>
    /// Our counterpart of <c>Prepare.PreparedResult</c>, less the member that interface exists for:
    /// <c>getBindable</c>, which hands back a linq4j <c>Enumerable</c>. A plan of these conventions is a
    /// compiled delegate, so what a subclass carries differs by convention and is not expressible here —
    /// hence a base holding only what every statement has, and one subclass per kind of result.
    ///
    /// <para>Everything on it is what <c>Prepare.PreparedResultImpl</c> holds, with two differences.
    /// <c>collations</c> is readable rather than protected, because the driver needs it and Calcite's own
    /// reaches it only by being in the same package. And <see cref="RowType"/> is nullable, because an
    /// <c>EXPLAIN</c> of a type has none.</para>
    /// </remarks>
    abstract class ClrPrepareResult
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        protected ClrPrepareResult(
            RelDataType? rowType,
            RelDataType parameterRowType,
            java.util.List fieldOrigins,
            java.util.List collations,
            RelNode? rootRel,
            TableModify.Operation? tableModOp,
            bool isDml)
        {
            RowType = rowType;
            ParameterRowType = parameterRowType ?? throw new ArgumentNullException(nameof(parameterRowType));
            FieldOrigins = fieldOrigins ?? throw new ArgumentNullException(nameof(fieldOrigins));
            Collations = collations ?? throw new ArgumentNullException(nameof(collations));
            RootRel = rootRel;
            TableModOp = tableModOp;
            IsDml = isDml;
        }

        /// <summary>
        /// Gets the row type of the result, or <see langword="null"/> where there is none.
        /// </summary>
        public RelDataType? RowType { get; }

        /// <summary>
        /// Gets the row type of the statement's dynamic parameters.
        /// </summary>
        public RelDataType ParameterRowType { get; }

        /// <summary>
        /// Gets, per field, the fully qualified name it originates from, or <see langword="null"/>.
        /// </summary>
        public java.util.List FieldOrigins { get; }

        /// <summary>
        /// Gets the collations the result is known to carry.
        /// </summary>
        public java.util.List Collations { get; }

        /// <summary>
        /// Gets the root of the plan, or <see langword="null"/> where the statement produced none.
        /// </summary>
        public RelNode? RootRel { get; }

        /// <summary>
        /// Gets which modification a DML statement performs.
        /// </summary>
        public TableModify.Operation? TableModOp { get; }

        /// <summary>
        /// Gets whether the statement modifies data.
        /// </summary>
        public bool IsDml { get; }

        /// <summary>
        /// Gets the Java type of one row, which decides how a row is read back.
        /// </summary>
        /// <remarks>
        /// <c>Typed.getElementType</c>, which <c>PreparedResultImpl</c> declares abstract and Calcite's
        /// driver reads to choose a cursor factory.
        /// </remarks>
        public abstract java.lang.reflect.Type? ElementType { get; }

    }

}
