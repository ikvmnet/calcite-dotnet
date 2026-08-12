using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql2rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Extensions.Prepare.AsyncEnumerable
{

    /// <summary>
    /// Prepares a statement into the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    sealed class ClrAsyncEnumerablePreparingStmt : ClrPrepareImpl.PreparingStmt
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrAsyncEnumerablePreparingStmt(
            ClrPrepareImpl prepare,
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            CalciteSchema schema,
            ClrEnumerablePrefer prefer,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable) :
            base(prepare, context, catalogReader, typeFactory, schema, prefer, cluster, ClrAsyncEnumerableConvention.Instance, convertletTable)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>Programs.standard()</c>'s six passes, in its order. The last two are this convention's:
        /// <c>PlannerRules</c> is the planner pass written out, and <c>PlannerCalcRules</c> is
        /// <c>calc(metadataProvider)</c> with this convention's calc rules added to
        /// <c>RelOptRules.CALC_RULES</c>.
        /// </remarks>
        protected override Program GetStandardProgram()
        {
            return Programs.sequence(
                ClrAsyncEnumerablePrograms.SubQuery(),
                Programs.decorrelate(),
                Programs.measure(DefaultRelMetadataProvider.INSTANCE),
                Programs.trim(),
                ClrAsyncEnumerablePrograms.PlannerRules(),
                ClrAsyncEnumerablePrograms.PlannerCalcRules());
        }

        /// <inheritdoc />
        protected override ClrPrepare.IPreparedResult Implement(RelRoot root)
        {
            org.apache.calcite.runtime.Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);

            var resultType = root.rel.getRowType();
            var isDml = root.kind.belongsTo(SqlKind.DML);

            var node = (ClrAsyncEnumerableRel)root.rel;

            if (root.isRefTrivial() == false)
            {
                var rexBuilder = node.getCluster().getRexBuilder();
                var projects = new java.util.ArrayList();
                for (var i = org.apache.calcite.util.Pair.left(root.fields).iterator(); i.hasNext();)
                    projects.add(rexBuilder.makeInputRef(node, ((java.lang.Integer)i.next()).intValue()));

                var program = RexProgram.create(node.getRowType(), projects, null, root.validatedRowType, rexBuilder);
                node = Apache.Calcite.Extensions.Adapter.AsyncEnumerable.ClrAsyncEnumerableCalc.Create(node, program);
            }

            IClrAsyncBindable bindable;
            try
            {
                org.apache.calcite.prepare.Prepare.CatalogReader.THREAD_LOCAL.set(CatalogReader);
                InternalParameters.put("_conformance", Context.config().conformance());
                bindable = ClrAsyncEnumerableInterpretable.ToBindable(InternalParameters, node, Prefer);
            }
            finally
            {
                org.apache.calcite.prepare.Prepare.CatalogReader.THREAD_LOCAL.remove();
            }

            var collations = root.collation.getFieldCollations().isEmpty()
                ? (java.util.List)com.google.common.collect.ImmutableList.of()
                : com.google.common.collect.ImmutableList.of(root.collation);

            return new ClrAsyncEnumerablePrepareResult(
                resultType,
                ParameterRowType,
                FieldOrigins,
                collations,
                node,
                MapTableModOp(isDml, root.kind),
                isDml,
                bindable,
                // the type factory's answer, which is what the cursor factory is deduced from
                Apache.Calcite.Extensions.Adapter.Enumerable.ClrPhysTypeImpl.Of(
                    (org.apache.calcite.adapter.java.JavaTypeFactory)node.getCluster().getTypeFactory(),
                    node.getRowType(),
                    Prefer.PreferArray()).RowType);
        }

    }

}
