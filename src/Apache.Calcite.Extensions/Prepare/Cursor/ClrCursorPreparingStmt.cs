using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql2rel;

namespace Apache.Calcite.Extensions.Prepare.Cursor
{

    /// <summary>
    /// Prepares a statement into the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Calcite's <c>CalcitePreparingStmt</c> with the convention and the compile swapped. The root is
    /// implemented once, through both of its bodies, into a <see cref="ClrCursorFactory"/>, which is
    /// the bindable the signature carries; each of the factory's two opens is compiled the first time a
    /// caller opens that way.
    /// </remarks>
    sealed class ClrCursorPreparingStmt : ClrPrepareImpl.PreparingStmt
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrCursorPreparingStmt(
            ClrPrepareImpl prepare,
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            RelDataTypeFactory typeFactory,
            CalciteSchema schema,
            ClrEnumerablePrefer prefer,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable) :
            base(prepare, context, catalogReader, typeFactory, schema, prefer, cluster, ClrCursorConvention.Instance, convertletTable)
        {

        }

        /// <inheritdoc />
        protected override ClrPrepare.IPreparedResult Implement(RelRoot root)
        {
            org.apache.calcite.runtime.Hook.PLAN_BEFORE_IMPLEMENTATION.run(root);

            var resultType = root.rel.getRowType();
            var isDml = root.kind.belongsTo(SqlKind.DML);

            var node = (ClrCursorRel)root.rel;

            if (root.isRefTrivial() == false)
            {
                var rexBuilder = node.getCluster().getRexBuilder();
                var projects = new java.util.ArrayList();
                for (var i = org.apache.calcite.util.Pair.left(root.fields).iterator(); i.hasNext();)
                    projects.add(rexBuilder.makeInputRef(node, ((java.lang.Integer)i.next()).intValue()));

                var program = RexProgram.create(node.getRowType(), projects, null, root.validatedRowType, rexBuilder);
                node = ClrCursorCalc.Create(node, program);
            }

            ClrCursorFactory factory;
            try
            {
                org.apache.calcite.prepare.Prepare.CatalogReader.THREAD_LOCAL.set(CatalogReader);
                InternalParameters.put("_conformance", Context.config().conformance());

                // a caller that wants a fractional FETCH or OFFSET rounded its own way puts the policy on the
                // planner's context, and the limit reads it back out of the data context
                var roundingPolicy = node.getCluster().getPlanner().getContext().unwrap((java.lang.Class)typeof(org.apache.calcite.adapter.enumerable.FetchOffsetRoundingPolicy));
                if (roundingPolicy != null)
                    InternalParameters.put(ClrCursorRelImplementor.FetchOffsetRoundingPolicy, roundingPolicy);

                // both bodies, now: translation was measured at a few milliseconds of a prepare, and the
                // factory compiles each open only when it is first asked for it
                var implementor = new ClrCursorRelImplementor(node.getCluster().getRexBuilder(), InternalParameters);
                factory = implementor.ImplementRoot(node, Prefer);
            }
            finally
            {
                org.apache.calcite.prepare.Prepare.CatalogReader.THREAD_LOCAL.remove();
            }

            var collations = root.collation.getFieldCollations().isEmpty()
                ? (java.util.List)com.google.common.collect.ImmutableList.of()
                : com.google.common.collect.ImmutableList.of(root.collation);

            return new ClrCursorPrepareResult(
                resultType,
                ParameterRowType,
                FieldOrigins,
                collations,
                node,
                MapTableModOp(isDml, root.kind),
                isDml,
                factory);
        }

    }

}
