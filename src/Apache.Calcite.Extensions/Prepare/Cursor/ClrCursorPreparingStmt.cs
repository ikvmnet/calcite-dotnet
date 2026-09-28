using Apache.Calcite.Extensions.Adapter.Cursor;

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
    /// The counterpart of Calcite's <c>CalcitePreparingStmt</c>, with this convention in place of
    /// <c>EnumerableConvention</c> and expression trees in place of Janino. The root is implemented into a
    /// <see cref="ClrCursorFactory"/>, whose synchronous and asynchronous opens are each compiled the first
    /// time they are used.
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
            ClrCursorPrefer prefer,
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

                // a FetchOffsetRoundingPolicy on the planner's context is passed to the limit through the
                // internal parameters
                var roundingPolicy = node.getCluster().getPlanner().getContext().unwrap((java.lang.Class)typeof(org.apache.calcite.adapter.enumerable.FetchOffsetRoundingPolicy));
                if (roundingPolicy != null)
                    InternalParameters.put(ClrCursorRelImplementor.FetchOffsetRoundingPolicy, roundingPolicy);

                // builds both opens now; the factory compiles each only when it is first used
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
