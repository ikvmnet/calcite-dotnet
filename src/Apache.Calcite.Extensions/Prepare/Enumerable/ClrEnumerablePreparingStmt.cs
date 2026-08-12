using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql2rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Extensions.Prepare.Enumerable
{

    /// <summary>
    /// Prepares a statement into the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    sealed class ClrEnumerablePreparingStmt : ClrPrepareImpl.PreparingStmt
    {

        readonly ClrEnumerablePrefer prefer;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrEnumerablePreparingStmt(
            ClrPrepareImpl prepare,
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            CalciteSchema schema,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            ClrEnumerablePrefer prefer) :
            base(prepare, context, catalogReader, schema, cluster, ClrEnumerableConvention.Instance, convertletTable)
        {
            this.prefer = prefer;
        }

        /// <inheritdoc />
        protected override Program GetProgram()
        {
            return Programs.sequence(
                ClrEnumerablePrograms.SubQuery(),
                ClrEnumerablePrograms.PlannerRules(),
                ClrEnumerablePrograms.PlannerCalcRules());
        }

        /// <inheritdoc />
        protected override ClrPrepare.IPreparedResult Implement(RelRoot root)
        {
            var resultType = root.rel.getRowType();
            var isDml = root.kind.belongsTo(SqlKind.DML);

            var node = (ClrEnumerableRel)root.rel;

            if (root.isRefTrivial() == false)
            {
                var rexBuilder = node.getCluster().getRexBuilder();
                var projects = new java.util.ArrayList();
                for (var i = org.apache.calcite.util.Pair.left(root.fields).iterator(); i.hasNext();)
                    projects.add(rexBuilder.makeInputRef(node, ((java.lang.Integer)i.next()).intValue()));

                var program = RexProgram.create(node.getRowType(), projects, null, root.validatedRowType, rexBuilder);
                node = Apache.Calcite.Extensions.Adapter.Enumerable.ClrEnumerableCalc.Create(node, program);
            }

            InternalParameters.put("_conformance", Context.config().conformance());

            var bindable = ClrEnumerableInterpretable.ToBindable(InternalParameters, node, prefer);

            var collations = root.collation.getFieldCollations().isEmpty()
                ? (java.util.List)com.google.common.collect.ImmutableList.of()
                : com.google.common.collect.ImmutableList.of(root.collation);

            return new ClrEnumerablePrepareResult(
                resultType,
                ParameterRowType,
                FieldOrigins,
                collations,
                node,
                MapTableModOp(isDml, root.kind),
                isDml,
                bindable,
                // the type factory's answer, which is what the cursor factory is deduced from
                ClrPhysTypeImpl.Of(
                    (org.apache.calcite.adapter.java.JavaTypeFactory)node.getCluster().getTypeFactory(),
                    node.getRowType(),
                    prefer.PreferArray()).RowType);
        }

    }

}
