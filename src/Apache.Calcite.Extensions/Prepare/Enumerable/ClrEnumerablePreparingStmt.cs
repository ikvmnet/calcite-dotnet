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
    /// <remarks>
    /// The whole of what this convention adds to <see cref="ClrPreparingStmt"/>: the convention a plan must
    /// end in, the program that gets it there, the traits its root must satisfy, and the compiler. An
    /// asynchronous convention supplies the same four and shares everything else.
    /// </remarks>
    sealed class ClrEnumerablePreparingStmt : ClrPreparingStmt
    {

        readonly ClrEnumerablePrefer prefer;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrEnumerablePreparingStmt(
            CalcitePrepare.Context context,
            CalciteCatalogReader catalogReader,
            CalciteSchema schema,
            RelOptCluster cluster,
            SqlRexConvertletTable convertletTable,
            ClrEnumerablePrefer prefer) :
            base(context, catalogReader, schema, cluster, ClrEnumerableConvention.Instance, convertletTable)
        {
            this.prefer = prefer;
        }

        /// <inheritdoc />
        /// <remarks>
        /// The three passes, where Calcite's is <c>Programs.standard()</c>.
        /// </remarks>
        protected override Program GetProgram()
        {
            return Programs.sequence(
                ClrEnumerablePrograms.SubQuery(),
                ClrEnumerablePrograms.PlannerRules(),
                ClrEnumerablePrograms.PlannerCalcRules());
        }

        /// <inheritdoc />
        protected override RelTraitSet GetDesiredRootTraitSet(RelRoot root)
        {
            return ClrEnumerablePrograms.DesiredRootTraitSet(root.rel.getTraitSet());
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>CalcitePreparingStmt.implement</c> with the convention swapped: a root whose fields are not
        /// the plan's own gets a calc over it, as Calcite's does, and the plan is compiled by
        /// <see cref="ClrEnumerableInterpretable"/> rather than handed to Janino.
        /// </remarks>
        protected override ClrPrepareResult Implement(RelRoot root)
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
                    prefer.PreferArray()).JavaRowType);
        }

    }

}
