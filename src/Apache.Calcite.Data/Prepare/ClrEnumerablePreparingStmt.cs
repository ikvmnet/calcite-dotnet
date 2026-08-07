using Apache.Calcite.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql2rel;
using org.apache.calcite.tools;

namespace Apache.Calcite.Data.Prepare
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
            base(context, catalogReader, schema, cluster, convertletTable)
        {
            this.prefer = prefer;
        }

        /// <inheritdoc />
        protected override Convention ResultConvention => ClrEnumerableConvention.Instance;

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
                node = Linq.Rel.ClrEnumerableCalc.Create(node, program);
            }

            InternalParameters.put("_conformance", Context.config().conformance());

            var bindable = ClrEnumerableInterpretable.ToBindable(InternalParameters, null, node, prefer);

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
                bindable);
        }

        /// <summary>
        /// Returns which modification a DML statement performs.
        /// </summary>
        /// <param name="isDml"></param>
        /// <param name="sqlKind"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Prepare.mapTableModOp</c>.
        /// </remarks>
        static org.apache.calcite.rel.core.TableModify.Operation? MapTableModOp(bool isDml, SqlKind sqlKind)
        {
            if (isDml == false)
                return null;

            return sqlKind.name() switch
            {
                nameof(SqlKind.INSERT) => org.apache.calcite.rel.core.TableModify.Operation.INSERT,
                nameof(SqlKind.DELETE) => org.apache.calcite.rel.core.TableModify.Operation.DELETE,
                nameof(SqlKind.MERGE) => org.apache.calcite.rel.core.TableModify.Operation.MERGE,
                nameof(SqlKind.UPDATE) => org.apache.calcite.rel.core.TableModify.Operation.UPDATE,
                _ => null,
            };
        }

    }

}
