using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.runtime;
using org.apache.calcite.sql;
using org.apache.calcite.tools;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// A <see cref="CalcitePrepare"/> that plans a query into the <see cref="ClrEnumerableConvention"/>
    /// calling convention and compiles it as an expression tree.
    /// </summary>
    /// <remarks>
    /// Hand this to <c>CalciteConnection.PrepareFactory</c> and every statement on that connection runs
    /// through this convention rather than through Janino.
    ///
    /// <para>Three things differ from <c>CalcitePrepareImpl</c> and nothing else does: the convention a plan
    /// is asked to end in, the program that gets it there — <see cref="ClrEnumerablePrograms"/> — and the
    /// compiler at the end, which is <see cref="ClrEnumerableInterpretable"/>. Parsing, validation and
    /// sql-to-rel are Calcite's, untouched.</para>
    /// </remarks>
    public class ClrEnumerablePrepare : CalcitePrepareImpl
    {

        /// <inheritdoc />
        protected override CalcitePrepareImpl.CalcitePreparingStmt getPreparingStmt(CalcitePrepare.Context context, java.lang.reflect.Type elementType, CalciteCatalogReader catalogReader, RelOptPlanner planner)
        {
            var typeFactory = context.getTypeFactory();
            var prefer = elementType == (java.lang.reflect.Type)(java.lang.Class)typeof(object[])
                ? ClrEnumerablePrefer.Array
                : ClrEnumerablePrefer.Custom;

            return new ClrEnumerablePreparingStmt(
                this,
                context,
                catalogReader,
                typeFactory,
                context.getRootSchema(),
                prefer,
                createCluster(planner, new RexBuilder(typeFactory)),
                createConvertletTable());
        }

        /// <summary>
        /// The statement that plans and compiles into this convention.
        /// </summary>
        sealed class ClrEnumerablePreparingStmt : CalcitePrepareImpl.CalcitePreparingStmt
        {

            readonly CalcitePrepare.Context context;
            readonly ClrEnumerablePrefer prefer;

            /// <summary>
            /// The values passed to the executor rather than written into the plan.
            /// </summary>
            /// <remarks>
            /// Ours, because <c>CalcitePreparingStmt.internalParameters</c> is private and has no accessor.
            /// It carries the conformance, which is all this convention reads from it — see 5.6 in
            /// <c>PARITY.md</c> for the one thing that costs.
            /// </remarks>
            readonly java.util.Map parameters = new java.util.LinkedHashMap();

            public ClrEnumerablePreparingStmt(
                CalcitePrepareImpl prepare,
                CalcitePrepare.Context context,
                Prepare.CatalogReader catalogReader,
                RelDataTypeFactory typeFactory,
                CalciteSchema schema,
                ClrEnumerablePrefer prefer,
                RelOptCluster cluster,
                org.apache.calcite.sql2rel.SqlRexConvertletTable convertletTable) :
                base(prepare, context, catalogReader, typeFactory, schema, prefer.ToCalcite(), cluster, ClrEnumerableConvention.Instance, convertletTable)
            {
                this.context = context;
                this.prefer = prefer;
            }

            /// <inheritdoc />
            /// <remarks>
            /// The three passes, where Calcite's is <c>Programs.standard()</c>.
            /// </remarks>
            protected override Program getProgram()
            {
                var programs = ClrEnumerablePrograms.Standard();

                return Programs.sequence(
                    (Program)programs.get(0),
                    (Program)programs.get(1),
                    (Program)programs.get(2));
            }

            /// <inheritdoc />
            protected override RelTraitSet getDesiredRootTraitSet(RelRoot root)
            {
                return ClrEnumerablePrograms.DesiredRootTraitSet(root.rel.getTraitSet());
            }

            /// <inheritdoc />
            /// <remarks>
            /// <c>CalcitePreparingStmt.implement</c> with the convention swapped: a root whose fields are not
            /// the plan's own gets a calc over it, as Calcite's does, and the plan is compiled by
            /// <see cref="ClrEnumerableInterpretable"/> rather than handed to Janino.
            /// </remarks>
            protected override Prepare.PreparedResult implement(RelRoot root)
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
                    node = Rel.ClrEnumerableCalc.Create(node, program);
                }

                parameters.put("_conformance", context.config().conformance());

                var bindable = ClrEnumerableInterpretable.ToBindable(parameters, null, node, prefer);

                var collations = root.collation.getFieldCollations().isEmpty()
                    ? (java.util.List)com.google.common.collect.ImmutableList.of()
                    : com.google.common.collect.ImmutableList.of(root.collation);

                return new ClrEnumerablePreparedResult(
                    resultType,
                    parameterRowType,
                    fieldOrigins,
                    collations,
                    node,
                    mapTableModOp(isDml, root.kind),
                    isDml,
                    bindable);
            }

        }

        /// <summary>
        /// The compiled plan, as the prepare framework wants it.
        /// </summary>
        sealed class ClrEnumerablePreparedResult : Prepare.PreparedResultImpl
        {

            readonly Bindable bindable;

            public ClrEnumerablePreparedResult(
                RelDataType rowType,
                RelDataType parameterRowType,
                java.util.List fieldOrigins,
                java.util.List collations,
                RelNode rootRel,
                org.apache.calcite.rel.core.TableModify.Operation tableModOp,
                bool isDml,
                Bindable bindable) :
                base(rowType, parameterRowType, fieldOrigins, collations, rootRel, tableModOp, isDml)
            {
                this.bindable = bindable;
            }

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
            public override Bindable getBindable(org.apache.calcite.avatica.Meta.CursorFactory cursorFactory)
            {
                return bindable;
            }

            /// <inheritdoc />
            public override java.lang.reflect.Type getElementType()
            {
                return ((Typed)bindable).getElementType();
            }

        }

    }

}
