using Apache.Calcite.Linq.Runtime;

using org.apache.calcite;
using org.apache.calcite.jdbc;
using org.apache.calcite.linq4j;
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
    /// <code>
    /// using var c = new CalciteConnection(connectionString);
    /// c.PrepareFactory = () => new ClrEnumerablePrepare();
    /// c.Open();
    /// </code>
    ///
    /// <para>It must be set before the connection is opened. Three things differ from
    /// <c>CalcitePrepareImpl</c> and nothing else does: the convention a plan is asked to end in, the program
    /// that gets it there — <see cref="ClrEnumerablePrograms"/> — and the compiler at the end. Parsing,
    /// validation and sql-to-rel are Calcite's, untouched.</para>
    ///
    /// <para>Calcite's own rules stay on the planner, so a statement this convention has no node for is
    /// still planned and run — implemented in <c>EnumerableConvention</c>, with a converter carrying its
    /// rows. That is how a table modification works here.</para>
    /// </remarks>
    public class ClrEnumerablePrepare : CalcitePrepareImpl
    {

        /// <inheritdoc />
        /// <remarks>
        /// Calcite's planner with this convention's rules added. Calcite's own stay on it, so a node this
        /// convention has no rule for — a table modification, a MATCH_RECOGNIZE — is still implemented, in
        /// <c>EnumerableConvention</c>, and a converter carries its rows.
        /// </remarks>
        protected override RelOptPlanner createPlanner(CalcitePrepare.Context prepareContext, org.apache.calcite.plan.Context externalContext, RelOptCostFactory costFactory)
        {
            var planner = base.createPlanner(prepareContext, externalContext, costFactory);

            foreach (var rule in ClrEnumerableRules.Rules())
                planner.addRule(rule);

            return planner;
        }

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

            readonly ClrEnumerablePrefer prefer;

            /// <summary>
            /// The values the query reads through the <c>DataContext</c> rather than from the plan.
            /// </summary>
            /// <remarks>
            /// Its own, because <c>CalcitePreparingStmt.internalParameters</c> is private with no accessor. It
            /// carries the conformance, which is what this convention reads from it.
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
                this.prefer = prefer;
            }

            /// <inheritdoc />
            /// <remarks>
            /// The three passes, where Calcite's is <c>Programs.standard()</c>.
            /// </remarks>
            protected override Program getProgram()
            {
                return Programs.sequence(
                    ClrEnumerablePrograms.SubQuery(),
                    ClrEnumerablePrograms.PlannerRules(),
                    ClrEnumerablePrograms.PlannerCalcRules());
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

            readonly IClrBindable bindable;

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
            /// <remarks>
            /// Temporary. <c>Prepare.PreparedResult</c> is declared to return Calcite's <c>Bindable</c>, so
            /// the compiled delegate is wrapped back into a linq4j sequence here and unwrapped a row at a
            /// time by whoever reads it. That round trip is what the prepare pipeline in
            /// <c>Apache.Calcite.Data</c> removes; until it lands, this keeps the ADO.NET surface running
            /// against the new bindable without changing what it consumes.
            /// </remarks>
            public override Bindable getBindable(org.apache.calcite.avatica.Meta.CursorFactory cursorFactory)
            {
                return new JavaBindable(bindable);
            }

            /// <inheritdoc />
            public override java.lang.reflect.Type getElementType()
            {
                return bindable.ElementType;
            }

        }

        /// <summary>
        /// Presents an <see cref="IClrBindable"/> as the <see cref="Bindable"/> Calcite's prepare framework
        /// is declared against.
        /// </summary>
        /// <param name="bindable"></param>
        /// <remarks>
        /// Temporary, and the only thing left holding <c>JavaSequences.ToJava</c> on this path. See
        /// <see cref="ClrEnumerablePreparedResult.getBindable"/>.
        /// </remarks>
        sealed class JavaBindable(IClrBindable bindable) : Bindable, Typed
        {

            /// <inheritdoc />
            public Enumerable bind(DataContext dataContext)
            {
                return JavaSequences.ToJava(bindable.Bind(dataContext));
            }

            /// <inheritdoc />
            public java.lang.reflect.Type getElementType()
            {
                return bindable.ElementType;
            }

        }

    }

}
