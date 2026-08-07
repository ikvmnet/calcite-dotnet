using System;
using System.Collections.Generic;

using Apache.Calcite.Linq;

using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.prepare;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.tools;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Plans a statement into the <see cref="ClrEnumerableConvention"/> calling convention and compiles it
    /// as an expression tree.
    /// </summary>
    /// <remarks>
    /// Derives from Calcite's preparing statement, which is where parse-to-rel, field trimming, view
    /// expansion, materialization registration and <c>optimize</c> live. Three things differ and nothing
    /// else does: the convention a plan is asked to end in, the program that gets it there, and the
    /// compiler at the end.
    /// </remarks>
    sealed class ClrEnumerablePreparingStmt : CalcitePrepareImpl.CalcitePreparingStmt
    {

        readonly ClrEnumerablePrefer prefer;

        /// <summary>
        /// The values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        /// <remarks>
        /// Its own, because <c>CalcitePreparingStmt.internalParameters</c> is private with no accessor and
        /// is written only inside the <c>implement</c> this class overrides — so Calcite's stays empty here
        /// however the statement is prepared. <see cref="InternalParameters"/> is how the driver reads the
        /// one that matters.
        /// </remarks>
        readonly java.util.Map parameters = new java.util.LinkedHashMap();

        ClrEnumerablePreparedResult? result;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
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

        /// <summary>
        /// Gets the values the query reads through the <c>DataContext</c> rather than from the plan.
        /// </summary>
        public java.util.Map InternalParameters => parameters;

        /// <summary>
        /// Gets the compiled plan, once <c>prepareSql</c> has run.
        /// </summary>
        public IClrBindable Bindable =>
            (result ?? throw new InvalidOperationException("The statement has not been prepared.")).Bindable;

        /// <summary>
        /// Gets the validator the statement is validated with.
        /// </summary>
        /// <remarks>
        /// A forwarder, because the driver needs a validator to pass to <c>prepareSql</c> and Calcite builds
        /// one inside <c>prepare2_</c> — the method this pipeline replaces. <c>getSqlValidator</c> rather
        /// than <c>createSqlValidator</c>: it is the same call with <c>UnaryOperator.identity()</c>, and it
        /// caches, so asking twice cannot yield two validators for one statement.
        /// </remarks>
        public SqlValidator Validator => getSqlValidator();

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
        /// <c>ClrEnumerableInterpretable</c> rather than handed to Janino.
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
                node = Apache.Calcite.Linq.Rel.ClrEnumerableCalc.Create(node, program);
            }

            parameters.put("_conformance", context.config().conformance());

            var bindable = ClrEnumerableInterpretable.ToBindable(parameters, null, node, prefer);

            var collations = root.collation.getFieldCollations().isEmpty()
                ? (java.util.List)com.google.common.collect.ImmutableList.of()
                : com.google.common.collect.ImmutableList.of(root.collation);

            return result = new ClrEnumerablePreparedResult(
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

}
