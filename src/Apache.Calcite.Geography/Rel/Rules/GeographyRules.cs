using System;
using System.Collections.Generic;

using Apache.Calcite.Geography.Sql;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.rules;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.fun;
using org.apache.calcite.tools;

namespace Apache.Calcite.Geography.Rel.Rules
{

    /// <summary>
    /// Rules that simplify <c>CLR_ST_GEOG_*</c> expressions.
    /// </summary>
    /// <remarks>
    /// <para>A host sequences the pass in front of whatever program it runs:</para>
    ///
    /// <code>
    /// Programs.sequence(GeographyRules.Program(), Programs.standard())
    /// </code>
    ///
    /// <para><b>A pass and not rules on the planner, and the difference is not a preference.</b> Measured:
    /// with these registered on a <c>VolcanoPlanner</c>, one of the five rewrites takes effect and four do
    /// not. <c>VolcanoCost.isLt</c> compares the row count and nothing else — cpu and io are dead code behind
    /// <c>if (true)</c> — so a filter whose condition was simplified is never <em>cheaper</em> than the same
    /// filter unsimplified, and the planner keeps whichever it registered first, which is the original. The
    /// one that does take effect,
    /// <c>CLR_ST_GEOG_DISTANCE(…) &lt;= d</c> becoming <c>CLR_ST_GEOG_DWITHIN</c>, wins for a reason that has
    /// nothing to do with being better: <c>RelMdUtil.guessSelectivity</c> guesses 0.5 for a comparison and
    /// 0.25 for any other call, so the rewritten filter carries a smaller row count. This is the same
    /// argument that keeps <c>Programs.calc</c> a hep pass rather than a set of planner rules.</para>
    ///
    /// <para><b>Every rewrite here is an equality of values</b>, not of truth under a filter, so each is
    /// valid wherever an expression can stand — a projection, a filter, a join condition. That is why there
    /// is one shuttle and three rules that run it, rather than a context for each.</para>
    ///
    /// <para><b>What these are not.</b> Constant folding is Calcite's and already works:
    /// <c>CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')</c> in a predicate reduces to a <c>GEOMETRY</c> literal, and
    /// a wholly constant predicate reduces to nothing at all, under <c>CoreRules.FILTER_REDUCE_EXPRESSIONS</c>
    /// — which <c>RelOptUtil.registerDefaultRules</c> already registers. It needs an <b>executor</b>, and
    /// that is the whole of what a caller has to do: <c>CalcitePrepareImpl</c> sets one on every
    /// <c>jdbc:calcite:</c> connection, and <c>Frameworks</c> sets whatever the config names, which is
    /// nothing by default. Measured either way — without one the WKT is parsed once per row.</para>
    ///
    /// <code>
    /// Frameworks.newConfigBuilder().executor(RexUtil.EXECUTOR)
    /// </code>
    ///
    /// <para><b>And nor are the facts on the operators.</b> Strictness and symmetry are declared on
    /// <c>GeographyFunction</c> and read by <c>RelOptUtil.simplifyJoin</c>, <c>RexSimplify</c> and
    /// <c>RexNormalize</c> with no rule involved. They reach a plan only where this package's operator object
    /// does, which is why the shuttle puts it back first; see <see cref="GeographyOperatorTable.Rebind"/>.
    /// </para>
    /// </remarks>
    public static class GeographyRules
    {

        /// <summary>
        /// Simplifies the condition of a <see cref="Filter"/>.
        /// </summary>
        public static readonly RelOptRule Filter =
            new GeographyRule(Config("GeographyFilterRule", (java.lang.Class)typeof(Filter)));

        /// <summary>
        /// Simplifies the expressions of a <see cref="Project"/>.
        /// </summary>
        public static readonly RelOptRule Project =
            new GeographyRule(Config("GeographyProjectRule", (java.lang.Class)typeof(Project)));

        /// <summary>
        /// Simplifies the condition of a <see cref="Join"/>.
        /// </summary>
        public static readonly RelOptRule Join =
            new GeographyRule(Config("GeographyJoinRule", (java.lang.Class)typeof(Join)));

        /// <summary>
        /// Returns every rule in this set.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// For a host that already runs a hep pass of its own and would rather add these to it.
        /// <see cref="Program"/> is the one to reach for otherwise, and putting these on a
        /// <c>VolcanoPlanner</c> is what does not work; see the remarks on this class.
        /// </remarks>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return [Filter, Project, Join];
        }

        /// <summary>
        /// Returns these rules as a pass a host sequences in front of its own program.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>noDag</c> is true, matching <c>Programs.calc</c>: the rewrites are idempotent — each maps a
        /// spelling to a fixed canonical one or removes a node — so there is nothing for a second round to
        /// find, and a graph that keeps every intermediate would only cost memory.
        /// </remarks>
        public static Program Program()
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in Rules())
                rules.add(rule);

            return Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Returns the given expression with every <c>CLR_ST_GEOG_</c> simplification applied.
        /// </summary>
        /// <param name="rexBuilder"></param>
        /// <param name="node"></param>
        /// <returns>The simplified expression, or the one given where nothing applied.</returns>
        /// <remarks>
        /// Public because the rules are not the only way to want this: an adapter walking a plan to render it
        /// wants the canonical form of a call before it starts matching names, and a host that runs its own
        /// shuttle over a condition can compose this into it.
        /// </remarks>
        public static RexNode Simplify(RexBuilder rexBuilder, RexNode node)
        {
            ArgumentNullException.ThrowIfNull(rexBuilder);
            ArgumentNullException.ThrowIfNull(node);

            return (RexNode)node.accept(new Shuttle(rexBuilder));
        }

        /// <summary>
        /// Returns a configuration matching the given node class.
        /// </summary>
        /// <param name="description"></param>
        /// <param name="relClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>FilterToCalcRule</c>'s configuration is borrowed and re-pointed because there is no neutral one
        /// to start from: <c>RelRule.Config</c> is an interface with no instance of its own, and every
        /// concrete configuration is generated by immutables from some rule's own sub-interface. Only
        /// <c>operandSupplier</c>, <c>description</c> and <c>relBuilderFactory</c> are ever read from it, and
        /// <c>toRule</c> — the one member that would make the borrowing show — is never called, the rule
        /// being constructed directly.
        /// </remarks>
        static RelRule.Config Config(string description, java.lang.Class relClass)
        {
            return ((RelRule.Config)FilterToCalcRule.Config.DEFAULT)
                .withOperandSupplier(new OperandTransform(b => b.operand(relClass).anyInputs()))
                .withDescription(description);
        }

        /// <summary>
        /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
        /// </summary>
        /// <param name="transform"></param>
        sealed class OperandTransform(Func<RelRule.OperandBuilder, RelRule.Done> transform) : RelRule.OperandTransform
        {

            /// <inheritdoc />
            public object apply(object builder)
            {
                return transform((RelRule.OperandBuilder)builder);
            }

            /// <inheritdoc />
            /// <remarks>
            /// C# does not inherit the defaults of an interface IKVM compiled, so composition is forwarded
            /// rather than left to <see cref="java.util.function.Function"/>.
            /// </remarks>
            public java.util.function.Function andThen(java.util.function.Function after)
            {
                return java.util.function.Function.__DefaultMethods.andThen(this, after);
            }

            /// <inheritdoc cref="andThen" />
            public java.util.function.Function compose(java.util.function.Function before)
            {
                return java.util.function.Function.__DefaultMethods.compose(this, before);
            }

        }

        /// <summary>
        /// Runs the shuttle over whatever expressions the matched node holds.
        /// </summary>
        /// <param name="config"></param>
        /// <remarks>
        /// One rule class for all three node kinds, because <c>RelNode.accept(RexShuttle)</c> is what rewrites
        /// the expressions a node holds and every node answers it — a filter's condition, a project's list, a
        /// join's condition — so there is nothing per kind to write. The node class is the operand and the
        /// rest is the same.
        /// </remarks>
        sealed class GeographyRule(RelRule.Config config) : RelRule(config)
        {

            /// <inheritdoc />
            public override void onMatch(RelOptRuleCall call)
            {
                var rel = call.rel(0);
                var shuttle = new Shuttle(rel.getCluster().getRexBuilder());
                var rewritten = rel.accept(shuttle);

                // accept answers the same node where nothing changed, and transforming to an equal node
                // would have the planner match this rule against it again for ever
                if (shuttle.Changed == false || ReferenceEquals(rewritten, rel))
                    return;

                call.transformTo(rewritten);
            }

        }

        /// <summary>
        /// The simplifications themselves.
        /// </summary>
        /// <param name="rexBuilder"></param>
        sealed class Shuttle(RexBuilder rexBuilder) : RexShuttle
        {

            /// <summary>
            /// Whether anything was rewritten.
            /// </summary>
            public bool Changed { get; private set; }

            /// <inheritdoc />
            public override RexNode visitCall(RexCall call)
            {
                // the operands first, so that a rewrite here sees what its children became
                var visited = (RexCall)base.visitCall(call);

                var simplified =
                    Crossing(visited) ??
                    Alias(visited) ??
                    Transpose(visited) ??
                    Negation(visited) ??
                    Distance(visited) ??
                    Rebind(visited);

                if (simplified is null)
                    return visited;

                Changed = true;
                return simplified;
            }

            /// <summary>
            /// Puts this package's declaration of an operator back in a call that resolved through a schema.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// The call is unchanged but for the operator object, which is what carries the strictness and
            /// symmetry Calcite's own simplifications read. The last thing tried, so that a call a rewrite
            /// above already rebuilt is not rebuilt twice.
            /// </remarks>
            RexNode? Rebind(RexCall call)
            {
                var mine = GeographyOperatorTable.Rebind(call.getOperator());
                if (mine is null || ReferenceEquals(mine, call.getOperator()))
                    return null;

                return Call(call.getType(), mine, call.getOperands());
            }

            /// <summary>
            /// Drops a crossing that has nothing to convert.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <para><c>CLR_ST_GEOG_ASGEOM</c> and <c>CLR_ST_GEOM_ASGEOG</c> are both the identity —
            /// <c>GeographyFunctions.AsGeometry</c> is <c>return geography;</c> and its twin the same — so a
            /// call to either is worth a dispatch per row and nothing else, wherever the operand already has
            /// the type the call answers. Which subsumes the round trip
            /// <c>CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(x))</c>, the repeated crossing, and the lone one over
            /// a column that is already a geometry.</para>
            ///
            /// <para><b>The type is the whole of the guard.</b> There is nothing else to preserve: a
            /// geography and a geometry are one type and the operator's name is the only marking, so the
            /// question "is anything lost" is the question "does the type change". Where it does — an operand
            /// Calcite typed <c>GEOMETRY</c> under a call typed <c>JavaType(Geometry)</c> — the call stays,
            /// because dropping it would change what the enclosing expression is handed.</para>
            /// </remarks>
            static RexNode? Crossing(RexCall call)
            {
                if (GeographyOperatorTable.Matches(call.getOperator(), GeographyOperatorTable.ClrStGeogAsGeom) == false &&
                    GeographyOperatorTable.Matches(call.getOperator(), GeographyOperatorTable.ClrStGeomAsGeog) == false)
                    return null;

                var operand = (RexNode)call.getOperands().get(0);

                return call.getType().Equals(operand.getType()) ? operand : null;
            }

            /// <summary>
            /// Rewrites a call to the one spelling of a function that has two.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <para>Each pair is one function under two names, and which names those are is not a judgement:
            /// <c>ST_AsText</c> is <c>return ST_AsWKT(geom);</c>, <c>ST_AsBinary</c> is
            /// <c>ST_AsWKB(geom)</c>, <c>ST_NPoints</c> is <c>ST_NumPoints(geom)</c>,
            /// <c>ST_NumInteriorRing</c> is <c>ST_NumInteriorRings(geom)</c>,
            /// <c>GeographyFunctions.Extent</c> is <c>Envelope(geog)</c>, and
            /// <c>CLR_ST_GEOG_GEOMFROMTEXT</c>/<c>GEOMFROMWKT</c> and
            /// <c>CLR_ST_GEOG_POINT</c>/<c>MAKEPOINT</c> are declared on one method each.</para>
            ///
            /// <para><b>Canonical is the OGC spelling</b>, the other of each pair being the PostGIS or H2GIS
            /// synonym Calcite also carries. What it buys is one digest where a query used both, and one name
            /// for an adapter to match.</para>
            ///
            /// <para><b><c>CLR_ST_GEOG_ASEWKB</c> is not in this list</b>, though today it answers the same
            /// bytes as <c>ASBINARY</c>: Calcite's <c>ST_AsEWKB</c> is <c>return ST_AsWKB(geometry);</c> and
            /// writes no SRID, which is an oversight rather than a declared synonym — <c>ST_AsEWKT</c> has a
            /// body of its own and does write one. An alias rule may rest on two names meaning one thing and
            /// not on two things being equal by a defect.</para>
            /// </remarks>
            RexNode? Alias(RexCall call)
            {
                var canonical = call.getOperator().getName() switch
                {
                    "CLR_ST_GEOG_ASWKT" => GeographyOperatorTable.ClrStGeogAsText,
                    "CLR_ST_GEOG_ASWKB" => GeographyOperatorTable.ClrStGeogAsBinary,
                    "CLR_ST_GEOG_NPOINTS" => GeographyOperatorTable.ClrStGeogNumPoints,
                    "CLR_ST_GEOG_NUMINTERIORRINGS" => GeographyOperatorTable.ClrStGeogNumInteriorRing,
                    "CLR_ST_GEOG_EXTENT" => GeographyOperatorTable.ClrStGeogEnvelope,
                    "CLR_ST_GEOG_GEOMFROMWKT" => Arity(call) == 1 ? GeographyOperatorTable.ClrStGeogGeomFromText : GeographyOperatorTable.ClrStGeogGeomFromTextWithSrid,
                    "CLR_ST_GEOG_MAKEPOINT" => Arity(call) == 2 ? GeographyOperatorTable.ClrStGeogPoint : GeographyOperatorTable.ClrStGeogPoint3D,
                    _ => null,
                };

                return canonical is null ? null : Call(call.getType(), canonical, call.getOperands());
            }

            /// <summary>
            /// Rewrites a relation to the one of its pair the other is written in terms of.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <c>S2Geographies.Contains(a, b)</c> is <c>Within(b, a)</c> and <c>CoveredBy(a, b)</c> is
            /// <c>Covers(b, a)</c>, so each pair is one relation with its operands the other way round, and
            /// the transpose is exact including nulls — both halves answer null on a null argument and only
            /// then. Canonical is the one the other delegates to, which is the same criterion
            /// <c>CLR_ST_GEOG_EXTENT</c> is folded into <c>ENVELOPE</c> by.
            /// </remarks>
            RexNode? Transpose(RexCall call)
            {
                var transposed = call.getOperator().getName() switch
                {
                    "CLR_ST_GEOG_CONTAINS" => GeographyOperatorTable.ClrStGeogWithin,
                    "CLR_ST_GEOG_COVEREDBY" => GeographyOperatorTable.ClrStGeogCovers,
                    _ => null,
                };

                if (transposed is null)
                    return null;

                var operands = call.getOperands();

                return Call(call.getType(), transposed, [(RexNode)operands.get(1), (RexNode)operands.get(0)]);
            }

            /// <summary>
            /// Rewrites the negation of a relation as the relation that is its complement.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <c>S2Geographies.Disjoint(a, b)</c> is <c>Intersects(a, b) == false</c>, so the two are
            /// complements over non-null operands and both are null over a null one — which is what makes the
            /// rewrite exact under three-valued logic as well, <c>NOT NULL</c> being <c>NULL</c>. What it buys
            /// is a bare call where an adapter had a <c>NOT</c> wrapped round one.
            /// </remarks>
            RexNode? Negation(RexCall call)
            {
                if (call.getKind() != SqlKind.NOT)
                    return null;

                if (call.getOperands().get(0) is not RexCall inner)
                    return null;

                var complement = inner.getOperator().getName() switch
                {
                    "CLR_ST_GEOG_DISJOINT" => GeographyOperatorTable.ClrStGeogIntersects,
                    "CLR_ST_GEOG_INTERSECTS" => GeographyOperatorTable.ClrStGeogDisjoint,
                    _ => null,
                };

                return complement is null ? null : Call(call.getType(), complement, inner.getOperands());
            }

            /// <summary>
            /// Rewrites a distance compared against a bound as the predicate that says the same thing.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            /// <remarks>
            /// <para><c>S2Geographies.DWithin(a, b, d)</c> is <c>Distance(a, b) &lt;= d</c>, so
            /// <c>CLR_ST_GEOG_DISTANCE(a, b) &lt;= d</c> is the same expression written out. Only
            /// <c>&lt;=</c>, and <c>&gt;=</c> with the distance on the right: <c>&lt;</c> is a different
            /// predicate at the boundary and there is no operator for it.</para>
            ///
            /// <para><b>It is not cheaper in process</b> — <c>DWithin</c> calls <c>Distance</c> — and that is
            /// not what it is for. A geodesic store has a within-distance predicate its index can answer and
            /// a scalar distance it cannot, so this is the difference between a plan an adapter can push and
            /// one it has to read every row for. It is what PostGIS's own documentation tells a caller to
            /// write by hand.</para>
            /// </remarks>
            RexNode? Distance(RexCall call)
            {
                var (distance, bound) = call.getKind() switch
                {
                    var k when k == SqlKind.LESS_THAN_OR_EQUAL => (call.getOperands().get(0), call.getOperands().get(1)),
                    var k when k == SqlKind.GREATER_THAN_OR_EQUAL => (call.getOperands().get(1), call.getOperands().get(0)),
                    _ => (null, null),
                };

                if (distance is not RexCall measured)
                    return null;
                if (GeographyOperatorTable.Matches(measured.getOperator(), GeographyOperatorTable.ClrStGeogDistance) == false)
                    return null;

                var operands = measured.getOperands();

                return Call(call.getType(), GeographyOperatorTable.ClrStGeogDWithin,
                    [(RexNode)operands.get(0), (RexNode)operands.get(1), (RexNode)bound!]);
            }

            /// <summary>
            /// Returns how many operands a call has.
            /// </summary>
            /// <param name="call"></param>
            /// <returns></returns>
            static int Arity(RexCall call)
            {
                return call.getOperands().size();
            }

            /// <summary>
            /// Builds a call of the given operator, keeping the type the expression already had.
            /// </summary>
            /// <param name="type"></param>
            /// <param name="op"></param>
            /// <param name="operands"></param>
            /// <returns></returns>
            /// <remarks>
            /// The type is carried over rather than inferred again, because the two routes into a plan do not
            /// type a call the same way: this table's <c>CLR_ST_GEOG_WITHIN</c> answers
            /// <c>ReturnTypes.BOOLEAN_NULLABLE</c>, and the one <c>CalciteCatalogReader.toOp</c> builds around
            /// the schema declaration answers <c>createJavaType(Boolean.class)</c>. A rewrite is not the place
            /// to change which a plan has.
            /// </remarks>
            RexNode Call(org.apache.calcite.rel.type.RelDataType type, SqlOperator op, java.util.List operands)
            {
                return rexBuilder.makeCall(type, op, operands);
            }

            /// <inheritdoc cref="Call(org.apache.calcite.rel.type.RelDataType, SqlOperator, java.util.List)" />
            RexNode Call(org.apache.calcite.rel.type.RelDataType type, SqlOperator op, RexNode[] operands)
            {
                var list = new java.util.ArrayList(operands.Length);
                foreach (var operand in operands)
                    list.add(operand);

                return rexBuilder.makeCall(type, op, list);
            }

        }

    }

}
