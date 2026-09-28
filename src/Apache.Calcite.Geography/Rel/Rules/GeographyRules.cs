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
    /// Rules that rewrite <c>CLR_ST_GEOG_*</c> calls into a canonical form.
    /// </summary>
    /// <remarks>
    /// <para>Run them as a separate pass ahead of the host's own program:</para>
    ///
    /// <code>
    /// Programs.sequence(GeographyRules.Program(), Programs.standard())
    /// </code>
    ///
    /// <para>They do not work on a <c>VolcanoPlanner</c>. <c>VolcanoCost.isLt</c> compares row counts only, so a
    /// rewritten filter is never cheaper than the original and the planner keeps whichever it registered first.
    /// For the same reason Calcite runs <c>Programs.calc</c> as a hep pass.</para>
    ///
    /// <para>Every rewrite replaces an expression with one of equal value, including under three-valued logic, so it
    /// holds in a projection, a filter or a join condition alike. The rewrites are: a crossing
    /// (<c>CLR_ST_GEOG_ASGEOM</c>, <c>CLR_ST_GEOM_ASGEOG</c>) whose operand already has the call's type is removed;
    /// an alias is replaced by its canonical name; <c>CONTAINS</c> and <c>COVEREDBY</c> are rewritten as
    /// <c>WITHIN</c> and <c>COVERS</c> with the operands swapped; <c>NOT DISJOINT</c> and <c>NOT INTERSECTS</c>
    /// become <c>INTERSECTS</c> and <c>DISJOINT</c>; <c>DISTANCE(a, b) &lt;= d</c> becomes <c>DWITHIN(a, b, d)</c>;
    /// and a call resolved through a schema has this package's operator restored
    /// (<see cref="GeographyOperatorTable.Rebind"/>).</para>
    ///
    /// <para>Constant folding, such as reducing <c>CLR_ST_GEOG_GEOMFROMTEXT('POINT(0 0)')</c> to a literal, is done
    /// by Calcite's <c>CoreRules.FILTER_REDUCE_EXPRESSIONS</c> and needs an executor on the planner. A
    /// <c>jdbc:calcite:</c> connection has one; a <c>Frameworks</c> configuration has one only if given, and without
    /// it a WKT literal is parsed once per row:</para>
    ///
    /// <code>
    /// Frameworks.newConfigBuilder().executor(RexUtil.EXECUTOR)
    /// </code>
    /// </remarks>
    public static class GeographyRules
    {

        /// <summary>
        /// Rewrites the condition of a <see cref="org.apache.calcite.rel.core.Filter"/>.
        /// </summary>
        public static readonly RelOptRule Filter =
            new GeographyRule(Config("GeographyFilterRule", (java.lang.Class)typeof(Filter)));

        /// <summary>
        /// Rewrites the expressions of a <see cref="org.apache.calcite.rel.core.Project"/>.
        /// </summary>
        public static readonly RelOptRule Project =
            new GeographyRule(Config("GeographyProjectRule", (java.lang.Class)typeof(Project)));

        /// <summary>
        /// Rewrites the condition of a <see cref="org.apache.calcite.rel.core.Join"/>.
        /// </summary>
        public static readonly RelOptRule Join =
            new GeographyRule(Config("GeographyJoinRule", (java.lang.Class)typeof(Join)));

        /// <summary>
        /// Returns every rule in this set, for a host that adds them to a hep pass of its own.
        /// </summary>
        /// <returns><see cref="Filter"/>, <see cref="Project"/> and <see cref="Join"/>.</returns>
        /// <remarks>
        /// Use <see cref="Program"/> otherwise. The rules have little effect on a <c>VolcanoPlanner</c>; see the
        /// remarks on this class.
        /// </remarks>
        public static IReadOnlyList<RelOptRule> Rules()
        {
            return [Filter, Project, Join];
        }

        /// <summary>
        /// Returns these rules as a hep pass to run ahead of the host's own program.
        /// </summary>
        /// <returns>A program that applies <see cref="Rules"/> until none matches.</returns>
        /// <remarks>
        /// The pass runs with <c>noDag</c> set, as <c>Programs.calc</c> does, and uses Calcite's default metadata
        /// provider.
        /// </remarks>
        public static Program Program()
        {
            var rules = new java.util.ArrayList();
            foreach (var rule in Rules())
                rules.add(rule);

            return Programs.hep(rules, true, DefaultRelMetadataProvider.INSTANCE);
        }

        /// <summary>
        /// Applies every <c>CLR_ST_GEOG_*</c> rewrite to an expression.
        /// </summary>
        /// <param name="rexBuilder">The builder used to create rewritten calls.</param>
        /// <param name="node">The expression.</param>
        /// <returns>The rewritten expression, or <paramref name="node"/> where nothing applied.</returns>
        /// <remarks>
        /// For an adapter that wants the canonical form of an expression before matching operator names, or a host that
        /// runs its own <c>RexShuttle</c>.
        /// </remarks>
        public static RexNode Simplify(RexBuilder rexBuilder, RexNode node)
        {
            ArgumentNullException.ThrowIfNull(rexBuilder);
            ArgumentNullException.ThrowIfNull(node);

            return (RexNode)node.accept(new Shuttle(rexBuilder));
        }

        /// <summary>
        /// Returns a rule configuration whose operand matches any node of the given class.
        /// </summary>
        /// <remarks>
        /// <c>RelRule.Config</c> has no neutral instance; every concrete configuration is generated for a particular
        /// rule. <c>FilterToCalcRule</c>'s is borrowed and re-pointed. Only its operand supplier, description and
        /// <c>relBuilderFactory</c> are read, and its <c>toRule</c> is never called because the rule is constructed
        /// directly.
        /// </remarks>
        /// <param name="description">The name the rule is reported under in planner traces.</param>
        /// <param name="relClass">The class of node the operand matches, subclasses included.</param>
        /// <returns>A configuration to construct a <see cref="GeographyRule"/> from.</returns>
        static RelRule.Config Config(string description, java.lang.Class relClass)
        {
            return ((RelRule.Config)FilterToCalcRule.Config.DEFAULT)
                .withOperandSupplier(new OperandTransform(b => b.operand(relClass).anyInputs()))
                .withDescription(description);
        }

        /// <summary>
        /// A <see cref="RelRule.OperandTransform"/> backed by a delegate.
        /// </summary>
        /// <param name="transform">Builds the operand from the builder it is given.</param>
        sealed class OperandTransform(Func<RelRule.OperandBuilder, RelRule.Done> transform) : RelRule.OperandTransform
        {

            /// <inheritdoc />
            public object apply(object builder)
            {
                return transform((RelRule.OperandBuilder)builder);
            }

            /// <inheritdoc />
            /// <remarks>
            /// A C# class does not inherit the default methods of an interface IKVM compiled, so this forwards to the Java
            /// default.
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
        /// Runs the <see cref="Shuttle"/> over the expressions of the matched node.
        /// </summary>
        /// <remarks>
        /// One class serves all three node kinds, because <c>RelNode.accept(RexShuttle)</c> rewrites whatever expressions
        /// a node holds.
        /// </remarks>
        /// <param name="config">The configuration carrying the rule's operand and description.</param>
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
        /// Applies the rewrites to each call, bottom up.
        /// </summary>
        /// <param name="rexBuilder">Builds the calls that replace the rewritten ones.</param>
        sealed class Shuttle(RexBuilder rexBuilder) : RexShuttle
        {

            /// <summary>
            /// Gets whether anything was rewritten.
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
            /// Replaces the operator of a call resolved through a schema with this package's operator.
            /// </summary>
            /// <remarks>
            /// Only the operator changes, restoring the strictness and symmetry Calcite's simplifications read. Tried last,
            /// so that a call another rewrite has already rebuilt is not rebuilt again.
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The call with this package's operator, or <c>null</c> where the operator is not one
            /// <see cref="GeographyOperatorTable.Rebind"/> recognises or is already this package's.</returns>
            RexNode? Rebind(RexCall call)
            {
                var mine = GeographyOperatorTable.Rebind(call.getOperator());
                if (mine is null || ReferenceEquals(mine, call.getOperator()))
                    return null;

                return Call(call.getType(), mine, call.getOperands());
            }

            /// <summary>
            /// Removes a <c>CLR_ST_GEOG_ASGEOM</c> or <c>CLR_ST_GEOM_ASGEOG</c> whose operand already has the call's type.
            /// </summary>
            /// <remarks>
            /// Both return their argument unchanged, and geographies and geometries share one type, so the call does
            /// nothing but cost a dispatch per row. This also removes a round trip such as
            /// <c>CLR_ST_GEOG_ASGEOM(CLR_ST_GEOM_ASGEOG(x))</c>. Where the types differ (an operand typed <c>GEOMETRY</c>
            /// under a call typed <c>JavaType(Geometry)</c>) the call stays, so that the enclosing expression is handed the
            /// same type.
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
            static RexNode? Crossing(RexCall call)
            {
                if (GeographyOperatorTable.Matches(call.getOperator(), GeographyOperatorTable.ClrStGeogAsGeom) == false &&
                    GeographyOperatorTable.Matches(call.getOperator(), GeographyOperatorTable.ClrStGeomAsGeog) == false)
                    return null;

                var operand = (RexNode)call.getOperands().get(0);

                return call.getType().Equals(operand.getType()) ? operand : null;
            }

            /// <summary>
            /// Replaces a call to an alias with a call to the canonical name.
            /// </summary>
            /// <remarks>
            /// <para>Each pair is one function under two names: Calcite's <c>ST_AsText</c> is <c>ST_AsWKT</c>,
            /// <c>ST_AsBinary</c> is <c>ST_AsWKB</c>, <c>ST_NPoints</c> is <c>ST_NumPoints</c> and
            /// <c>ST_NumInteriorRings</c> is <c>ST_NumInteriorRing</c>; <see cref="Apache.Calcite.Geography.Runtime.GeographyFunctions.Extent"/> is
            /// <see cref="Apache.Calcite.Geography.Runtime.GeographyFunctions.Envelope"/>; and <c>GEOMFROMTEXT</c>/<c>GEOMFROMWKT</c> and
            /// <c>POINT</c>/<c>MAKEPOINT</c> are declared on the same methods. The canonical name is the OGC one.</para>
            ///
            /// <para><c>CLR_ST_GEOG_ASEWKB</c> is not treated as an alias of <c>ASBINARY</c>, though it returns the same
            /// bytes: Calcite's <c>ST_AsEWKB</c> writes no SRID, which is a defect rather than a declared synonym, and
            /// <c>ST_AsEWKT</c> does write one.</para>
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
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
            /// Rewrites <c>CONTAINS(a, b)</c> as <c>WITHIN(b, a)</c> and <c>COVEREDBY(a, b)</c> as <c>COVERS(b, a)</c>.
            /// </summary>
            /// <remarks>
            /// <c>S2Geographies.Contains(a, b)</c> is <c>Within(b, a)</c> and <c>CoveredBy(a, b)</c> is <c>Covers(b, a)</c>,
            /// and each pair treats a null argument alike, so the rewrite is exact.
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
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
            /// Rewrites <c>NOT DISJOINT</c> as <c>INTERSECTS</c> and <c>NOT INTERSECTS</c> as <c>DISJOINT</c>.
            /// </summary>
            /// <remarks>
            /// <c>S2Geographies.Disjoint</c> is the negation of <c>Intersects</c>, and both return null for a null
            /// argument, so the rewrite holds under three-valued logic. It gives an adapter a bare call to match rather than
            /// one wrapped in <c>NOT</c>.
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
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
            /// Rewrites <c>CLR_ST_GEOG_DISTANCE(a, b) &lt;= d</c>, or <c>d &gt;= CLR_ST_GEOG_DISTANCE(a, b)</c>, as
            /// <c>CLR_ST_GEOG_DWITHIN(a, b, d)</c>.
            /// </summary>
            /// <remarks>
            /// <para><c>S2Geographies.DWithin(a, b, d)</c> is <c>Distance(a, b) &lt;= d</c>. A strict <c>&lt;</c> differs at
            /// the boundary and has no operator, so it is left alone.</para>
            ///
            /// <para>The rewrite is not cheaper to evaluate. Its purpose is pushdown: a geodesic store can answer a
            /// within-distance predicate from its index but not a comparison on a computed distance.</para>
            /// </remarks>
            /// <param name="call">The call, its operands already rewritten.</param>
            /// <returns>The rewritten expression, or <c>null</c> where the rewrite does not apply.</returns>
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
            /// <param name="call">The call to count the operands of.</param>
            /// <returns>The number of operands.</returns>
            static int Arity(RexCall call)
            {
                return call.getOperands().size();
            }

            /// <summary>
            /// Builds a call of the given operator with the type the original expression had.
            /// </summary>
            /// <remarks>
            /// The type is kept rather than inferred again, because the two routes into a plan type a call differently:
            /// this table's <c>CLR_ST_GEOG_WITHIN</c> returns <c>ReturnTypes.BOOLEAN_NULLABLE</c>, and the operator
            /// <c>CalciteCatalogReader.toOp</c> builds from the schema declaration returns
            /// <c>createJavaType(Boolean.class)</c>. A rewrite must not change the type of the expression it replaces.
            /// </remarks>
            /// <param name="type">The type of the expression being replaced.</param>
            /// <param name="op">The operator to call.</param>
            /// <param name="operands">The operands, in order.</param>
            /// <returns>The new call.</returns>
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
