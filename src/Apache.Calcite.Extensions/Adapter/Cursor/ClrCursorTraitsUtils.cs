using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.util;
using org.apache.calcite.util.mapping;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Trait propagation helpers for projects and joins.
    /// </summary>
    /// <remarks>
    /// A port of <c>EnumerableTraitsUtils</c>, which is package private. Where Calcite calls
    /// <c>collation.apply(mapping)</c>, this calls <c>RexUtil.apply(mapping, collation)</c>, which is what
    /// <c>RelCollationImpl.apply</c> does.
    /// </remarks>
    static class ClrCursorTraitsUtils
    {

        /// <summary>
        /// Returns whether a field collation maps through a project to a field reference or an
        /// order-preserving cast. Mirrors <c>EnumerableTraitsUtils.isCollationOnTrivialExpr</c>.
        /// </summary>
        /// <param name="projects">The project's expressions.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="map">The mapping between input and output fields.</param>
        /// <param name="fc">The field collation.</param>
        /// <param name="passDown">Whether the collation is being passed down to the input (the field is an
        /// output field) rather than derived from it.</param>
        /// <returns><see langword="true"/> if the field maps through the project to an input reference or a cast that preserves order.</returns>
        static bool IsCollationOnTrivialExpr(java.util.List projects, RelDataTypeFactory typeFactory, Mappings.TargetMapping map, RelFieldCollation fc, bool passDown)
        {
            var index = fc.getFieldIndex();
            var target = map.getTargetOpt(index);
            if (target < 0)
                return false;

            var node = (RexNode)(passDown ? projects.get(index) : projects.get(target));
            if (node.isA(SqlKind.CAST))
            {
                // a cast preserves the order only if it is monotonic
                var cast = (RexCall)node;
                var newFieldCollation = RexUtil.apply(map, fc) ?? throw new java.lang.NullPointerException();
                var binding = RexCallBinding.create(typeFactory, cast, com.google.common.collect.ImmutableList.of(RelCollations.of([newFieldCollation])));

                return cast.getOperator().getMonotonicity(binding).name() != nameof(SqlMonotonicity.NOT_MONOTONIC);
            }

            return true;
        }

        /// <summary>
        /// Passes a required collation through a project to its input, where every sort field maps to an
        /// input field. Mirrors <c>EnumerableTraitsUtils.passThroughTraitsForProject</c>.
        /// </summary>
        /// <returns>The project's traits and its input's required traits, or <see langword="null"/> if the
        /// collation cannot be passed through.</returns>
        /// <param name="required">The traits required of the project.</param>
        /// <param name="exps">The project's expressions.</param>
        /// <param name="inputRowType">The row type of the project's input.</param>
        /// <param name="typeFactory">The type factory, used to judge whether a cast preserves order.</param>
        /// <param name="currentTraits">The project's current traits; both returned trait sets are these with the collation replaced.</param>
        public static Pair? PassThroughTraitsForProject(RelTraitSet required, java.util.List exps, RelDataType inputRowType, RelDataTypeFactory typeFactory, RelTraitSet currentTraits)
        {
            var collation = required.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null;

            var map = RelOptUtil.permutationIgnoreCast(exps, inputRowType);

            for (int i = 0; i < collation.getFieldCollations().size(); i++)
                if (IsCollationOnTrivialExpr(exps, typeFactory, map, (RelFieldCollation)collation.getFieldCollations().get(i), true) == false)
                    return null;

            var newCollation = RexUtil.apply(map, collation);

            return Pair.of(currentTraits.replace(collation), com.google.common.collect.ImmutableList.of(currentTraits.replace(newCollation)));
        }

        /// <summary>
        /// Derives a project's collation from the longest prefix of its input's collation that maps through the
        /// project. Mirrors <c>EnumerableTraitsUtils.deriveTraitsForProject</c>.
        /// </summary>
        /// <returns>The project's derived traits and its input's traits, or <see langword="null"/> if nothing
        /// can be derived.</returns>
        /// <param name="childTraits">The traits of the project's input.</param>
        /// <param name="childId">The ordinal of the input <paramref name="childTraits"/> belongs to; a project has only input 0.</param>
        /// <param name="exps">The project's expressions.</param>
        /// <param name="inputRowType">The row type of the project's input.</param>
        /// <param name="typeFactory">The type factory, used to judge whether a cast preserves order.</param>
        /// <param name="currentTraits">The project's current traits; both returned trait sets are these with the collation replaced.</param>
        public static Pair? DeriveTraitsForProject(RelTraitSet childTraits, int childId, java.util.List exps, RelDataType inputRowType, RelDataTypeFactory typeFactory, RelTraitSet currentTraits)
        {
            var collation = childTraits.getCollation();
            if (collation == null || collation == RelCollations.EMPTY)
                return null;

            var maxField = java.lang.Math.max(exps.size(), inputRowType.getFieldCount());
            var mapping = Mappings.create(MappingType.FUNCTION, maxField, maxField);

            for (int i = 0; i < exps.size(); i++)
            {
                var e = (RexNode)exps.get(i);
                if (e is RexInputRef inputRef)
                {
                    mapping.set(inputRef.getIndex(), i);
                }
                else if (e.isA(SqlKind.CAST))
                {
                    var operand = (RexNode)((RexCall)e).getOperands().get(0);
                    if (operand is RexInputRef operandRef)
                        mapping.set(operandRef.getIndex(), i);
                }
            }

            var collationFieldsToDerive = new java.util.ArrayList();
            for (int i = 0; i < collation.getFieldCollations().size(); i++)
            {
                var rc = (RelFieldCollation)collation.getFieldCollations().get(i);
                if (IsCollationOnTrivialExpr(exps, typeFactory, mapping, rc, false))
                    collationFieldsToDerive.add(rc);
                else
                    break;
            }

            if (collationFieldsToDerive.isEmpty() == false)
            {
                var newCollation = RexUtil.apply(mapping, RelCollations.of(collationFieldsToDerive));

                return Pair.of(currentTraits.replace(newCollation), com.google.common.collect.ImmutableList.of(currentTraits.replace(collation)));
            }

            return null;
        }

        /// <summary>
        /// Passes a required collation on left fields only to a join's left input. Mirrors
        /// <c>EnumerableTraitsUtils.passThroughTraitsForJoin</c>.
        /// </summary>
        /// <param name="required">The traits required of the join.</param>
        /// <param name="joinType">The join type; nothing is passed for <c>RIGHT</c> or <c>FULL</c>.</param>
        /// <param name="leftInputFieldCount">The number of fields of the left input.</param>
        /// <param name="joinTraitSet">The join's traits.</param>
        /// <returns>The join's traits and its inputs' required traits, or <see langword="null"/>.</returns>
        public static Pair? PassThroughTraitsForJoin(RelTraitSet required, JoinRelType joinType, int leftInputFieldCount, RelTraitSet joinTraitSet)
        {
            var collation = required.getCollation();
            if (collation == null
                || collation == RelCollations.EMPTY
                || joinType.name() == nameof(JoinRelType.FULL)
                || joinType.name() == nameof(JoinRelType.RIGHT))
                return null;

            for (int i = 0; i < collation.getFieldCollations().size(); i++)
            {
                // a sort field of the right input cannot be pushed down
                if (((RelFieldCollation)collation.getFieldCollations().get(i)).getFieldIndex() >= leftInputFieldCount)
                    return null;
            }

            var passthroughTraitSet = joinTraitSet.replace(collation);

            return Pair.of(
                passthroughTraitSet,
                com.google.common.collect.ImmutableList.of(passthroughTraitSet, passthroughTraitSet.replace(RelCollations.EMPTY)));
        }

        /// <summary>
        /// Derives a join's collation from its left input's. Mirrors
        /// <c>EnumerableTraitsUtils.deriveTraitsForJoin</c>.
        /// </summary>
        /// <param name="childTraits">The left input's traits.</param>
        /// <param name="childId">The input's ordinal, which must be 0.</param>
        /// <param name="joinType">The join type; nothing is derived for <c>RIGHT</c> or <c>FULL</c>.</param>
        /// <param name="joinTraitSet">The join's traits.</param>
        /// <param name="rightTraitSet">The right input's traits.</param>
        /// <returns>The join's derived traits and its inputs' traits, or <see langword="null"/>.</returns>
        /// <exception cref="java.lang.AssertionError"><paramref name="childId"/> is not 0.</exception>
        public static Pair? DeriveTraitsForJoin(RelTraitSet childTraits, int childId, JoinRelType joinType, RelTraitSet joinTraitSet, RelTraitSet rightTraitSet)
        {
            if (childId != 0)
                throw new java.lang.AssertionError();

            var collation = childTraits.getCollation();
            if (collation == null
                || collation == RelCollations.EMPTY
                || joinType.name() == nameof(JoinRelType.FULL)
                || joinType.name() == nameof(JoinRelType.RIGHT))
                return null;

            var derivedTraits = joinTraitSet.replace(collation);

            return Pair.of(derivedTraits, com.google.common.collect.ImmutableList.of(derivedTraits, rightTraitSet));
        }

    }

}
