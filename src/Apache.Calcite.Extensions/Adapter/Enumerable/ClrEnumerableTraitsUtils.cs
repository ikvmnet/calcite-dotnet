using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.sql.validate;
using org.apache.calcite.util;
using org.apache.calcite.util.mapping;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Utilities for trait propagation.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableTraitsUtils</c>, which is package private, so every method here is a
    /// port. What a node does with a required or a derived collation is a property of the algorithm and not
    /// of the language it is written in, so these are line-by-line.
    ///
    /// <para>One reachable-public-route substitution: Calcite writes <c>collation.apply(mapping)</c>, and
    /// <c>RelCollationImpl.apply</c> is <c>RexUtil.apply(mapping, this)</c>. The interface method is a
    /// generic default that IKVM erases, so the public static is called directly. It is the same call.</para>
    /// </remarks>
    static class ClrEnumerableTraitsUtils
    {

        /// <summary>
        /// Returns whether there is a mapping between a project's input and output fields, bailing out where a
        /// sort relies on a non-trivial expression.
        /// </summary>
        /// <param name="projects"></param>
        /// <param name="typeFactory"></param>
        /// <param name="map"></param>
        /// <param name="fc"></param>
        /// <param name="passDown"></param>
        /// <returns></returns>
        static bool IsCollationOnTrivialExpr(java.util.List projects, RelDataTypeFactory typeFactory, Mappings.TargetMapping map, RelFieldCollation fc, bool passDown)
        {
            var index = fc.getFieldIndex();
            var target = map.getTargetOpt(index);
            if (target < 0)
                return false;

            var node = (RexNode)(passDown ? projects.get(index) : projects.get(target));
            if (node.isA(SqlKind.CAST))
            {
                // check whether it is a monotonic preserving cast
                var cast = (RexCall)node;
                var newFieldCollation = RexUtil.apply(map, fc) ?? throw new java.lang.NullPointerException();
                var binding = RexCallBinding.create(typeFactory, cast, com.google.common.collect.ImmutableList.of(RelCollations.of([newFieldCollation])));

                return cast.getOperator().getMonotonicity(binding).name() != nameof(SqlMonotonicity.NOT_MONOTONIC);
            }

            return true;
        }

        /// <summary>
        /// Passes a required collation down through a project, where the projection is a permutation of its
        /// input's fields.
        /// </summary>
        /// <param name="required"></param>
        /// <param name="exps"></param>
        /// <param name="inputRowType"></param>
        /// <param name="typeFactory"></param>
        /// <param name="currentTraits"></param>
        /// <returns></returns>
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
        /// Derives a collation of a project from the collation its input carries.
        /// </summary>
        /// <param name="childTraits"></param>
        /// <param name="childId"></param>
        /// <param name="exps"></param>
        /// <param name="inputRowType"></param>
        /// <param name="typeFactory"></param>
        /// <param name="currentTraits"></param>
        /// <returns></returns>
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
        /// Passes a required collation down to a join's left input, and to no other.
        /// </summary>
        /// <param name="required">required trait set for the join</param>
        /// <param name="joinType">the join type</param>
        /// <param name="leftInputFieldCount">number of fields of the left join input</param>
        /// <param name="joinTraitSet">trait set of the join</param>
        /// <returns></returns>
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
                // if the field collation belongs to the right input the collation cannot be pushed down
                if (((RelFieldCollation)collation.getFieldCollations().get(i)).getFieldIndex() >= leftInputFieldCount)
                    return null;
            }

            var passthroughTraitSet = joinTraitSet.replace(collation);

            return Pair.of(
                passthroughTraitSet,
                com.google.common.collect.ImmutableList.of(passthroughTraitSet, passthroughTraitSet.replace(RelCollations.EMPTY)));
        }

        /// <summary>
        /// Derives a join's collation from its left input's, and from no other.
        /// </summary>
        /// <param name="childTraits">trait set of the child</param>
        /// <param name="childId">id of the child (0 is the left join input)</param>
        /// <param name="joinType">the join type</param>
        /// <param name="joinTraitSet">trait set of the join</param>
        /// <param name="rightTraitSet">trait set of the right join input</param>
        /// <returns></returns>
        public static Pair? DeriveTraitsForJoin(RelTraitSet childTraits, int childId, JoinRelType joinType, RelTraitSet joinTraitSet, RelTraitSet rightTraitSet)
        {
            // should only derive traits (limited to collation for now) from the left join input
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
