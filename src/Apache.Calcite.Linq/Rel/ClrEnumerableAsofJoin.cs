using System.Linq.Expressions;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.sql;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="AsofJoin"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// An ASOF join takes, for each left row, the single right row with the same key whose timestamp is
    /// nearest in the direction the match condition gives. The condition is restricted to equalities plus
    /// that one comparison, which is why the node needs the field index and the direction and nothing else.
    /// </remarks>
    public class ClrEnumerableAsofJoin : AsofJoin, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableAsofJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="matchCondition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        /// <returns></returns>
        public static ClrEnumerableAsofJoin Create(RelNode left, RelNode right, RexNode condition, RexNode matchCondition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableHashJoin(mq, left, right, joinType)));

            return new ClrEnumerableAsofJoin(cluster, traitSet, left, right, condition, matchCondition, variablesSet, joinType);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <param name="matchCondition"></param>
        /// <param name="variablesSet"></param>
        /// <param name="joinType"></param>
        public ClrEnumerableAsofJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, RexNode matchCondition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, matchCondition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        /// <remarks>This form knows nothing of the match condition, so it is not the one to call.</remarks>
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            throw new java.lang.RuntimeException("This method should not be called");
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableAsofJoin(
                getCluster(),
                traitSet,
                (RelNode)inputs.get(0),
                (RelNode)inputs.get(1),
                getCondition(),
                getMatchCondition(),
                getVariablesSet(),
                joinType);
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet())!;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left join input
            return ClrEnumerableTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet())!;
        }

        /// <inheritdoc />
        public override RelOptCost computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return planner.getCostFactory().makeCost(mq.getRowCount(this).doubleValue(), 0, 0);
        }

        /// <summary>
        /// Returns the comparator ordering two rows of the right input on their timestamp field, so that the
        /// nearer of two candidates compares greater.
        /// </summary>
        /// <param name="rightCollectionType">type of the rows of the right input</param>
        /// <param name="kind">comparison kind</param>
        /// <param name="timestampFieldIndex">index of the timestamp field</param>
        /// <returns></returns>
        static J.Expression GenerateTimestampComparator(PhysType rightCollectionType, SqlKind kind, int timestampFieldIndex)
        {
            var direction = kind.name() switch
            {
                nameof(SqlKind.LESS_THAN) or nameof(SqlKind.LESS_THAN_OR_EQUAL) => RelFieldCollation.Direction.ASCENDING,
                nameof(SqlKind.GREATER_THAN) or nameof(SqlKind.GREATER_THAN_OR_EQUAL) => RelFieldCollation.Direction.DESCENDING,
                _ => throw new java.lang.RuntimeException($"Unexpected timestamp comparison in ASOF join {kind}"),
            };

            var fieldCollations = new java.util.ArrayList(1);
            fieldCollations.add(new RelFieldCollation(timestampFieldIndex, direction, RelFieldCollation.NullDirection.FIRST));

            return rightCollectionType.generateComparator(RelCollations.of(fieldCollations));
        }

        /// <summary>
        /// Returns the index, within the right input's row, of the field the match condition compares.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        int GetTimestampFieldIndex(RexCall call)
        {
            var leftFieldCount = getLeft().getRowType().getFieldCount();
            var leftInputRef = (RexInputRef)call.getOperands().get(0);
            var rightInputRef = (RexInputRef)call.getOperands().get(1);

            // one of the two comes from each input, and which is which is not known in advance
            return leftInputRef.getIndex() < leftFieldCount
                ? rightInputRef.getIndex() - leftFieldCount
                : leftInputRef.getIndex() - leftFieldCount;
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);

            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            // an ASOF join's condition is equalities and the match condition, and nothing else
            var info = analyzeCondition();
            if (info.nonEquiConditions.isEmpty() == false)
                throw new java.lang.AssertionError();

            var call = (RexCall)getMatchCondition();
            var timestampComparator = implementor.Translator.Translate(GenerateTimestampComparator(rightResult.PhysType, call.getKind(), GetTimestampFieldIndex(call)));

            // the rows are boxed for the same reason a hash join boxes them: the selector and the predicate
            // Calcite builds are against boxed rows, and a row here is compared to null
            var leftSource = ClrEnumUtils.BoxRows(leftResult.PhysType, leftResult.Expression);
            var rightSource = ClrEnumUtils.BoxRows(rightResult.PhysType, rightResult.Expression);
            var leftType = leftSource.Type.GetGenericArguments()[0];
            var rightType = rightSource.Type.GetGenericArguments()[0];
            var rowType = physType.RowType();

            // without nulls, as Calcite keys an ASOF join and has since 1.41: a key of two or more fields is
            // null as a whole where any field of it is null, so a row with a null in its key matches nothing
            // rather than matching another row with a null in the same place
            var leftKey = implementor.Translator.TranslateSelector(leftResult.PhysType.generateAccessorWithoutNulls(info.leftKeys), leftType);
            var rightKey = implementor.Translator.TranslateSelector(rightResult.PhysType.generateAccessorWithoutNulls(info.rightKeys), rightType);

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var matchPredicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getMatchCondition());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.AsofJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType, rowType),
                    leftSource,
                    rightSource,
                    leftKey,
                    rightKey,
                    selector,
                    matchPredicate,
                    timestampComparator,
                    Expression.Constant(joinType.generatesNullsOnRight())));
        }

    }

}
