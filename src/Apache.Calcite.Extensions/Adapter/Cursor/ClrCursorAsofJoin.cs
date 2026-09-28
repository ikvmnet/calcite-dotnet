using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;
using org.apache.calcite.sql;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="AsofJoin"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableAsofJoin</c>. For each left row the join takes the one right row with an equal key
    /// that satisfies the match condition and is nearest on the compared timestamp field. The condition is
    /// equalities only; the match condition is a single comparison.
    ///
    /// <para>Both inputs are drained when the node's cursor is opened, the left and then the right, as in
    /// linq4j's <c>asofJoin</c>. The right input is passed as an opener so it is acquired only after the left
    /// has been drained and closed.</para>
    /// </remarks>
    public class ClrCursorAsofJoin : AsofJoin, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorAsofJoin"/>, deriving its collation as Calcite does for a hash join.
        /// </summary>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The equi-join condition.</param>
        /// <param name="matchCondition">The comparison that selects the nearest right row.</param>
        /// <param name="variablesSet">The correlation variables set by this join.</param>
        /// <param name="joinType">The join type.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorAsofJoin Create(RelNode left, RelNode right, RexNode condition, RexNode matchCondition, java.util.Set variablesSet, JoinRelType joinType)
        {
            var cluster = left.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.enumerableHashJoin(mq, left, right, joinType)));

            return new ClrCursorAsofJoin(cluster, traitSet, left, right, condition, matchCondition, variablesSet, joinType);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The equi-join condition.</param>
        /// <param name="matchCondition">The comparison that selects the nearest right row.</param>
        /// <param name="variablesSet">The correlation variables set by this join.</param>
        /// <param name="joinType">The join type.</param>
        public ClrCursorAsofJoin(RelOptCluster cluster, RelTraitSet traits, RelNode left, RelNode right, RexNode condition, RexNode matchCondition, java.util.Set variablesSet, JoinRelType joinType) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), left, right, condition, matchCondition, variablesSet, joinType)
        {

        }

        /// <inheritdoc />
        /// <remarks>Always throws, as in Calcite: this overload cannot carry the match condition.</remarks>
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            throw new java.lang.RuntimeException("This method should not be called");
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorAsofJoin(
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
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForJoin(required, joinType, getLeft().getRowType().getFieldCount(), getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            // should only derive traits (limited to collation for now) from the left join input
            return ClrCursorTraitsUtils.DeriveTraitsForJoin(childTraits, childId, joinType, getTraitSet(), getRight().getTraitSet());
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return planner.getCostFactory().makeCost(mq.getRowCount(this).doubleValue(), 0, 0);
        }

        /// <summary>
        /// Returns a comparator ordering right rows on their timestamp field, ascending for <c>&lt;</c> and
        /// <c>&lt;=</c> and descending for <c>&gt;</c> and <c>&gt;=</c>, nulls first. The join keeps the
        /// candidate that compares greatest.
        /// </summary>
        /// <param name="rightCollectionType">The physical type of the right input's rows.</param>
        /// <param name="kind">The match condition's comparison kind.</param>
        /// <param name="timestampFieldIndex">The index of the timestamp field in the right row.</param>
        /// <returns>An expression producing the comparator.</returns>
        /// <remarks>Mirrors <c>EnumerableAsofJoin.generateTimestampComparator</c>.</remarks>
        static Expression GenerateTimestampComparator(ClrPhysType rightCollectionType, SqlKind kind, int timestampFieldIndex)
        {
            var direction = kind.name() switch
            {
                nameof(SqlKind.LESS_THAN) or nameof(SqlKind.LESS_THAN_OR_EQUAL) => RelFieldCollation.Direction.ASCENDING,
                nameof(SqlKind.GREATER_THAN) or nameof(SqlKind.GREATER_THAN_OR_EQUAL) => RelFieldCollation.Direction.DESCENDING,
                _ => throw new java.lang.RuntimeException($"Unexpected timestamp comparison in ASOF join {kind}"),
            };

            var fieldCollations = new java.util.ArrayList(1);
            fieldCollations.add(new RelFieldCollation(timestampFieldIndex, direction, RelFieldCollation.NullDirection.FIRST));

            return rightCollectionType.GenerateComparator(RelCollations.of(fieldCollations));
        }

        /// <summary>
        /// Returns the index, within the right input's row, of the field the match condition compares.
        /// </summary>
        /// <param name="call">The match condition.</param>
        /// <returns>The field index relative to the right input.</returns>
        int GetTimestampFieldIndex(RexCall call)
        {
            var leftFieldCount = getLeft().getRowType().getFieldCount();
            var leftInputRef = (RexInputRef)call.getOperands().get(0);
            var rightInputRef = (RexInputRef)call.getOperands().get(1);

            // one operand comes from each input, in either order
            return leftInputRef.getIndex() < leftFieldCount
                ? rightInputRef.getIndex() - leftFieldCount
                : leftInputRef.getIndex() - leftFieldCount;
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            // an ASOF join's condition holds only equalities
            var info = analyzeCondition();
            if (info.nonEquiConditions.isEmpty() == false)
                throw new java.lang.AssertionError();

            var call = (RexCall)getMatchCondition();
            var timestampComparator = GenerateTimestampComparator(rightResult.PhysType, call.getKind(), GetTimestampFieldIndex(call));

            // the row types are the physical types' boxed rows: the selector and predicate are built against
            // boxed rows, and an outer join compares a row to null
            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            // keyed without nulls, as Calcite keys an ASOF join: a key with any null field is null as a whole,
            // so a row with a null in its key matches nothing
            var leftKey = leftResult.PhysType.GenerateAccessorWithoutNulls(info.leftKeys);
            var rightKey = rightResult.PhysType.GenerateAccessorWithoutNulls(info.rightKeys);

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var matchPredicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getMatchCondition());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.AsofJoin.MakeGenericMethod(leftType, rightType, leftKey.ReturnType, rowType),
                    leftResult.Expression,
                    implementor.Opener(rightResult),
                    leftKey,
                    rightKey,
                    selector,
                    matchPredicate,
                    timestampComparator,
                    Expression.Constant(joinType.generatesNullsOnRight())));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());

            // an ASOF join's condition holds only equalities
            var info = analyzeCondition();
            if (info.nonEquiConditions.isEmpty() == false)
                throw new java.lang.AssertionError();

            var call = (RexCall)getMatchCondition();
            var timestampComparator = GenerateTimestampComparator(rightResult.PhysType, call.getKind(), GetTimestampFieldIndex(call));

            // the row types are the physical types' boxed rows: the selector and predicate are built against
            // boxed rows, and an outer join compares a row to null
            var leftType = leftResult.PhysType.RowType;
            var rightType = rightResult.PhysType.RowType;
            var rowType = physType.RowType;

            // keyed without nulls, as Calcite keys an ASOF join: a key with any null field is null as a whole,
            // so a row with a null in its key matches nothing
            var leftKey = leftResult.PhysType.GenerateAccessorWithoutNulls(info.leftKeys);
            var rightKey = rightResult.PhysType.GenerateAccessorWithoutNulls(info.rightKeys);

            var selector = ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftResult.PhysType, rightResult.PhysType);
            var matchPredicate = ClrEnumUtils.GeneratePredicate(implementor, getCluster().getRexBuilder(), getLeft(), getRight(), leftResult.PhysType, rightResult.PhysType, getMatchCondition());

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.AsofJoinAsync.MakeGenericMethod(leftType, rightType, leftKey.ReturnType, rowType),
                    leftResult.Expression,
                    implementor.OpenerAsync(rightResult),
                    leftKey,
                    rightKey,
                    selector,
                    matchPredicate,
                    timestampComparator,
                    Expression.Constant(joinType.generatesNullsOnRight())));
        }

    }

}
