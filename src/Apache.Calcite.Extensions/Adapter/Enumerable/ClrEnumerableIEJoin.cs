using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of an inner <see cref="Join"/> of two inequality predicates in the
    /// <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <c>EnumerableIEJoin</c>. The condition is exactly two conjunctions, each an inequality between one
    /// field of the left input and one field of the right; the third and later conjunctions of a wider
    /// condition are left to a calc above, by the rule.
    ///
    /// <para>Based on Khayyat et al., "Lightning Fast and Space Efficient Inequality Joins", PVLDB 8(13),
    /// 2015, which is the algorithm <see cref="ClrEnumerableDefaults.IeJoin"/> walks.</para>
    /// </remarks>
    public class ClrEnumerableIEJoin : Join, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableIEJoin"/>.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static ClrEnumerableIEJoin Create(RelNode left, RelNode right, RexNode condition)
        {
            System.ArgumentNullException.ThrowIfNull(left);

            return new ClrEnumerableIEJoin(
                left.getCluster(),
                left.getCluster().traitSetOf(ClrEnumerableConvention.Instance),
                left,
                right,
                condition);
        }

        /// <summary>
        /// Returns the normalized form of a conjunction that is an inequality between one field of the left
        /// input and one field of the right, or <see langword="null"/> where it is anything else.
        /// </summary>
        /// <param name="node"></param>
        /// <param name="leftFieldCount"></param>
        /// <returns></returns>
        internal static Condition? AnalyzeConjunction(RexNode node, int leftFieldCount)
        {
            if (node is not RexCall call || call.operands.size() != 2)
                return null;
            if (call.operands.get(0) is not RexInputRef first_ || call.operands.get(1) is not RexInputRef second_)
                return null;

            var first = first_.getIndex();
            var second = second_.getIndex();
            var firstIsLeft = first < leftFieldCount;
            var secondIsLeft = second < leftFieldCount;

            // both operands on one side is not a join condition this node can drive
            if (firstIsLeft == secondIsLeft)
                return null;

            // the condition is normalized to read left-to-right, so a right-first one is reversed
            var kind = firstIsLeft ? call.getKind() : call.getKind().reverse();

            ExpressionType op;
            switch (kind.name())
            {
                case nameof(org.apache.calcite.sql.SqlKind.LESS_THAN):
                    op = ExpressionType.LessThan;
                    break;
                case nameof(org.apache.calcite.sql.SqlKind.LESS_THAN_OR_EQUAL):
                    op = ExpressionType.LessThanOrEqual;
                    break;
                case nameof(org.apache.calcite.sql.SqlKind.GREATER_THAN):
                    op = ExpressionType.GreaterThan;
                    break;
                case nameof(org.apache.calcite.sql.SqlKind.GREATER_THAN_OR_EQUAL):
                    op = ExpressionType.GreaterThanOrEqual;
                    break;
                default:
                    return null;
            }

            return firstIsLeft
                ? new Condition(first, second - leftFieldCount, op)
                : new Condition(second, first - leftFieldCount, op);
        }

        /// <summary>
        /// Returns whether the two fields a condition names can be ordered by the one comparator the scan
        /// walks.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        internal static bool SupportsKeyTypes(RelNode left, RelNode right, Condition condition)
        {
            var leftType = ((RelDataTypeField)left.getRowType().getFieldList().get(condition.LeftKey)).getType();
            var rightType = ((RelDataTypeField)right.getRowType().getFieldList().get(condition.RightKey)).getType();
            var typeName = leftType.getSqlTypeName();

            // a floating-point sort order disagrees with <, <=, > and >= for NaN and for signed zero
            return SqlTypeUtil.equalSansNullability(left.getCluster().getTypeFactory(), leftType, rightType)
                && (SqlTypeUtil.isBoolean(leftType)
                    || (SqlTypeUtil.isExactNumeric(leftType) && SqlTypeName.UNSIGNED_TYPES.contains(typeName) == false)
                    || SqlTypeUtil.isCharacter(leftType)
                    || SqlTypeUtil.isBinary(leftType)
                    || SqlTypeUtil.isDatetime(leftType)
                    || SqlTypeUtil.isInterval(leftType));
        }

        readonly Condition[] conditions;

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="condition"></param>
        public ClrEnumerableIEJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), left, right, condition, com.google.common.collect.ImmutableSet.of(), JoinRelType.INNER)
        {
            var conjunctions = RelOptUtil.conjunctions(condition);
            if (conjunctions.size() != 2)
                throw new java.lang.IllegalArgumentException("condition must contain exactly two supported cross-input inequalities");

            var leftFieldCount = left.getRowType().getFieldCount();
            var first = AnalyzeConjunction((RexNode)conjunctions.get(0), leftFieldCount);
            var second = AnalyzeConjunction((RexNode)conjunctions.get(1), leftFieldCount);
            if (first == null || second == null)
                throw new java.lang.IllegalArgumentException("condition must contain supported cross-input inequalities");

            foreach (var inequality in new[] { first, second })
            {
                if (SupportsKeyTypes(left, right, inequality) == false)
                {
                    throw new java.lang.IllegalArgumentException("unsupported IEJoin key types: left "
                        + ((RelDataTypeField)left.getRowType().getFieldList().get(inequality.LeftKey)).getType()
                        + ", right "
                        + ((RelDataTypeField)right.getRowType().getFieldList().get(inequality.RightKey)).getType());
                }
            }

            conditions = [first, second];
        }

        /// <inheritdoc />
        public override Join copy(RelTraitSet traitSet, RexNode conditionExpr, RelNode left, RelNode right, JoinRelType joinType, bool semiJoinDone)
        {
            if (joinType.name() != nameof(JoinRelType.INNER))
                throw new java.lang.IllegalArgumentException("ClrEnumerableIEJoin only supports inner joins");

            return new ClrEnumerableIEJoin(getCluster(), traitSet, left, right, conditionExpr);
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            var leftRows = mq.getRowCount(getLeft());
            var rightRows = mq.getRowCount(getRight());
            var joinRows = mq.getRowCount(this);
            if (leftRows == null || rightRows == null || joinRows == null)
                return null;

            var outputRows = joinRows.doubleValue();
            if (RelNodes.COMPARATOR.compare(getLeft(), getRight()) > 0)
                outputRows = RelMdUtil.addEpsilon(outputRows);

            var inputRows = leftRows.doubleValue() + rightRows.doubleValue();

            // the combined inputs are sorted by each inequality key, then scanned and the pairs emitted
            var cost = 2D * Util.nLogN(inputRows) + inputRows + outputRows;

            return planner.getCostFactory().makeCost(cost, 0, 0);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            System.ArgumentNullException.ThrowIfNull(implementor);
            System.ArgumentNullException.ThrowIfNull(pref);

            var leftResult = implementor.VisitChild(this, 0, (ClrEnumerableRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrEnumerableRel)getRight(), pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var arguments = Arguments(implementor, leftResult.PhysType, rightResult.PhysType, physType);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.IeJoin.MakeGenericMethod(
                        leftResult.PhysType.RowType, rightResult.PhysType.RowType, arguments.Key1Type, arguments.Key2Type, physType.RowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    arguments.LeftKeySelector1,
                    arguments.RightKeySelector1,
                    arguments.LeftKeySelector2,
                    arguments.RightKeySelector2,
                    arguments.Comparator1,
                    arguments.Comparator2,
                    arguments.Operator1,
                    arguments.Operator2,
                    arguments.Selector));
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            System.ArgumentNullException.ThrowIfNull(implementor);
            System.ArgumentNullException.ThrowIfNull(pref);

            var leftResult = implementor.VisitChildAsync(this, 0, (ClrEnumerableRel)getLeft(), pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrEnumerableRel)getRight(), pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var arguments = Arguments(implementor, leftResult.PhysType, rightResult.PhysType, physType);

            return implementor.ResultAsync(physType,
                ClrBuiltInMethod.CallAsync(
                    ClrBuiltInMethod.IeJoinAsync.MakeGenericMethod(
                        leftResult.PhysType.RowType, rightResult.PhysType.RowType, arguments.Key1Type, arguments.Key2Type, physType.RowType),
                    leftResult.Expression,
                    rightResult.Expression,
                    arguments.LeftKeySelector1,
                    arguments.RightKeySelector1,
                    arguments.LeftKeySelector2,
                    arguments.RightKeySelector2,
                    arguments.Comparator1,
                    arguments.Comparator2,
                    arguments.Operator1,
                    arguments.Operator2,
                    arguments.Selector));
        }

        /// <summary>
        /// Returns everything the operator is called with but the two sequences, which is the whole of what
        /// the two bodies share.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="leftPhysType"></param>
        /// <param name="rightPhysType"></param>
        /// <param name="physType"></param>
        /// <returns></returns>
        /// <remarks>
        /// A key is read at the type the two sides have in common, and that type is the key physical type's
        /// row type, which is boxed. Calcite leaves it unboxed and lets javac box it at <c>Function1</c>'s
        /// erased boundary; a delegate has no such boundary, and the comparator the same physical type
        /// generates takes the boxed class, so the choice is made once here. It is also what lets the scan
        /// ask whether a key is null, which is how a row with a null key is dropped.
        /// </remarks>
        CallArguments Arguments(ClrEnumerableRelImplementor implementor, ClrPhysType leftPhysType, ClrPhysType rightPhysType, ClrPhysType physType)
        {
            var typeFactory = implementor.TypeFactory;
            var left_ = Expression.Parameter(leftPhysType.RowType, "leftRow");
            var right_ = Expression.Parameter(rightPhysType.RowType, "rightRow");

            var keyTypes = new List<System.Type>();
            var keySelectors = new List<LambdaExpression>();
            var comparators = new List<Expression>();

            foreach (var condition in conditions)
            {
                var leftType = ((RelDataTypeField)getLeft().getRowType().getFieldList().get(condition.LeftKey)).getType();
                var rightType = ((RelDataTypeField)getRight().getRowType().getFieldList().get(condition.RightKey)).getType();

                // the SQL storage type, so that a timestamp is compared at millisecond precision
                var keyType = typeFactory.toSql(
                    typeFactory.leastRestrictive(com.google.common.collect.ImmutableList.of(leftType, rightType))
                        ?? throw new java.lang.NullPointerException($"leastRestrictive returns null for {leftType} and {rightType}"));

                // a comparator is generated for a row's fields, so the key is wrapped in a scalar row type
                var keyPhysType = ClrPhysTypeImpl.Of(typeFactory, typeFactory.builder().add("key", keyType).build(), JavaRowFormat.SCALAR);
                var keyClass = keyPhysType.RowType;

                keyTypes.Add(keyClass);
                keySelectors.Add(Expression.Lambda(ClrEnumUtils.Convert(leftPhysType.FieldReference(left_, condition.LeftKey), keyClass), left_));
                keySelectors.Add(Expression.Lambda(ClrEnumUtils.Convert(rightPhysType.FieldReference(right_, condition.RightKey), keyClass), right_));

                comparators.Add(
                    keyPhysType.GenerateComparator(
                        RelCollations.of(new RelFieldCollation(0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.LAST))));
            }

            return new CallArguments(
                keyTypes[0],
                keyTypes[1],
                keySelectors[0],
                keySelectors[1],
                keySelectors[2],
                keySelectors[3],
                comparators[0],
                comparators[1],
                Expression.Constant(conditions[0].Operator),
                Expression.Constant(conditions[1].Operator),
                ClrEnumUtils.JoinSelector(implementor, joinType, physType, leftPhysType, rightPhysType));
        }

        /// <summary>
        /// What <see cref="Implement"/> and <see cref="ImplementAsync"/> call their operator with, less the
        /// two sequences.
        /// </summary>
        /// <param name="Key1Type"></param>
        /// <param name="Key2Type"></param>
        /// <param name="LeftKeySelector1"></param>
        /// <param name="RightKeySelector1"></param>
        /// <param name="LeftKeySelector2"></param>
        /// <param name="RightKeySelector2"></param>
        /// <param name="Comparator1"></param>
        /// <param name="Comparator2"></param>
        /// <param name="Operator1"></param>
        /// <param name="Operator2"></param>
        /// <param name="Selector"></param>
        sealed record CallArguments(
            System.Type Key1Type,
            System.Type Key2Type,
            LambdaExpression LeftKeySelector1,
            LambdaExpression RightKeySelector1,
            LambdaExpression LeftKeySelector2,
            LambdaExpression RightKeySelector2,
            Expression Comparator1,
            Expression Comparator2,
            Expression Operator1,
            Expression Operator2,
            Expression Selector);

        /// <summary>
        /// One inequality of the condition, read left-to-right.
        /// </summary>
        /// <param name="LeftKey">The field of the left input, by its index in that input.</param>
        /// <param name="RightKey">The field of the right input, by its index in that input.</param>
        /// <param name="Operator">The comparison of the left key against the right one.</param>
        internal sealed record Condition(int LeftKey, int RightKey, ExpressionType Operator);

    }

}
