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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of an inner <see cref="Join"/> of two inequality predicates in the
    /// <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableIEJoin</c>. The condition is exactly two conjunctions, each an inequality between a
    /// field of the left input and a field of the right; <see cref="ClrCursorIEJoinRule"/> puts any further
    /// conjunctions in a calc above the join.
    ///
    /// <para>The algorithm, in <see cref="ClrCursorDefaults.IeJoin"/>, is from Khayyat et al., "Lightning Fast
    /// and Space Efficient Inequality Joins", PVLDB 8(13), 2015.</para>
    /// </remarks>
    public class ClrCursorIEJoin : Join, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorIEJoin"/>.
        /// </summary>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The two inequalities, as a conjunction.</param>
        /// <returns>The new node.</returns>
        /// <exception cref="java.lang.IllegalArgumentException">The condition is not two supported
        /// cross-input inequalities.</exception>
        public static ClrCursorIEJoin Create(RelNode left, RelNode right, RexNode condition)
        {
            System.ArgumentNullException.ThrowIfNull(left);

            return new ClrCursorIEJoin(
                left.getCluster(),
                left.getCluster().traitSetOf(ClrCursorConvention.Instance),
                left,
                right,
                condition);
        }

        /// <summary>
        /// Returns a conjunction that compares a field of the left input with a field of the right by
        /// <c>&lt;</c>, <c>&lt;=</c>, <c>&gt;</c> or <c>&gt;=</c>, normalized to read left to right; or
        /// <see langword="null"/> for any other expression.
        /// </summary>
        /// <param name="node">The conjunction.</param>
        /// <param name="leftFieldCount">The number of fields of the left input.</param>
        /// <returns>The normalized inequality, or <see langword="null"/>.</returns>
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

            // both operands from one input cannot drive the join
            if (firstIsLeft == secondIsLeft)
                return null;

            // normalized to read left to right, so a comparison written right-first is reversed
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
        /// Returns whether the two fields an inequality compares can be ordered by a single comparator: they
        /// have the same type, ignoring nullability, and it is boolean, signed exact numeric, character,
        /// binary, datetime or interval.
        /// </summary>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The inequality.</param>
        /// <returns>True if the key types are supported.</returns>
        internal static bool SupportsKeyTypes(RelNode left, RelNode right, Condition condition)
        {
            var leftType = ((RelDataTypeField)left.getRowType().getFieldList().get(condition.LeftKey)).getType();
            var rightType = ((RelDataTypeField)right.getRowType().getFieldList().get(condition.RightKey)).getType();
            var typeName = leftType.getSqlTypeName();

            // floating point is excluded: its sort order disagrees with <, <=, > and >= for NaN and signed zero
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
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it supplies the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="condition">The two inequalities, as a conjunction.</param>
        /// <exception cref="java.lang.IllegalArgumentException">The condition is not two supported
        /// cross-input inequalities.</exception>
        public ClrCursorIEJoin(RelOptCluster cluster, RelTraitSet traitSet, RelNode left, RelNode right, RexNode condition) :
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
                throw new java.lang.IllegalArgumentException("ClrCursorIEJoin only supports inner joins");

            return new ClrCursorIEJoin(getCluster(), traitSet, left, right, conditionExpr);
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

            // the combined inputs are sorted once per inequality key, then scanned, and the pairs emitted
            var cost = 2D * Util.nLogN(inputRows) + inputRows + outputRows;

            return planner.getCostFactory().makeCost(cost, 0, 0);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            System.ArgumentNullException.ThrowIfNull(implementor);
            System.ArgumentNullException.ThrowIfNull(pref);

            var leftResult = implementor.VisitChild(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChild(this, 1, (ClrCursorRel)getRight(), pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var arguments = Arguments(implementor, leftResult.PhysType, rightResult.PhysType, physType);

            // the right input is passed as an opener and acquired only once the left has been drained and
            // closed, as in linq4j's IEJoinEnumerator
            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.IeJoin.MakeGenericMethod(
                        leftResult.PhysType.RowType, rightResult.PhysType.RowType, arguments.Key1Type, arguments.Key2Type, physType.RowType),
                    leftResult.Expression,
                    implementor.Opener(rightResult),
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
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            System.ArgumentNullException.ThrowIfNull(implementor);
            System.ArgumentNullException.ThrowIfNull(pref);

            var leftResult = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getLeft(), pref);
            var rightResult = implementor.VisitChildAsync(this, 1, (ClrCursorRel)getRight(), pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var arguments = Arguments(implementor, leftResult.PhysType, rightResult.PhysType, physType);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor,
                    ClrCursorBuiltInMethod.IeJoinAsync.MakeGenericMethod(
                        leftResult.PhysType.RowType, rightResult.PhysType.RowType, arguments.Key1Type, arguments.Key2Type, physType.RowType),
                    leftResult.Expression,
                    implementor.OpenerAsync(rightResult),
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
        /// Returns the operator's arguments other than the two inputs, which both bodies share.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="leftPhysType">The left input's physical type.</param>
        /// <param name="rightPhysType">The right input's physical type.</param>
        /// <param name="physType">The output's physical type.</param>
        /// <returns>The arguments.</returns>
        /// <remarks>
        /// Each key is read as the boxed row type of a one-field physical type over the two sides' common SQL
        /// type. Calcite leaves the key unboxed and relies on javac boxing it for <c>Function1</c>; a delegate is
        /// typed, and the comparator generated from that physical type takes the boxed class. A boxed key can
        /// also be null, which is how the operator drops a row with a null key.
        /// </remarks>
        CallArguments Arguments(ClrCursorRelImplementor implementor, ClrPhysType leftPhysType, ClrPhysType rightPhysType, ClrPhysType physType)
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

                // the SQL storage type, so a timestamp is compared at millisecond precision
                var keyType = typeFactory.toSql(
                    typeFactory.leastRestrictive(com.google.common.collect.ImmutableList.of(leftType, rightType))
                        ?? throw new java.lang.NullPointerException($"leastRestrictive returns null for {leftType} and {rightType}"));

                // comparators are generated over a row's fields, so the key is given a one-field scalar row type
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
        /// The arguments <see cref="Implement"/> and <see cref="ImplementAsync"/> pass their operator, other
        /// than the two inputs. Members suffixed 1 and 2 belong to the first and second inequality.
        /// </summary>
        /// <param name="Key1Type">The boxed key type of the first inequality.</param>
        /// <param name="Key2Type">The boxed key type of the second inequality.</param>
        /// <param name="LeftKeySelector1">Reads the first key from a left row.</param>
        /// <param name="RightKeySelector1">Reads the first key from a right row.</param>
        /// <param name="LeftKeySelector2">Reads the second key from a left row.</param>
        /// <param name="RightKeySelector2">Reads the second key from a right row.</param>
        /// <param name="Comparator1">Orders the first key ascending, nulls last.</param>
        /// <param name="Comparator2">Orders the second key ascending, nulls last.</param>
        /// <param name="Operator1">The first inequality's comparison, as an <see cref="ExpressionType"/> constant.</param>
        /// <param name="Operator2">The second inequality's comparison, as an <see cref="ExpressionType"/> constant.</param>
        /// <param name="Selector">Combines a left and a right row into an output row.</param>
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
        /// One inequality of the condition, read left to right.
        /// </summary>
        /// <param name="LeftKey">The field of the left input, by its index in that input.</param>
        /// <param name="RightKey">The field of the right input, by its index in that input.</param>
        /// <param name="Operator">The comparison of the left key against the right one.</param>
        internal sealed record Condition(int LeftKey, int RightKey, ExpressionType Operator);

    }

}
