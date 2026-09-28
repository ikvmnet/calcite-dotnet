using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Values"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableValues</c>. The rows are built as an array of the boxed row type, as
    /// <c>EnumerableValues</c> does.
    /// </remarks>
    public class ClrCursorValues : Values, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorValues"/>, deriving its collation and distribution from the tuples.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="rowType">The row type.</param>
        /// <param name="tuples">The rows, each a list of <see cref="RexLiteral"/>.</param>
        /// <returns>The new values node.</returns>
        public static ClrCursorValues Create(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples)
        {
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.values(mq, rowType, tuples)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.values(rowType, tuples)));

            return new ClrCursorValues(cluster, rowType, tuples, traitSet);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set; this constructor takes it as
        /// given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="rowType">The row type.</param>
        /// <param name="tuples">The rows, each a list of <see cref="RexLiteral"/>.</param>
        /// <param name="traitSet">The node's traits.</param>
        public ClrCursorValues(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples, RelTraitSet traitSet) :
            base(cluster, rowType, tuples, traitSet)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorValues(getCluster(), getRowType(), tuples, traitSet);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors <c>EnumerableValues.passThrough</c>: returns a copy carrying the required collation if the
        /// tuples are already in that order, and <see langword="null"/> otherwise.
        /// </remarks>
        public RelNode? passThrough(RelTraitSet required)
        {
            var collation = required.getCollation();
            if (collation == null || collation.isDefault())
                return null;

            // zero or one rows satisfy any collation
            if (tuples.size() > 1)
            {
                com.google.common.collect.Ordering? ordering = null;

                for (int i = 0; i < collation.getFieldCollations().size(); i++)
                {
                    var comparator = org.apache.calcite.rel.metadata.RelMdCollation.comparator((RelFieldCollation)collation.getFieldCollations().get(i));
                    ordering = ordering == null ? comparator : ordering.compound(comparator);
                }

                if (ordering!.isOrdered(tuples) == false)
                    return null;
            }

            return copy(getTraitSet().replace(collation), com.google.common.collect.ImmutableList.of());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.PROHIBITED;
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferCustom());
            // boxed, as EnumerableValues builds its array with Primitive.box(rowClass)
            var rowType = physType.RowType;

            var fields = getRowType().getFieldList();
            var rows = new List<Expression>();

            for (int i = 0; i < tuples.size(); i++)
            {
                var tuple = (java.util.List)tuples.get(i);
                var literals = new List<Expression>(tuple.size());

                // each literal is translated from linq4j as it is produced, not composed into a row first
                for (int j = 0; j < tuple.size(); j++)
                    literals.Add(
                        implementor.Translator.Translate(
                            RexToLixTranslator.translateLiteral(
                                (RexLiteral)tuple.get(j),
                                ((RelDataTypeField)fields.get(j)).getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL)));

                // a null literal translates to an Object constant, which Java assigns to any reference type
                // but an expression tree's array initializer does not, so each row is converted
                rows.Add(ClrEnumUtils.Convert(physType.Record(literals), rowType));
            }

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.AsCursorArray.MakeGenericMethod(rowType),
                    Expression.NewArrayInit(rowType, rows)));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferCustom());
            // boxed, as EnumerableValues builds its array with Primitive.box(rowClass)
            var rowType = physType.RowType;

            var fields = getRowType().getFieldList();
            var rows = new List<Expression>();

            for (int i = 0; i < tuples.size(); i++)
            {
                var tuple = (java.util.List)tuples.get(i);
                var literals = new List<Expression>(tuple.size());

                // each literal is translated from linq4j as it is produced, not composed into a row first
                for (int j = 0; j < tuple.size(); j++)
                    literals.Add(
                        implementor.Translator.Translate(
                            RexToLixTranslator.translateLiteral(
                                (RexLiteral)tuple.get(j),
                                ((RelDataTypeField)fields.get(j)).getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL)));

                // a null literal translates to an Object constant, which Java assigns to any reference type
                // but an expression tree's array initializer does not, so each row is converted
                rows.Add(ClrEnumUtils.Convert(physType.Record(literals), rowType));
            }

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.AsCursorArrayAsync.MakeGenericMethod(rowType),
                    Expression.NewArrayInit(rowType, rows)));
        }

    }

}
