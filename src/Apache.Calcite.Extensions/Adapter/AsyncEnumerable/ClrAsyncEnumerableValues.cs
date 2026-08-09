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

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Values"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrAsyncEnumerableValues : Values, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableValues"/>.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="rowType"></param>
        /// <param name="tuples"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableValues Create(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples)
        {
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.values(mq, rowType, tuples)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.values(rowType, tuples)));

            return new ClrAsyncEnumerableValues(cluster, rowType, tuples, traitSet);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="rowType"></param>
        /// <param name="tuples"></param>
        /// <param name="traitSet"></param>
        public ClrAsyncEnumerableValues(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples, RelTraitSet traitSet) :
            base(cluster, rowType, tuples, traitSet)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableValues(getCluster(), getRowType(), tuples, traitSet);
        }

        /// <inheritdoc />
        public RelNode? passThrough(RelTraitSet required)
        {
            var collation = required.getCollation();
            if (collation == null || collation.isDefault())
                return null;

            // a VALUES of 0 or 1 rows can be ordered by any collation
            if (tuples.size() > 1)
            {
                com.google.common.collect.Ordering? ordering = null;

                // generate an ordering comparator according to the required collations
                for (int i = 0; i < collation.getFieldCollations().size(); i++)
                {
                    var comparator = org.apache.calcite.rel.metadata.RelMdCollation.comparator((RelFieldCollation)collation.getFieldCollations().get(i));
                    ordering = ordering == null ? comparator : ordering.compound(comparator);
                }

                // check whether the tuples are sorted by the required collations
                if (ordering!.isOrdered(tuples) == false)
                    return null;
            }

            // the tuples' order satisfies the collation, so a node carrying it is all that is needed
            return copy(getTraitSet().replace(collation), com.google.common.collect.ImmutableList.of());
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.PROHIBITED;
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferCustom());
            // Calcite boxes here too, EnumerableValues building its array with Primitive.box(rowClass)
            var rowType = physType.RowType;

            var fields = getRowType().getFieldList();
            var rows = new List<Expression>();

            for (int i = 0; i < tuples.size(); i++)
            {
                var tuple = (java.util.List)tuples.get(i);
                var literals = new List<Expression>(tuple.size());

                // Rex produces a literal in linq4j, so each is translated as it is produced rather than
                // composed into a row first
                for (int j = 0; j < tuple.size(); j++)
                    literals.Add(
                        implementor.Translator.Translate(
                            RexToLixTranslator.translateLiteral(
                                (RexLiteral)tuple.get(j),
                                ((RelDataTypeField)fields.get(j)).getType(),
                                typeFactory,
                                RexImpTable.NullAs.NULL)));

                // a null literal translates to a constant of Object, and Java may assign that to a field of
                // any reference type where an array initializer may not
                rows.Add(ClrEnumUtils.Convert(physType.Record(literals), rowType));
            }

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.AsEnumerable.MakeGenericMethod(rowType),
                    Expression.NewArrayInit(rowType, rows)));
        }

    }

}
