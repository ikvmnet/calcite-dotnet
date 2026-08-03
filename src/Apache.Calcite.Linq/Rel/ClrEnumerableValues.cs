using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Values"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    public class ClrEnumerableValues : Values, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableValues"/>.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="rowType"></param>
        /// <param name="tuples"></param>
        /// <returns></returns>
        public static ClrEnumerableValues Create(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples)
        {
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.values(mq, rowType, tuples)));

            return new ClrEnumerableValues(cluster, rowType, tuples, traitSet);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="rowType"></param>
        /// <param name="tuples"></param>
        /// <param name="traitSet"></param>
        public ClrEnumerableValues(RelOptCluster cluster, RelDataType rowType, com.google.common.collect.ImmutableList tuples, RelTraitSet traitSet) :
            base(cluster, rowType, tuples, traitSet)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableValues(getCluster(), getRowType(), tuples, traitSet);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), pref.preferCustom());
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            var fields = getRowType().getFieldList();
            var rows = new List<Expression>();

            for (int i = 0; i < tuples.size(); i++)
            {
                var tuple = (java.util.List)tuples.get(i);
                var literals = new java.util.ArrayList();

                for (int j = 0; j < tuple.size(); j++)
                    literals.add(
                        RexToLixTranslator.translateLiteral(
                            (RexLiteral)tuple.get(j),
                            ((RelDataTypeField)fields.get(j)).getType(),
                            typeFactory,
                            RexImpTable.NullAs.NULL));

                rows.Add(implementor.Translator.Translate(physType.record(literals)));
            }

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.AsEnumerable.MakeGenericMethod(rowType),
                    Expression.NewArrayInit(rowType, rows)));
        }

    }

}
