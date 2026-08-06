using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Uncollect"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Turns each row holding a collection into a row per element, which is what UNNEST becomes.
    /// </remarks>
    public class ClrEnumerableUncollect : Uncollect, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableUncollect"/>. Each field of the input must be an array or a
        /// multiset.
        /// </summary>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality">whether the output carries an ORDINALITY column</param>
        /// <returns></returns>
        public static ClrEnumerableUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality)
        {
            return new ClrEnumerableUncollect(input.getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of());
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality"></param>
        /// <param name="itemAliases"></param>
        public ClrEnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases) :
            base(cluster, traitSet, input, withOrdinality, itemAliases)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrEnumerableUncollect(getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of());
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var fieldCounts = new java.util.ArrayList();
            var inputTypes = new java.util.ArrayList();
            org.apache.calcite.linq4j.tree.Expression? flatListForSingleItem = null;

            var fields = child.getRowType().getFieldList();
            for (int i = 0; i < fields.size(); i++)
            {
                var type = ((RelDataTypeField)fields.get(i)).getType();

                if (type is MapSqlType)
                {
                    fieldCounts.add(java.lang.Integer.valueOf(2));
                    inputTypes.add(SqlFunctions.FlatProductInputType.MAP);
                    continue;
                }

                var elementType = org.apache.calcite.sql.type.NonNullableAccessors.getComponentTypeOrThrow(type);
                if (elementType.isStruct() == false)
                {
                    fieldCounts.add(java.lang.Integer.valueOf(-1));
                    inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
                    continue;
                }

                // CALCITE-4063: one field, itself a struct of one item, and no ordinality, means the result is
                // a scalar rather than a list of one
                if (elementType.getFieldCount() == 1 && fields.size() == 1 && withOrdinality == false)
                    flatListForSingleItem = org.apache.calcite.linq4j.tree.Expressions.call(BuiltInMethod.FLAT_LIST.method);
                else
                {
                    fieldCounts.add(java.lang.Integer.valueOf(elementType.getFieldCount()));
                    inputTypes.add(SqlFunctions.FlatProductInputType.LIST);
                }
            }

            var counts = new int[fieldCounts.size()];
            for (int i = 0; i < counts.Length; i++)
                counts[i] = ((java.lang.Integer)fieldCounts.get(i)).intValue();

            var types = new SqlFunctions.FlatProductInputType[inputTypes.size()];
            for (int i = 0; i < types.Length; i++)
                types[i] = (SqlFunctions.FlatProductInputType)inputTypes.get(i);

            var lambda = flatListForSingleItem
                ?? org.apache.calcite.linq4j.tree.Expressions.call(
                    BuiltInMethod.FLAT_PRODUCT.method,
                    org.apache.calcite.linq4j.tree.Expressions.constant(counts),
                    org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(withOrdinality)),
                    org.apache.calcite.linq4j.tree.Expressions.constant(types));

            var sourceType = ClrTypes.Resolve(result.PhysType.getJavaRowType());
            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.SelectMany.MakeGenericMethod(sourceType, rowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(lambda), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

    }

}
