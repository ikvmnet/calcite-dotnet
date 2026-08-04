using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Collect"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Turns a whole sequence into one row holding a collection, which is what a sub-query used as an array,
    /// a multiset or a map becomes.
    /// </remarks>
    public class ClrEnumerableCollect : Collect, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCollect"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public static ClrEnumerableCollect Create(RelNode input, RelDataType rowType)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableCollect(cluster, traitSet, input, rowType);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="rowType"></param>
        public ClrEnumerableCollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType rowType) :
            base(cluster, traitSet, input, rowType)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrEnumerableCollect(getCluster(), traitSet, input, getRowType());
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();

            // rows are asked for as arrays, though as Calcite notes the child need not oblige
            var result = implementor.VisitChild(this, 0, child, ClrEnumerablePrefer.Array);
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var collectionType = getCollectionType();
            var source = result.Expression;
            var sourceType = TypeResolver.Resolve(result.PhysType.getJavaRowType());

            Expression collection;

            switch (collectionType.name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    var componentType = getRowType().getFieldList().get(0) is RelDataTypeField field
                        ? field.getType().getComponentType()
                        : null;
                    var childRecordType = ((RelDataTypeField)result.PhysType.getRowType().getFieldList().get(0)).getType();

                    if (componentType != null && SqlTypeUtil.sameNamedType(componentType, childRecordType) == false)
                    {
                        // every element of a multiset is a record, so a scalar is wrapped in something that can
                        // hold one; an array of a single field stays scalar so it still compares correctly
                        var targetFormat = collectionType.name() == nameof(SqlTypeName.ARRAY) && child.getRowType().getFieldCount() == 1
                            ? JavaRowFormat.SCALAR
                            : JavaRowFormat.ARRAY;

                        source = ClrPhysTypes.ConvertTo(implementor, result.PhysType, source, targetFormat);
                        sourceType = source.Type.GetGenericArguments()[0];
                    }

                    collection = Expression.Call(null, ClrBuiltInMethod.ToJavaList.MakeGenericMethod(sourceType), source);
                    break;

                case nameof(SqlTypeName.MAP):
                    // the key and the value are the first two fields of each row, and the order they arrive in
                    // is kept, so no comparer is given
                    var input = Expression.Parameter(sourceType, "input");
                    var array = Expression.Convert(input, typeof(object[]));

                    collection = Expression.Call(null,
                        ClrBuiltInMethod.ToJavaMap.MakeGenericMethod(sourceType),
                        source,
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(0)), input),
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(1)), input));
                    break;

                default:
                    throw new java.lang.IllegalArgumentException($"unknown collection type {collectionType}");
            }

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.Singleton.MakeGenericMethod(collection.Type), collection));
        }

    }

}
