using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Collect"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Turns a whole sequence into one row holding a collection, which is what a sub-query used as an array,
    /// a multiset or a map becomes.
    /// </remarks>
    public class ClrAsyncEnumerableCollect : Collect, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableCollect"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableCollect Create(RelNode input, RelDataType rowType)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSet().replace(ClrAsyncEnumerableConvention.Instance);

            return new ClrAsyncEnumerableCollect(cluster, traitSet, input, rowType);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="rowType"></param>
        public ClrAsyncEnumerableCollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType rowType) :
            base(cluster, traitSet, input, rowType)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrAsyncEnumerableCollect(getCluster(), traitSet, input, getRowType());
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrAsyncEnumerableRel)getInput();

            // rows are asked for as arrays, though as Calcite notes the child need not oblige
            var result = implementor.VisitChild(this, 0, child, ClrEnumerablePrefer.Array);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var collectionType = getCollectionType();
            var source = result.Expression;
            var sourceType = result.PhysType.RowType;

            // the sequence of one row, rather than the collection the synchronous node builds and then
            // wraps: the two steps are one operator here, because the reading has to be awaited
            Expression rows;

            switch (collectionType.name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    var componentType = ((RelDataTypeField)getRowType().getFieldList().get(0)).getType().getComponentType()
                        ?? throw new java.lang.NullPointerException();
                    var childRecordType = ((RelDataTypeField)result.PhysType.RelRowType.getFieldList().get(0)).getType();

                    if (SqlTypeUtil.sameNamedType(componentType, childRecordType) == false)
                    {
                        // every element of a multiset is a record, so a scalar is wrapped in something that can
                        // hold one; an array of a single field stays scalar so it still compares correctly
                        var targetFormat = collectionType.name() == nameof(SqlTypeName.ARRAY) && child.getRowType().getFieldCount() == 1
                            ? JavaRowFormat.SCALAR
                            : JavaRowFormat.ARRAY;

                        source = result.PhysType.ConvertToAsync(source, targetFormat);
                        sourceType = source.Type.GetGenericArguments()[0];
                    }

                    rows = ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.SingletonJavaList.MakeGenericMethod(sourceType), source);
                    break;

                case nameof(SqlTypeName.MAP):
                    // the key and the value are the first two fields of each row, and the order they arrive in
                    // is kept, so no comparer is given
                    var input = Expression.Parameter(sourceType, "input");
                    var array = Expression.Convert(input, typeof(object[]));

                    rows = ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.SingletonJavaMap.MakeGenericMethod(sourceType),
                        source,
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(0)), input),
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(1)), input));
                    break;

                default:
                    throw new java.lang.IllegalArgumentException($"unknown collection type {collectionType}");
            }

            return implementor.Result(physType, rows);
        }

    }

}
