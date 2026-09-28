using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Collect"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableCollect</c>: collects all of its input into one row holding an array, multiset or
    /// map. The input is drained when the node's cursor is opened, as Calcite's generated code calls
    /// <c>toList</c> or <c>toMap</c> before wrapping the value in <c>singletonEnumerable</c>.
    /// </remarks>
    public class ClrCursorCollect : Collect, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCollect"/>.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="rowType">The row type: one field of the collection type.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorCollect Create(RelNode input, RelDataType rowType)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorCollect(cluster, traitSet, input, rowType);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="rowType">The row type: one field of the collection type.</param>
        public ClrCursorCollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RelDataType rowType) :
            base(cluster, traitSet, input, rowType)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrCursorCollect(getCluster(), traitSet, input, getRowType());
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();

            // arrays are preferred, as in Calcite, but the child may produce another format
            var result = implementor.VisitChild(this, 0, child, ClrCursorPrefer.Array);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var collectionType = getCollectionType();
            var source = result.Expression;
            var sourceType = result.PhysType.RowType;

            Expression collection;

            switch (collectionType.name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    var componentType = ((RelDataTypeField)getRowType().getFieldList().get(0)).getType().getComponentType()
                        ?? throw new java.lang.NullPointerException();
                    var childRecordType = ((RelDataTypeField)result.PhysType.RelRowType.getFieldList().get(0)).getType();

                    if (SqlTypeUtil.sameNamedType(componentType, childRecordType) == false)
                    {
                        // as in Calcite: a multiset's elements are records, so rows become arrays; an array over
                        // a single field keeps scalar elements
                        var targetFormat = collectionType.name() == nameof(SqlTypeName.ARRAY) && child.getRowType().getFieldCount() == 1
                            ? JavaRowFormat.SCALAR
                            : JavaRowFormat.ARRAY;

                        source = result.PhysType.ConvertTo(source, targetFormat);
                        sourceType = source.Type.GetGenericArguments()[0];
                    }

                    collection = Expression.Call(null, ClrCursorBuiltInMethod.ToJavaList.MakeGenericMethod(sourceType), source);
                    break;

                case nameof(SqlTypeName.MAP):
                    // the key and value are the first two fields of each row
                    var input = Expression.Parameter(sourceType, "input");
                    var array = Expression.Convert(input, typeof(object[]));

                    collection = Expression.Call(null,
                        ClrCursorBuiltInMethod.ToJavaMap.MakeGenericMethod(sourceType),
                        source,
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(0)), input),
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(1)), input));
                    break;

                default:
                    throw new java.lang.IllegalArgumentException($"unknown collection type {collectionType}");
            }

            return implementor.Result(physType,
                Expression.Call(null, ClrCursorBuiltInMethod.Singleton.MakeGenericMethod(collection.Type), collection));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();

            // arrays are preferred, as in Calcite, but the child may produce another format
            var result = implementor.VisitChildAsync(this, 0, child, ClrCursorPrefer.Array);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var collectionType = getCollectionType();
            var source = result.Expression;
            var sourceType = result.PhysType.RowType;

            // one operator that builds the collection and yields it as a single row, where the synchronous body
            // composes two: the drain has to be awaited, which an expression tree cannot do. It still drains at
            // the open
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
                        // as in Calcite: a multiset's elements are records, so rows become arrays; an array over
                        // a single field keeps scalar elements
                        var targetFormat = collectionType.name() == nameof(SqlTypeName.ARRAY) && child.getRowType().getFieldCount() == 1
                            ? JavaRowFormat.SCALAR
                            : JavaRowFormat.ARRAY;

                        source = result.PhysType.ConvertToAsync(implementor, source, targetFormat);

                        // an awaiting open is a ValueTask of the cursor, so the row type is one generic level deeper
                        sourceType = source.Type.GetGenericArguments()[0].GetGenericArguments()[0];
                    }

                    rows = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SingletonJavaListAsync.MakeGenericMethod(sourceType), source);
                    break;

                case nameof(SqlTypeName.MAP):
                    // the key and value are the first two fields of each row
                    var input = Expression.Parameter(sourceType, "input");
                    var array = Expression.Convert(input, typeof(object[]));

                    rows = ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SingletonJavaMapAsync.MakeGenericMethod(sourceType),
                        source,
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(0)), input),
                        Expression.Lambda(typeof(Func<,>).MakeGenericType(sourceType, typeof(object)), Expression.ArrayAccess(array, Expression.Constant(1)), input));
                    break;

                default:
                    throw new java.lang.IllegalArgumentException($"unknown collection type {collectionType}");
            }

            return implementor.ResultAsync(physType, rows);
        }

    }

}
