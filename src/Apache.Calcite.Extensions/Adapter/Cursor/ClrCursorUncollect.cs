using System.Linq.Expressions;


using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Uncollect"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableUncollect</c>, which implements <c>UNNEST</c>: each input row produces a row per
    /// element of its collection fields, carrying its pass-through fields unchanged. Like
    /// <c>EnumerableUncollect</c>, it fails to implement an input of a single <c>ANY</c> column, which has no
    /// component type.
    /// </remarks>
    public class ClrCursorUncollect : Uncollect, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorUncollect"/> that unnests every input field, expands struct elements
        /// into their fields, and produces no row for an empty collection.
        /// </summary>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input, whose every field is an array, multiset or map.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <returns>The new uncollect.</returns>
        public static ClrCursorUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality)
        {
            return Create(traitSet, input, withOrdinality, true, false);
        }

        /// <summary>
        /// Creates a <see cref="ClrCursorUncollect"/> that unnests every input field.
        /// </summary>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input, whose every field is an array, multiset or map.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <param name="expandStructFields">Whether a struct element produces a column per field, rather than
        /// one column holding the element.</param>
        /// <param name="isOuter">Whether an empty or null collection produces one row of nulls rather than no
        /// row.</param>
        /// <returns>The new uncollect.</returns>
        public static ClrCursorUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality, bool expandStructFields, bool isOuter)
        {
            return Create(traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of(), ImmutableBitSet.of(), ImmutableBitSet.range(input.getRowType().getFieldCount()), expandStructFields, isOuter);
        }

        /// <summary>
        /// Creates a <see cref="ClrCursorUncollect"/>.
        /// </summary>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <param name="itemAliases">Aliases for the output items, a list of strings, or an empty list.</param>
        /// <param name="passthroughFieldIndices">The input fields carried to the output unchanged.</param>
        /// <param name="collectionFieldIndices">The input fields whose collections are unnested.</param>
        /// <param name="expandStructFields">Whether a struct element produces a column per field, rather than
        /// one column holding the element.</param>
        /// <param name="isOuter">Whether an empty or null collection produces one row of nulls rather than no
        /// row.</param>
        /// <returns>The new uncollect.</returns>
        public static ClrCursorUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases, ImmutableBitSet passthroughFieldIndices, ImmutableBitSet collectionFieldIndices, bool expandStructFields, bool isOuter)
        {
            return new ClrCursorUncollect(input.getCluster(), traitSet, input, withOrdinality, itemAliases, passthroughFieldIndices, collectionFieldIndices, expandStructFields, isOuter);
        }

        /// <summary>
        /// Initializes a new instance that unnests every input field, whose struct elements are expanded only if
        /// <paramref name="itemAliases"/> is empty, as with <see cref="Uncollect"/>'s constructor of the same
        /// arity, and which is not outer.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <param name="itemAliases">Aliases for the output items, a list of strings, or an empty list.</param>
        public ClrCursorUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases) :
            this(cluster, traitSet, input, withOrdinality, itemAliases, ImmutableBitSet.of(), ImmutableBitSet.range(input.getRowType().getFieldCount()), itemAliases.isEmpty(), false)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <param name="itemAliases">Aliases for the output items, a list of strings, or an empty list.</param>
        /// <param name="passthroughFieldIndices">The input fields carried to the output unchanged.</param>
        /// <param name="collectionFieldIndices">The input fields whose collections are unnested.</param>
        /// <param name="expandStructFields">Whether a struct element produces a column per field, rather than
        /// one column holding the element.</param>
        /// <param name="isOuter">Whether an empty or null collection produces one row of nulls rather than no
        /// row.</param>
        public ClrCursorUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases, ImmutableBitSet passthroughFieldIndices, ImmutableBitSet collectionFieldIndices, bool expandStructFields, bool isOuter) :
            base(cluster, traitSet, input, withOrdinality, itemAliases, passthroughFieldIndices, collectionFieldIndices, expandStructFields, isOuter)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrCursorUncollect(getCluster(), traitSet, input, withOrdinality, getItemAliases(), getPassthroughFieldIndices(), getCollectionFieldIndices(), expandStructFields, isOuter);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.SelectMany.MakeGenericMethod(result.PhysType.RowType, physType.RowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(UncollectFunction()), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SelectManyAsync.MakeGenericMethod(result.PhysType.RowType, physType.RowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(UncollectFunction()), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

        /// <summary>
        /// Returns the call to <c>SqlFunctions.flatUncollect</c> that maps an input row to its output rows, built
        /// as <c>EnumerableUncollect</c> builds it.
        /// </summary>
        org.apache.calcite.linq4j.tree.Expression UncollectFunction()
        {
            var passthroughIndices = getPassthroughFieldIndices();
            var collectionIndices = getCollectionFieldIndices();
            var fields = getInput().getRowType().getFieldList();

            var counts = new int[collectionIndices.cardinality()];
            var types = new SqlFunctions.FlatProductInputType[counts.Length];

            var n = 0;
            for (var i = collectionIndices.nextSetBit(0); i >= 0; i = collectionIndices.nextSetBit(i + 1), n++)
            {
                var type = ((RelDataTypeField)fields.get(i)).getType();

                if (type is MapSqlType)
                {
                    counts[n] = 2;
                    types[n] = SqlFunctions.FlatProductInputType.MAP;
                    continue;
                }

                var elementType = NonNullableAccessors.getComponentTypeOrThrow(type);
                if (elementType.isStruct() && expandStructFields)
                {
                    counts[n] = elementType.getFieldCount();
                    types[n] = SqlFunctions.FlatProductInputType.LIST;
                }
                else if (elementType.isStruct())
                {
                    // a struct element kept whole occupies one output column, like a scalar, but is
                    // converted from the collection's internal list representation
                    counts[n] = -1;
                    types[n] = SqlFunctions.FlatProductInputType.STRUCT;
                }
                else
                {
                    counts[n] = -1;
                    types[n] = SqlFunctions.FlatProductInputType.SCALAR;
                }
            }

            return org.apache.calcite.linq4j.tree.Expressions.call(
                BuiltInMethod.FLAT_UNCOLLECT.method,
                org.apache.calcite.linq4j.tree.Expressions.constant(passthroughIndices.toArray()),
                org.apache.calcite.linq4j.tree.Expressions.constant(collectionIndices.toArray()),
                org.apache.calcite.linq4j.tree.Expressions.constant(counts),
                org.apache.calcite.linq4j.tree.Expressions.constant(types),
                org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(withOrdinality)),
                org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Integer.valueOf(fields.size())),
                org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(isOuter)));
        }

    }

}
