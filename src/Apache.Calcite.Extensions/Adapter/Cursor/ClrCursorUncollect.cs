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
    /// element of its collections. Like <c>EnumerableUncollect</c>, it fails to implement an input of a
    /// single <c>ANY</c> column, which has no component type.
    /// </remarks>
    public class ClrCursorUncollect : Uncollect, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorUncollect"/> that expands struct elements into their fields and
        /// produces no row for an empty collection.
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
        /// Creates a <see cref="ClrCursorUncollect"/>.
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
            return new ClrCursorUncollect(input.getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of(), expandStructFields, isOuter);
        }

        /// <summary>
        /// Initializes a new instance whose struct elements are expanded only if <paramref name="itemAliases"/>
        /// is empty, as with <see cref="Uncollect"/>'s constructor of the same arity, and which is not outer.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits.</param>
        /// <param name="input">The input.</param>
        /// <param name="withOrdinality">Whether the output has an <c>ORDINALITY</c> column.</param>
        /// <param name="itemAliases">Aliases for the output items, a list of strings, or an empty list.</param>
        public ClrCursorUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases) :
            this(cluster, traitSet, input, withOrdinality, itemAliases, itemAliases.isEmpty(), false)
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
        /// <param name="expandStructFields">Whether a struct element produces a column per field, rather than
        /// one column holding the element.</param>
        /// <param name="isOuter">Whether an empty or null collection produces one row of nulls rather than no
        /// row.</param>
        public ClrCursorUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases, bool expandStructFields, bool isOuter) :
            base(cluster, traitSet, input, withOrdinality, itemAliases, expandStructFields, isOuter)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// Mirrors <c>EnumerableUncollect.copy</c>: <c>expandStructFields</c> and <c>isOuter</c> are copied, and
        /// the item aliases are not, because <c>Uncollect.itemAliases</c> is private with no getter. The flags
        /// are passed explicitly because the constructor without them derives <c>expandStructFields</c> from the
        /// aliases.
        /// </remarks>
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrCursorUncollect(getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of(), expandStructFields, isOuter);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

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
                if (elementType.isStruct() && expandStructFields)
                {
                    // as in EnumerableUncollect, a single field whose element is a struct of
                    // one field, without ordinality, yields scalars rather than one-element lists; the
                    // outer variant yields one null for an empty or null collection
                    if (elementType.getFieldCount() == 1 && fields.size() == 1 && withOrdinality == false)
                        flatListForSingleItem = org.apache.calcite.linq4j.tree.Expressions.call(
                            isOuter ? BuiltInMethod.FLAT_LIST_OUTER.method : BuiltInMethod.FLAT_LIST.method);
                    else
                    {
                        fieldCounts.add(java.lang.Integer.valueOf(elementType.getFieldCount()));
                        inputTypes.add(SqlFunctions.FlatProductInputType.LIST);
                    }
                }
                else if (elementType.isStruct())
                {
                    // a struct element kept whole occupies one output column, like a scalar, but is
                    // converted from the collection's internal list representation
                    fieldCounts.add(java.lang.Integer.valueOf(-1));
                    inputTypes.add(SqlFunctions.FlatProductInputType.STRUCT);
                }
                else
                {
                    fieldCounts.add(java.lang.Integer.valueOf(-1));
                    inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
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
                    BuiltInMethod.FLAT_ZIP.method,
                    org.apache.calcite.linq4j.tree.Expressions.constant(counts),
                    org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(withOrdinality)),
                    org.apache.calcite.linq4j.tree.Expressions.constant(types),
                    org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(isOuter)));

            var sourceType = result.PhysType.RowType;
            var rowType = physType.RowType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.SelectMany.MakeGenericMethod(sourceType, rowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(lambda), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

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
                if (elementType.isStruct() && expandStructFields)
                {
                    // as in EnumerableUncollect, a single field whose element is a struct of
                    // one field, without ordinality, yields scalars rather than one-element lists; the
                    // outer variant yields one null for an empty or null collection
                    if (elementType.getFieldCount() == 1 && fields.size() == 1 && withOrdinality == false)
                        flatListForSingleItem = org.apache.calcite.linq4j.tree.Expressions.call(
                            isOuter ? BuiltInMethod.FLAT_LIST_OUTER.method : BuiltInMethod.FLAT_LIST.method);
                    else
                    {
                        fieldCounts.add(java.lang.Integer.valueOf(elementType.getFieldCount()));
                        inputTypes.add(SqlFunctions.FlatProductInputType.LIST);
                    }
                }
                else if (elementType.isStruct())
                {
                    // a struct element kept whole occupies one output column, like a scalar, but is
                    // converted from the collection's internal list representation
                    fieldCounts.add(java.lang.Integer.valueOf(-1));
                    inputTypes.add(SqlFunctions.FlatProductInputType.STRUCT);
                }
                else
                {
                    fieldCounts.add(java.lang.Integer.valueOf(-1));
                    inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
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
                    BuiltInMethod.FLAT_ZIP.method,
                    org.apache.calcite.linq4j.tree.Expressions.constant(counts),
                    org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(withOrdinality)),
                    org.apache.calcite.linq4j.tree.Expressions.constant(types),
                    org.apache.calcite.linq4j.tree.Expressions.constant(java.lang.Boolean.valueOf(isOuter)));

            var sourceType = result.PhysType.RowType;
            var rowType = physType.RowType;

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SelectManyAsync.MakeGenericMethod(sourceType, rowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(lambda), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

    }

}
