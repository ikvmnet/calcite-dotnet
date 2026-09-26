using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
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
        /// multiset, or the input must be the single column of type ANY that
        /// <see cref="IsSingleAnyColumn"/> is about.
        /// </summary>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality">whether the output carries an ORDINALITY column</param>
        /// <returns></returns>
        public static ClrEnumerableUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality)
        {
            return Create(traitSet, input, withOrdinality, true, false);
        }

        /// <summary>
        /// Creates a <see cref="ClrEnumerableUncollect"/>.
        /// </summary>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality">whether the output carries an ORDINALITY column</param>
        /// <param name="expandStructFields">whether a collection of a struct gives one column per field of
        /// it, rather than one column holding the element whole</param>
        /// <param name="isOuter">whether an empty or null collection gives one row of nulls, as a LEFT JOIN
        /// would, rather than no row at all</param>
        /// <returns></returns>
        public static ClrEnumerableUncollect Create(RelTraitSet traitSet, RelNode input, bool withOrdinality, bool expandStructFields, bool isOuter)
        {
            return new ClrEnumerableUncollect(input.getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of(), expandStructFields, isOuter);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create(RelTraitSet, RelNode, bool)"/> unless you know
        /// what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality"></param>
        /// <param name="itemAliases"></param>
        /// <remarks>
        /// <c>expandStructFields</c> is derived from the aliases being absent, which is what
        /// <see cref="Uncollect"/>'s own constructor of this arity does and why this delegates rather than
        /// naming a default: non-empty aliases historically meant the struct is not expanded.
        /// </remarks>
        public ClrEnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases) :
            this(cluster, traitSet, input, withOrdinality, itemAliases, itemAliases.isEmpty(), false)
        {

        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create(RelTraitSet, RelNode, bool, bool, bool)"/>
        /// unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="withOrdinality"></param>
        /// <param name="itemAliases"></param>
        /// <param name="expandStructFields"></param>
        /// <param name="isOuter"></param>
        public ClrEnumerableUncollect(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, bool withOrdinality, java.util.List itemAliases, bool expandStructFields, bool isOuter) :
            base(cluster, traitSet, input, withOrdinality, itemAliases, expandStructFields, isOuter)
        {

        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>EnumerableUncollect.copy</c>, which carries the two fields 1.43 added and hands on no aliases.
        /// Dropping <c>isOuter</c> would make a copy mean something the original did not — an uncollect that
        /// yields a row of nulls for an empty collection becoming one that yields no row.
        ///
        /// <para>The aliases are not carried because they cannot be: <c>Uncollect.itemAliases</c> is private
        /// and has no getter. Calcite's own node is in the same position and does the same thing, every one
        /// of its constructors passing <c>Collections.emptyList()</c>. It matters only that the flags are
        /// passed explicitly — the constructor that takes aliases instead <em>derives</em>
        /// <c>expandStructFields</c> from their absence, so going through it here would set that field from
        /// an emptiness that says nothing about the node being copied.</para>
        /// </remarks>
        public override RelNode copy(RelTraitSet traitSet, RelNode input)
        {
            return new ClrEnumerableUncollect(getCluster(), traitSet, input, withOrdinality, com.google.common.collect.ImmutableList.of(), expandStructFields, isOuter);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var fieldCounts = new java.util.ArrayList();
            var inputTypes = new java.util.ArrayList();
            org.apache.calcite.linq4j.tree.Expression? flatListForSingleItem = null;

            var fields = child.getRowType().getFieldList();

            if (IsSingleAnyColumn(fields))
            {
                // the same pair every non-struct element type emits below, which SqlFunctions.flatProduct
                // answers with LIST_AS_ENUMERABLE: it reads the run-time value as a java.util.List and takes
                // a null as the empty sequence, which is what UNNEST of a null array answers anyway
                fieldCounts.add(java.lang.Integer.valueOf(-1));
                inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
            }
            else
            {
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
                        // CALCITE-4063: one field, itself a struct of one item, and no ordinality, means the
                        // result is a scalar rather than a list of one. The outer variant answers one null
                        // scalar for an empty or null collection.
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
                        // a struct element kept whole occupies a single output column, like a scalar one, but
                        // its row value is converted out of the collection's internal list representation
                        fieldCounts.add(java.lang.Integer.valueOf(-1));
                        inputTypes.add(SqlFunctions.FlatProductInputType.STRUCT);
                    }
                    else
                    {
                        fieldCounts.add(java.lang.Integer.valueOf(-1));
                        inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
                    }
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
                    ClrBuiltInMethod.SelectMany.MakeGenericMethod(sourceType, rowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(lambda), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult ImplementAsync(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.LIST);

            var fieldCounts = new java.util.ArrayList();
            var inputTypes = new java.util.ArrayList();
            org.apache.calcite.linq4j.tree.Expression? flatListForSingleItem = null;

            var fields = child.getRowType().getFieldList();

            if (IsSingleAnyColumn(fields))
            {
                // the same pair every non-struct element type emits below, which SqlFunctions.flatProduct
                // answers with LIST_AS_ENUMERABLE: it reads the run-time value as a java.util.List and takes
                // a null as the empty sequence, which is what UNNEST of a null array answers anyway
                fieldCounts.add(java.lang.Integer.valueOf(-1));
                inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
            }
            else
            {
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
                        // CALCITE-4063: one field, itself a struct of one item, and no ordinality, means the
                        // result is a scalar rather than a list of one. The outer variant answers one null
                        // scalar for an empty or null collection.
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
                        // a struct element kept whole occupies a single output column, like a scalar one, but
                        // its row value is converted out of the collection's internal list representation
                        fieldCounts.add(java.lang.Integer.valueOf(-1));
                        inputTypes.add(SqlFunctions.FlatProductInputType.STRUCT);
                    }
                    else
                    {
                        fieldCounts.add(java.lang.Integer.valueOf(-1));
                        inputTypes.add(SqlFunctions.FlatProductInputType.SCALAR);
                    }
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
                ClrBuiltInMethod.CallAsync(ClrBuiltInMethod.SelectManyAsync.MakeGenericMethod(sourceType, rowType),
                    result.Expression,
                    ClrEnumUtils.Convert(implementor.Translator.Translate(lambda), typeof(org.apache.calcite.linq4j.function.Function1))));
        }

        /// <summary>
        /// Whether the input is the one column of type ANY that <c>Uncollect.deriveUncollectRowType</c>
        /// answers with a single ANY column of its own.
        /// </summary>
        /// <param name="fields">the input's fields</param>
        /// <returns></returns>
        /// <remarks>
        /// Not a port, and an addition rather than a defect: <c>EnumerableUncollect</c> cannot implement this
        /// shape either, and fails before a row is read — it asks
        /// <c>NonNullableAccessors.getComponentTypeOrThrow</c> for an element type an ANY has not got, which
        /// is a plan Calcite forms and then throws <c>componentType is null for ANY</c> over.
        /// <see cref="ClrAnyAggImplementors"/> is the same argument for the aggregates, and says more about
        /// why a column of type ANY is the ordinary case rather than an exotic one.
        ///
        /// <para>The test is <c>deriveUncollectRowType</c>'s own, which is what makes it exact rather than a
        /// guess: one field and ANY is the only shape that reaches the branch, because two fields of which
        /// one is ANY throws <c>unnestArgument</c> while the node is being built and never arrives here.</para>
        /// </remarks>
        static bool IsSingleAnyColumn(java.util.List fields)
        {
            return fields.size() == 1
                && ((RelDataTypeField)fields.get(0)).getType().getSqlTypeName() == SqlTypeName.ANY;
        }

    }

}
