using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.interpreter;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="TableScan"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// The table's own expression is a linq4j tree yielding a linq4j <c>Enumerable</c>, and stays one: how a
    /// table produces its rows is the table's business and none of this convention's. Only the last hop is
    /// ours, reading that sequence as an <see cref="System.Collections.Generic.IEnumerable{T}"/>. The rows are
    /// not touched.
    /// </remarks>
    public class ClrEnumerableTableScan : TableScan, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableTableScan"/>.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="relOptTable"></param>
        /// <returns></returns>
        public static ClrEnumerableTableScan Create(RelOptCluster cluster, RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            var elementType = EnumerableTableScan.deduceElementType(table);
            var traitSet = cluster.traitSetOf(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => table != null ? table.getStatistic().getCollations() : com.google.common.collect.ImmutableList.of()));

            return new ClrEnumerableTableScan(cluster, traitSet, relOptTable, elementType);
        }

        /// <summary>
        /// Returns whether this convention can produce rows for a particular variant of the table SPI.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static bool CanHandle(Table table)
        {
            // CALCITE-3673: a TransientTable has no expression, so no plan of this convention can read one
            if (table is TransientTable)
                return false;

            // see org.apache.calcite.prepare.RelOptTableImpl.getClassExpressionFunction
            return table is QueryableTable
                || table is FilterableTable
                || table is ProjectableFilterableTable
                || table is ScannableTable;
        }

        /// <summary>
        /// Returns whether this convention can produce rows for a particular variant of the table SPI.
        /// </summary>
        /// <param name="relOptTable"></param>
        /// <returns></returns>
        public static bool CanHandle(RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            if (table != null && CanHandle(table) == false)
                return false;

            var supportArray = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_ARRAY.value()).booleanValue();
            var supportMap = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MAP.value()).booleanValue();
            var supportMultiset = ((java.lang.Boolean)org.apache.calcite.config.CalciteSystemProperty.ENUMERABLE_ENABLE_TABLESCAN_MULTISET.value()).booleanValue();
            if (supportArray && supportMap && supportMultiset)
                return true;

            // struct fields are not supported
            for (int i = 0; i < relOptTable.getRowType().getFieldList().size(); i++)
            {
                var field = (RelDataTypeField)relOptTable.getRowType().getFieldList().get(i);
                var unsupportedType = field.getType().getSqlTypeName().name() switch
                {
                    nameof(SqlTypeName.ARRAY) => supportArray,
                    nameof(SqlTypeName.MAP) => supportMap,
                    nameof(SqlTypeName.MULTISET) => supportMultiset,
                    _ => false,
                };

                if (unsupportedType)
                    return false;
            }

            return true;
        }

        /// <summary>
        /// Returns the row format a table's element type implies.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        public static JavaRowFormat DeduceFormat(RelOptTable table)
        {
            var elementType = EnumerableTableScan.deduceElementType((Table)table.unwrapOrThrow(typeof(Table)));

            return elementType == (java.lang.Class)typeof(object[]) ? JavaRowFormat.ARRAY : JavaRowFormat.CUSTOM;
        }

        readonly java.lang.Class elementType;

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="table"></param>
        /// <param name="elementType"></param>
        public ClrEnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, java.lang.Class elementType) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), table)
        {
            if (getConvention() is not ClrEnumerableConvention)
                throw new java.lang.AssertionError();
            if (CanHandle(table) == false)
                throw new java.lang.AssertionError($"ClrEnumerableTableScan can't implement {table}, see ClrEnumerableTableScan.CanHandle");

            this.elementType = elementType;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableTableScan(getCluster(), traitSet, table, elementType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Where a table had an index on the required collation keys this is where an index scan would be
        /// returned. There is none, and Calcite's own answer here is the same null.
        /// </remarks>
        public RelNode passThrough(RelTraitSet required)
        {
            return null!;
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.PROHIBITED;
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), Format());

            // the only linq4j here is the table's own expression, which the schema SPI defines as one, and the
            // row shape below. It is translated as soon as it is in hand; what a sequence is made to do after
            // that is this convention's, and is built as this convention builds everything.
            var expression = table.getExpression(typeof(Queryable))
                ?? throw new java.lang.IllegalStateException($"Unable to implement {RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: {table}.getExpression(Queryable.class) returned null");

            var source = ToEnumerable(implementor.Translator.Translate(expression));
            var element = TypeResolver.FromClass(elementType);

            return implementor.Result(physType,
                ToRows(implementor, physType, Expression.Call(null, ClrBuiltInMethod.FromJava.MakeGenericMethod(element), source), element));
        }

        /// <summary>
        /// Brings whatever the table's expression yields to a linq4j <see cref="Enumerable"/>.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static Expression ToEnumerable(Expression expression)
        {
            var type = expression.Type;

            if (type.IsArray)
            {
                if (type.GetElementType()!.IsValueType)
                    expression = Expression.Call(null, AsList, expression);

                return Expression.Call(null, AsEnumerable, expression);
            }

            if (typeof(java.lang.Iterable).IsAssignableFrom(type) && typeof(Enumerable).IsAssignableFrom(type) == false)
                return Expression.Call(null, AsEnumerable2, expression);

            // Queryable extends Enumerable but is too clever, so asEnumerable makes take(int) evaluate directly
            if (typeof(Queryable).IsAssignableFrom(type))
                return Expression.Call(expression, QueryableAsEnumerable);

            return expression;
        }

        static readonly System.Reflection.MethodInfo AsList = MethodResolver.Resolve(BuiltInMethod.AS_LIST.method);
        static readonly System.Reflection.MethodInfo AsEnumerable = MethodResolver.Resolve(BuiltInMethod.AS_ENUMERABLE.method);
        static readonly System.Reflection.MethodInfo AsEnumerable2 = MethodResolver.Resolve(BuiltInMethod.AS_ENUMERABLE2.method);
        static readonly System.Reflection.MethodInfo QueryableAsEnumerable = MethodResolver.Resolve(BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);

        /// <summary>
        /// Brings the table's rows into the physical type asked for.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <param name="element"></param>
        /// <returns></returns>
        Expression ToRows(ClrEnumerableRelImplementor implementor, PhysType physType, Expression source, Type element)
        {
            if (physType.getFormat() == JavaRowFormat.SCALAR
                && ((java.lang.Class)typeof(object[])).isAssignableFrom(elementType)
                && getRowType().getFieldCount() == 1
                && (table.unwrap(typeof(ScannableTable)) != null
                    || table.unwrap(typeof(FilterableTable)) != null
                    || table.unwrap(typeof(ProjectableFilterableTable)) != null))
                return Expression.Call(null, ClrBuiltInMethod.Slice0, source);

            var oldFormat = Format();
            if (physType.getFormat() == oldFormat && HasCollectionField(getRowType()) == false)
                return source;

            // the row shape is PhysType's, and record takes linq4j, so the field expressions are linq4j too.
            // That is the whole of it: one call feeding another, translated the moment it is built.
            var row = J.Expressions.parameter(elementType, "row");
            var parameter = Expression.Parameter(element, "row");
            implementor.Translator.Bind(row, parameter);

            var fieldCount = table.getRowType().getFieldCount();
            var expressionList = new java.util.ArrayList(fieldCount);
            for (int i = 0; i < fieldCount; i++)
                expressionList.add(FieldExpression(row, i, physType, oldFormat));

            var rowType = TypeResolver.Resolve(physType.getJavaRowType());
            var selector = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(element, rowType),
                implementor.Translator.Translate(physType.record(expressionList)),
                parameter);

            return Expression.Call(null, ClrBuiltInMethod.Select.MakeGenericMethod(element, rowType), source, selector);
        }

        /// <summary>
        /// Returns the expression reading one field of a row of the table.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="i"></param>
        /// <param name="physType"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        J.Expression FieldExpression(J.ParameterExpression row, int i, PhysType physType, JavaRowFormat format)
        {
            var e = format.field(row, i, null, physType.getJavaFieldType(i));
            var relFieldType = ((RelDataTypeField)physType.getRowType().getFieldList().get(i)).getType();

            switch (relFieldType.getSqlTypeName().name())
            {
                case nameof(SqlTypeName.ARRAY):
                case nameof(SqlTypeName.MULTISET):
                    var fieldType = relFieldType.getComponentType()
                        ?? throw new java.lang.IllegalStateException($"relFieldType.getComponentType() for {relFieldType}");

                    if (fieldType.isStruct() == false)
                        return e;

                    // a multiset or an array cannot be a List<Employee>, because the consumer does not know the
                    // element type, so the standard element type is List and this becomes a List<List>
                    var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
                    var elementPhysType = PhysTypeImpl.of(typeFactory, fieldType, JavaRowFormat.CUSTOM);
                    var e2 = J.Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, e);
                    var e3 = elementPhysType.convertTo(e2, JavaRowFormat.LIST);
                    return J.Expressions.call(e3, BuiltInMethod.ENUMERABLE_TO_LIST.method);

                default:
                    return e;
            }
        }

        /// <summary>
        /// Returns how a row of this table is represented.
        /// </summary>
        /// <returns></returns>
        JavaRowFormat Format()
        {
            var fieldCount = getRowType().getFieldCount();
            if (fieldCount == 0)
                return JavaRowFormat.LIST;

            if (((java.lang.Class)typeof(object[])).isAssignableFrom(elementType))
                return fieldCount == 1 ? JavaRowFormat.SCALAR : JavaRowFormat.ARRAY;

            if (((java.lang.Class)typeof(Row)).isAssignableFrom(elementType))
                return JavaRowFormat.ROW;

            if (fieldCount == 1
                && ((java.lang.Class)typeof(java.lang.Object) == elementType
                    || J.Primitive.@is(elementType)
                    || ((java.lang.Class)typeof(java.lang.Number)).isAssignableFrom(elementType)
                    || (java.lang.Class)typeof(java.lang.String) == elementType))
                return JavaRowFormat.SCALAR;

            return JavaRowFormat.CUSTOM;
        }

        /// <summary>
        /// Returns whether any field of a row type is an array or a multiset.
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        static bool HasCollectionField(RelDataType rowType)
        {
            var fields = rowType.getFieldList();
            for (int i = 0; i < fields.size(); i++)
            {
                switch (((RelDataTypeField)fields.get(i)).getType().getSqlTypeName().name())
                {
                    case nameof(SqlTypeName.ARRAY):
                    case nameof(SqlTypeName.MULTISET):
                        return true;
                }
            }

            return false;
        }

    }

}
