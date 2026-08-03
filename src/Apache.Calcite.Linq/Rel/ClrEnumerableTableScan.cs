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
            this.elementType = elementType;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableTableScan(getCluster(), traitSet, table, elementType);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var physType = PhysTypeImpl.of(implementor.TypeFactory, getRowType(), Format());
            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            // everything up to here is Calcite's, in linq4j, because how a table produces rows is the table's
            var expression = GetExpression(physType);
            var translated = JavaCast.To(implementor.Translator.Translate(expression), typeof(Enumerable));

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType), translated));
        }

        /// <summary>
        /// Returns the linq4j expression yielding the table's rows, in the physical type asked for.
        /// </summary>
        /// <param name="physType"></param>
        /// <returns></returns>
        J.Expression GetExpression(PhysType physType)
        {
            var expression = table.getExpression(typeof(Queryable));
            if (expression == null)
                throw new java.lang.IllegalStateException($"Unable to implement {RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: {table}.getExpression(Queryable.class) returned null");

            return ToRows(physType, ToEnumerable(expression));
        }

        /// <summary>
        /// Brings whatever the table's expression yields to a linq4j <see cref="Enumerable"/>.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static J.Expression ToEnumerable(J.Expression expression)
        {
            var type = expression.getType();

            if (J.Types.isArray(type))
            {
                var component = J.Types.toClass(type).getComponentType();
                if (component != null && component.isPrimitive())
                    expression = J.Expressions.call(BuiltInMethod.AS_LIST.method, expression);

                return J.Expressions.call(BuiltInMethod.AS_ENUMERABLE.method, expression);
            }

            if (J.Types.isAssignableFrom((java.lang.Class)typeof(java.lang.Iterable), type) && J.Types.isAssignableFrom((java.lang.Class)typeof(Enumerable), type) == false)
                return J.Expressions.call(BuiltInMethod.AS_ENUMERABLE2.method, expression);

            // Queryable extends Enumerable but is too clever, so asEnumerable makes take(int) evaluate directly
            if (J.Types.isAssignableFrom((java.lang.Class)typeof(Queryable), type))
                return J.Expressions.call(expression, BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);

            return expression;
        }

        /// <summary>
        /// Brings the table's rows into the physical type asked for.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        J.Expression ToRows(PhysType physType, J.Expression expression)
        {
            if (physType.getFormat() == JavaRowFormat.SCALAR
                && ((java.lang.Class)typeof(object[])).isAssignableFrom(elementType)
                && getRowType().getFieldCount() == 1
                && (table.unwrap(typeof(ScannableTable)) != null
                    || table.unwrap(typeof(FilterableTable)) != null
                    || table.unwrap(typeof(ProjectableFilterableTable)) != null))
                return J.Expressions.call(BuiltInMethod.SLICE0.method, expression);

            var oldFormat = Format();
            if (physType.getFormat() == oldFormat && HasCollectionField(getRowType()) == false)
                return expression;

            var row = J.Expressions.parameter(elementType, "row");
            var fieldCount = table.getRowType().getFieldCount();
            var expressionList = new java.util.ArrayList(fieldCount);
            for (int i = 0; i < fieldCount; i++)
                expressionList.add(FieldExpression(row, i, physType, oldFormat));

            return J.Expressions.call(expression,
                BuiltInMethod.SELECT.method,
                J.Expressions.lambda((java.lang.Class)typeof(org.apache.calcite.linq4j.function.Function1), physType.record(expressionList), row));
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
