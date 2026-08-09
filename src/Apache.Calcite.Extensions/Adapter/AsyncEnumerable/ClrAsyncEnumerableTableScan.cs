using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Schema;

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

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="TableScan"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableTableScan"/> over an
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/>, and the same node in every other way.
    ///
    /// <para><b>A table of Calcite's is reached the way Calcite reaches one</b> —
    /// <c>getExpression(Queryable.class)</c>, translated, and read across with <c>FromJavaAsync</c> — rather
    /// than by calling <c>ScannableTable.scan</c> directly. That is the access Calcite maintains: it is what
    /// <c>RelOptTableImpl.getClassExpressionFunction</c> is for, it is what changes when Calcite changes how
    /// a table is read, and going around it means re-deriving <c>deduceElementType</c>'s precedence by hand.
    /// Doing that broke seventy tests once: a <c>ReflectiveSchema</c> table is a <c>QueryableTable</c> and a
    /// <c>ScannableTable</c> both, and calling the scan on one reads <c>Object[]</c> where its element type
    /// says <c>Employee</c>.</para>
    ///
    /// <para>What that buys is the point of it: a query over an ordinary Calcite table is one node of this
    /// convention rather than a Calcite subtree under
    /// <see cref="EnumerableToClrAsyncEnumerableConverter"/>. The converter is still there for what this
    /// convention has no node for at all.</para>
    ///
    /// <para>Nothing about reading a Calcite table suspends — a linq4j <c>Enumerable</c> is pulled — and
    /// nothing about it blocks either. What suspends is an <see cref="Schema.IClrAsyncScannableTable"/> or
    /// an <see cref="Schema.IClrAsyncQueryableTable"/>, which are this convention's own and are read
    /// directly.</para>
    /// </remarks>
    public class ClrAsyncEnumerableTableScan : TableScan, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrAsyncEnumerableTableScan"/>.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="relOptTable"></param>
        /// <returns></returns>
        public static ClrAsyncEnumerableTableScan Create(RelOptCluster cluster, RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            var elementType = DeduceElementType(table);
            var traitSet = cluster.traitSetOf(ClrAsyncEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => table != null ? table.getStatistic().getCollations() : com.google.common.collect.ImmutableList.of()));

            return new ClrAsyncEnumerableTableScan(cluster, traitSet, relOptTable, elementType);
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

            // this convention's own table SPI, which is read directly rather than through linq4j
            if (table is IClrAsyncScannableTable or IClrAsyncQueryableTable)
                return true;

            // see org.apache.calcite.prepare.RelOptTableImpl.getClassExpressionFunction
            return table is QueryableTable
                || table is FilterableTable
                || table is ProjectableFilterableTable
                || table is ScannableTable;
        }

        /// <summary>
        /// Returns the type of one row of a table.
        /// </summary>
        /// <param name="table"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableTableScan.deduceElementType</c>, with this convention's own table SPI answered first
        /// and everything else handed to Calcite's. The two new cases are the two Calcite already has, for
        /// the two interfaces that mirror them: an <see cref="IClrAsyncQueryableTable"/> names its element type
        /// as a <see cref="QueryableTable"/> does, and an <see cref="IClrAsyncScannableTable"/> yields arrays as a
        /// <see cref="ScannableTable"/> does.
        /// </remarks>
        public static java.lang.Class DeduceElementType(Table? table)
        {
            if (table is IClrAsyncQueryableTable queryable)
                return (java.lang.Class)queryable.ElementType;

            if (table is IClrAsyncScannableTable)
                return (java.lang.Class)typeof(object[]);

            return EnumerableTableScan.deduceElementType(table);
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
            var elementType = DeduceElementType((Table)table.unwrapOrThrow(typeof(Table)));

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
        public ClrAsyncEnumerableTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, java.lang.Class elementType) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), table)
        {
            if (getConvention() is not ClrAsyncEnumerableConvention)
                throw new java.lang.AssertionError();
            if (CanHandle(table) == false)
                throw new java.lang.AssertionError($"ClrAsyncEnumerableTableScan can't implement {table}, see ClrAsyncEnumerableTableScan.CanHandle");

            this.elementType = elementType;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableTableScan(getCluster(), traitSet, table, elementType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Where a table had an index on the required collation keys this is where an index scan would be
        /// returned. There is none, and Calcite's own answer here is the same null.
        /// </remarks>
        public RelNode? passThrough(RelTraitSet required)
        {
            return null;
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return DeriveMode.PROHIBITED;
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), Format());

            // the only linq4j here is the table's own expression, which the schema SPI defines as one, and the
            // row shape below. It is translated as soon as it is in hand; what a sequence is made to do after
            // that is this convention's, and is built as this convention builds everything.
            var unwrapped = (Table)table.unwrap(typeof(Table));

            // this convention's own table SPI is read directly: the rows are already a .NET sequence, so
            // there is no linq4j tree to translate and no FromJava to read one back
            if (unwrapped is IClrAsyncScannableTable or IClrAsyncQueryableTable)
                return implementor.Result(physType, ToRows(implementor, physType, ClrSource(implementor), true));

            var expression = table.getExpression(typeof(Queryable))
                ?? throw new java.lang.IllegalStateException($"Unable to implement {RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: {table}.getExpression(Queryable.class) returned null");

            var source = ToEnumerable(implementor.Translator.Translate(expression));

            return implementor.Result(physType, ToRows(implementor, physType, source, false));
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

            if (typeof(java.lang.Iterable).IsAssignableFrom(type) && typeof(org.apache.calcite.linq4j.Enumerable).IsAssignableFrom(type) == false)
                return Expression.Call(null, AsEnumerable2, expression);

            // Queryable extends Enumerable but is too clever, so asEnumerable makes take(int) evaluate directly
            if (typeof(Queryable).IsAssignableFrom(type))
                return Expression.Call(expression, QueryableAsEnumerable);

            return expression;
        }

        /// <summary>
        /// Returns the expression yielding the rows of a table of this convention's own SPI.
        /// </summary>
        /// <param name="implementor"></param>
        /// <returns></returns>
        /// <remarks>
        /// An <see cref="IClrAsyncQueryableTable"/> writes its own reading into the plan, as a
        /// <see cref="QueryableTable"/> does; an <see cref="IClrAsyncScannableTable"/> is called, as a
        /// <see cref="ScannableTable"/> is. Either way what comes back is already an
        /// <see cref="System.Collections.Generic.IEnumerable{T}"/> of the deduced element type.
        /// </remarks>
        Expression ClrSource(ClrAsyncEnumerableRelImplementor implementor)
        {
            var unwrapped = (Table)table.unwrap(typeof(Table));

            if (unwrapped is IClrAsyncQueryableTable queryable)
            {
                var names = table.getQualifiedName();

                return queryable.GetAsyncExpression(
                    ((org.apache.calcite.jdbc.CalciteSchema)table.unwrap(typeof(org.apache.calcite.jdbc.CalciteSchema)))?.plus(),
                    (string)names.get(names.size() - 1))
                    ?? throw new java.lang.IllegalStateException($"{table}.GetExpression returned null");
            }

            // reached as a constant, the way EnumerableRelImplementor.stash reaches an object a plan cannot
            // hold. An expression tree can hold one, so it is a constant rather than a stash.
            return Expression.Call(
                Expression.Constant((IClrAsyncScannableTable)unwrapped, typeof(IClrAsyncScannableTable)),
                ScanMethod,
                implementor.Root);
        }


        static readonly System.Reflection.MethodInfo ScanMethod = typeof(IClrAsyncScannableTable).GetMethod(nameof(IClrAsyncScannableTable.ScanAsync))
            ?? throw new System.InvalidOperationException($"'{nameof(IClrAsyncScannableTable.ScanAsync)}' is missing.");

        static readonly System.Reflection.MethodInfo AsList = ClrTypes.Resolve(BuiltInMethod.AS_LIST.method);
        static readonly System.Reflection.MethodInfo AsEnumerable = ClrTypes.Resolve(BuiltInMethod.AS_ENUMERABLE.method);
        static readonly System.Reflection.MethodInfo AsEnumerable2 = ClrTypes.Resolve(BuiltInMethod.AS_ENUMERABLE2.method);
        static readonly System.Reflection.MethodInfo QueryableAsEnumerable = ClrTypes.Resolve(BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);

        /// <summary>
        /// Brings the table's rows into the physical type asked for.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        Expression ToRows(ClrAsyncEnumerableRelImplementor implementor, ClrPhysType physType, Expression source, bool native)
        {
            var element = ClrTypes.FromClass(elementType);

            // a table of this convention's own SPI has already handed back a .NET sequence; one of Calcite's
            // handed back a linq4j Enumerable, which is read across the boundary. The rest is the same.
            Expression Source(System.Type rowType) => native ? source : FromJava(rowType, source);

            if (physType.Format == JavaRowFormat.SCALAR
                && ((java.lang.Class)typeof(object[])).isAssignableFrom(elementType)
                && getRowType().getFieldCount() == 1
                && (table.unwrap(typeof(ScannableTable)) != null
                    || table.unwrap(typeof(FilterableTable)) != null
                    || table.unwrap(typeof(ProjectableFilterableTable)) != null))
                return ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Slice0.MakeGenericMethod(physType.RowType),
                    Source(element));

            var oldFormat = Format();
            if (physType.Format == oldFormat && HasCollectionField(getRowType()) == false)
                // the rows are of the physical row type, which is what every reader of this sequence expects.
                // Calcite passes the table's own element type along here because a linq4j Enumerable erases
                // it; a CLR sequence does not, and the two differ wherever a format was optimized away — a
                // one column table declares Object[] and holds the value itself.
                return Source(physType.RowType);

            // the row shape is PhysType's, and one field of it can be a multiset that has to be reformatted
            // through linq4j's own select -- an Enumerable of Java's, not a sequence of this convention's. So
            // the selector is the one Calcite writes, built against their physical type and translated whole.
            var calcite = PhysTypeImpl.of(implementor.TypeFactory, physType.RelRowType, physType.Format, false);

            var row = J.Expressions.parameter(elementType, "row");
            var parameter = Expression.Parameter(element, "row");
            implementor.Translator.Bind(row, parameter);

            var fieldCount = table.getRowType().getFieldCount();
            var expressionList = new java.util.ArrayList(fieldCount);
            for (int i = 0; i < fieldCount; i++)
                expressionList.add(FieldExpression(row, i, calcite, oldFormat));

            var rowType = physType.RowType;
            var selector = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(element, rowType),
                implementor.Translator.Translate(calcite.record(expressionList)),
                parameter);

            return ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Select.MakeGenericMethod(element, rowType), Source(element), selector);
        }

        /// <summary>
        /// Reads the table's linq4j sequence as a .NET one of the given row type.
        /// </summary>
        /// <param name="element"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        static Expression FromJava(Type element, Expression source)
        {
            return ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.FromJavaAsync.MakeGenericMethod(element), source);
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
