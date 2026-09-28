using System;
using System.Linq.Expressions;

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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="TableScan"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableTableScan</c>. A table of Calcite's SPI is read through its
    /// <c>getExpression(Queryable.class)</c>, a linq4j expression yielding an <c>Enumerable</c>, over which a
    /// cursor is opened. A table implementing <see cref="IClrScannableTable"/>, <see cref="IClrQueryableTable"/>
    /// or <see cref="IClrCursorTable"/> is read directly, synchronously or awaiting as the implementation
    /// requires. Rows are reshaped only where the physical type's format differs from the table's or a field
    /// holds a collection of structs.
    /// </remarks>
    public class ClrCursorTableScan : TableScan, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorTableScan"/>, taking its collations from the table's statistics.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="relOptTable">The table to scan.</param>
        /// <returns>The new scan.</returns>
        public static ClrCursorTableScan Create(RelOptCluster cluster, RelOptTable relOptTable)
        {
            var table = (Table)relOptTable.unwrap(typeof(Table));
            var elementType = DeduceElementType(table);
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => table != null ? table.getStatistic().getCollations() : com.google.common.collect.ImmutableList.of()));

            return new ClrCursorTableScan(cluster, traitSet, relOptTable, elementType);
        }

        /// <summary>
        /// Returns whether a scan of this convention can read a table.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns><see langword="true"/> for a table of this project's SPI or a
        /// <see cref="QueryableTable"/>, <see cref="FilterableTable"/>, <see cref="ProjectableFilterableTable"/>
        /// or <see cref="ScannableTable"/>; <see langword="false"/> for a <see cref="TransientTable"/> or
        /// anything else.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableTableScan.canHandle(Table)</c>, with this project's table interfaces added.
        /// </remarks>
        public static bool CanHandle(Table table)
        {
            // a TransientTable has no expression, so a scan cannot read it
            if (table is TransientTable)
                return false;

            if (table is IClrScannableTable or IClrQueryableTable or IClrCursorTable)
                return true;

            // see org.apache.calcite.prepare.RelOptTableImpl.getClassExpressionFunction
            return table is QueryableTable
                || table is FilterableTable
                || table is ProjectableFilterableTable
                || table is ScannableTable;
        }

        /// <summary>
        /// Returns whether a scan of this convention can read a table, considering its field types.
        /// </summary>
        /// <param name="relOptTable">The table.</param>
        /// <returns><see langword="true"/> if the scan can read the table.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableTableScan.canHandle(RelOptTable)</c>, including its treatment of the
        /// <c>ENUMERABLE_ENABLE_TABLESCAN_ARRAY</c>, <c>_MAP</c> and <c>_MULTISET</c> properties: unless all
        /// three are set, a field of a type whose property is set makes the table unreadable.
        /// </remarks>
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

            // reproduces Calcite, which marks a type unsupported when its property is set
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
        /// Returns the Java class of a table's rows.
        /// </summary>
        /// <param name="table">The table, or <see langword="null"/>.</param>
        /// <returns>The element type.</returns>
        /// <remarks>
        /// An <see cref="IClrQueryableTable"/> gives its <see cref="IClrQueryableTable.ElementType"/>, and an
        /// <see cref="IClrScannableTable"/> or <see cref="IClrCursorTable"/> yields <c>Object[]</c>. Any other
        /// table is answered by <c>EnumerableTableScan.deduceElementType</c>.
        /// </remarks>
        public static java.lang.Class DeduceElementType(Table? table)
        {
            if (table is IClrQueryableTable queryable)
                return (java.lang.Class)queryable.ElementType;

            if (table is IClrScannableTable or IClrCursorTable)
                return (java.lang.Class)typeof(object[]);

            return EnumerableTableScan.deduceElementType(table);
        }

        /// <summary>
        /// Returns the row format a table's element type implies.
        /// </summary>
        /// <param name="table">The table.</param>
        /// <returns><see cref="JavaRowFormat.ARRAY"/> if the element type is <c>Object[]</c>; otherwise
        /// <see cref="JavaRowFormat.CUSTOM"/>.</returns>
        public static JavaRowFormat DeduceFormat(RelOptTable table)
        {
            var elementType = DeduceElementType((Table)table.unwrapOrThrow(typeof(Table)));

            return elementType == (java.lang.Class)typeof(object[]) ? JavaRowFormat.ARRAY : JavaRowFormat.CUSTOM;
        }

        readonly java.lang.Class elementType;

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> derives the trait set and element type; this
        /// constructor takes them as given.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traitSet">The node's traits, in <see cref="ClrCursorConvention"/>.</param>
        /// <param name="table">The table, which <see cref="CanHandle(RelOptTable)"/> must accept.</param>
        /// <param name="elementType">The Java class of the table's rows; see <see cref="DeduceElementType"/>.</param>
        /// <exception cref="java.lang.AssertionError">The convention is wrong or the table cannot be read.</exception>
        public ClrCursorTableScan(RelOptCluster cluster, RelTraitSet traitSet, RelOptTable table, java.lang.Class elementType) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), table)
        {
            if (getConvention() is not ClrCursorConvention)
                throw new java.lang.AssertionError();
            if (CanHandle(table) == false)
                throw new java.lang.AssertionError($"ClrCursorTableScan can't implement {table}, see ClrCursorTableScan.CanHandle");

            this.elementType = elementType;
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorTableScan(getCluster(), traitSet, table, elementType);
        }

        /// <inheritdoc />
        /// <remarks>
        /// Always returns <see langword="null"/>, as <c>EnumerableTableScan.passThrough</c> does; there is no
        /// index scan to substitute.
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
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), Format());

            var unwrapped = (Table)table.unwrap(typeof(Table));

            // this project's table SPI yields CLR sequences or cursors, so there is no linq4j to translate
            if (unwrapped is IClrScannableTable or IClrQueryableTable or IClrCursorTable)
                return implementor.Result(physType, ToRows(implementor, physType, ClrSource(implementor), true));

            var expression = table.getExpression(typeof(Queryable))
                ?? throw new java.lang.IllegalStateException($"Unable to implement {RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: {table}.getExpression(Queryable.class) returned null");

            var source = ToEnumerable(implementor.Translator.Translate(expression));

            return implementor.Result(physType, ToRows(implementor, physType, source, false));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), Format());

            var unwrapped = (Table)table.unwrap(typeof(Table));

            // this project's table SPI yields CLR sequences or cursors, so there is no linq4j to translate
            if (unwrapped is IClrScannableTable or IClrQueryableTable or IClrCursorTable)
                return implementor.ResultAsync(physType, ToRowsAsync(implementor, physType, ClrSourceAsync(implementor), true));

            var expression = table.getExpression(typeof(Queryable))
                ?? throw new java.lang.IllegalStateException($"Unable to implement {RelOptUtil.toString(this, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}: {table}.getExpression(Queryable.class) returned null");

            var source = ToEnumerable(implementor.Translator.Translate(expression));

            return implementor.ResultAsync(physType, ToRowsAsync(implementor, physType, source, false));
        }

        /// <summary>
        /// Returns the synchronous open of a table of this project's SPI.
        /// </summary>
        /// <remarks>
        /// An <see cref="IClrCursorTable"/> opens its own cursor. An <see cref="IClrQueryableTable"/> supplies
        /// an expression, as a <see cref="QueryableTable"/> does, and an <see cref="IClrScannableTable"/> is
        /// called, as a <see cref="ScannableTable"/> is; either yields an
        /// <see cref="System.Collections.Generic.IEnumerable{T}"/> of the element type, over which a cursor is
        /// opened.
        /// </remarks>
        /// <param name="implementor">The implementor, whose root parameter is passed to the table.</param>
        /// <returns>An expression evaluating to the opened cursor over the table's elements.</returns>
        Expression ClrSource(ClrCursorRelImplementor implementor)
        {
            var unwrapped = (Table)table.unwrap(typeof(Table));
            var element = ClrTypes.FromClass(elementType);

            if (unwrapped is IClrCursorTable cursorTable)
                return Expression.Call(
                    Expression.Constant(cursorTable, typeof(IClrCursorTable)),
                    OpenMethod,
                    implementor.Root);

            Expression sequence;
            if (unwrapped is IClrQueryableTable queryable)
            {
                var names = table.getQualifiedName();

                sequence = queryable.GetExpression(
                    ((org.apache.calcite.jdbc.CalciteSchema)table.unwrap(typeof(org.apache.calcite.jdbc.CalciteSchema)))?.plus(),
                    (string)names.get(names.size() - 1))
                    ?? throw new java.lang.IllegalStateException($"{table}.GetExpression returned null");
            }
            else
            {
                // Calcite stashes the table; an expression tree can hold it as a constant
                sequence = Expression.Call(
                    Expression.Constant((IClrScannableTable)unwrapped, typeof(IClrScannableTable)),
                    ScanMethod,
                    implementor.Root);
            }

            return Expression.Call(null, ClrCursorBuiltInMethod.AsCursor.MakeGenericMethod(element), sequence);
        }

        /// <summary>
        /// Returns the awaiting open of a table of this project's SPI.
        /// </summary>
        /// <remarks>
        /// As <see cref="ClrSource"/>, using each interface's awaiting member, which yields an
        /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> or an awaiting open. A table that
        /// implements only the synchronous members answers through the interface's defaults.
        /// </remarks>
        /// <param name="implementor">The implementor, whose root and token parameters are passed to the table.</param>
        /// <returns>An expression evaluating to a task of the opened cursor over the table's elements.</returns>
        Expression ClrSourceAsync(ClrCursorRelImplementor implementor)
        {
            var unwrapped = (Table)table.unwrap(typeof(Table));
            var element = ClrTypes.FromClass(elementType);

            // the open's token is passed here; each ReadAsync supplies its own
            if (unwrapped is IClrCursorTable cursorTable)
                return Expression.Call(
                    Expression.Constant(cursorTable, typeof(IClrCursorTable)),
                    OpenAsyncMethod,
                    implementor.Root,
                    implementor.CancellationToken);

            Expression sequence;
            if (unwrapped is IClrQueryableTable queryable)
            {
                var names = table.getQualifiedName();

                sequence = queryable.GetAsyncExpression(
                    ((org.apache.calcite.jdbc.CalciteSchema)table.unwrap(typeof(org.apache.calcite.jdbc.CalciteSchema)))?.plus(),
                    (string)names.get(names.size() - 1))
                    ?? throw new java.lang.IllegalStateException($"{table}.GetAsyncExpression returned null");
            }
            else
            {
                // Calcite stashes the table; an expression tree can hold it as a constant
                sequence = Expression.Call(
                    Expression.Constant((IClrScannableTable)unwrapped, typeof(IClrScannableTable)),
                    ScanAsyncMethod,
                    implementor.Root);
            }

            return ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.AsCursorAsync.MakeGenericMethod(element), sequence);
        }

        /// <summary>
        /// Returns the synchronous open that yields the table's rows in the given physical type. Mirrors the
        /// row handling in <c>EnumerableTableScan.implement</c>.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="physType">The physical type the rows must have.</param>
        /// <param name="source">The table's rows: an open, for a table of this project's SPI, or a linq4j
        /// <c>Enumerable</c> over which a cursor is opened.</param>
        /// <param name="native">Whether <paramref name="source"/> is already an open.</param>
        /// <returns>An expression evaluating to the opened cursor over rows of <paramref name="physType"/>.</returns>
        Expression ToRows(ClrCursorRelImplementor implementor, ClrPhysType physType, Expression source, bool native)
        {
            var element = ClrTypes.FromClass(elementType);

            Expression Source(System.Type rowType) => native ? source : FromJava(rowType, source);

            if (IsSliced(physType))
                return Expression.Call(null,
                    ClrCursorBuiltInMethod.Slice0.MakeGenericMethod(physType.RowType),
                    Source(element));

            var oldFormat = Format();
            if (physType.Format == oldFormat && HasCollectionField(getRowType()) == false)
                // the cursor is typed by the physical row type, not the table's element type as in Calcite:
                // the two differ where the format was optimized, such as a one-column table whose element
                // type is Object[] but whose rows are the values themselves
                return Source(physType.RowType);

            // the selector is Calcite's, built against a Calcite physical type and translated, because a
            // collection field is reformatted through linq4j (see FieldExpression)
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

            return Expression.Call(null, ClrCursorBuiltInMethod.Select.MakeGenericMethod(element, rowType), Source(element), selector);
        }

        /// <summary>
        /// Returns the awaiting open that yields the table's rows in the given physical type.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="physType">The physical type the rows must have.</param>
        /// <param name="source">The table's rows: an awaiting open, for a table of this project's SPI, or a
        /// linq4j <c>Enumerable</c> over which a cursor is opened.</param>
        /// <param name="native">Whether <paramref name="source"/> is already an awaiting open.</param>
        /// <returns>An expression evaluating to a task of the opened cursor over rows of <paramref name="physType"/>.</returns>
        Expression ToRowsAsync(ClrCursorRelImplementor implementor, ClrPhysType physType, Expression source, bool native)
        {
            var element = ClrTypes.FromClass(elementType);

            Expression Source(System.Type rowType) => native ? source : FromJavaAsync(implementor, rowType, source);

            if (IsSliced(physType))
                return ClrCursorBuiltInMethod.CallAsync(implementor,
                    ClrCursorBuiltInMethod.Slice0Async.MakeGenericMethod(physType.RowType),
                    Source(element));

            var oldFormat = Format();
            if (physType.Format == oldFormat && HasCollectionField(getRowType()) == false)
                // the cursor is typed by the physical row type, not the table's element type as in Calcite:
                // the two differ where the format was optimized, such as a one-column table whose element
                // type is Object[] but whose rows are the values themselves
                return Source(physType.RowType);

            // the selector is Calcite's, built against a Calcite physical type and translated, because a
            // collection field is reformatted through linq4j (see FieldExpression)
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

            return ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.SelectAsync.MakeGenericMethod(element, rowType), Source(element), selector);
        }

        /// <summary>
        /// Returns whether each row the table yields is an array to be narrowed to its first element, the value
        /// of a one-column physical row. Mirrors the condition under which <c>EnumerableTableScan.implement</c>
        /// calls <c>slice0</c>.
        /// </summary>
        /// <param name="physType">The physical type the rows must have.</param>
        /// <returns><see langword="true"/> if the rows are to be narrowed to their first element.</returns>
        /// <remarks>
        /// Calcite names the tables of its SPI whose rows are arrays whatever the column count. An
        /// <see cref="IClrScannableTable"/> or <see cref="IClrCursorTable"/> yields an array per row by
        /// contract, one column included, so each is named beside its counterpart.
        ///
        /// <para>Calcite leaves a <see cref="QueryableTable"/> out: one of one column whose element type is
        /// <c>Object[]</c> yields the values themselves, as <c>ResultSetEnumerable</c> does for a
        /// <c>JdbcTable</c>, which erasure lets an <c>Enumerable&lt;Object[]&gt;</c> hold. An
        /// <see cref="IClrQueryableTable"/> is named here because the CLR does not erase: its sequence of
        /// <c>object[]</c> can hold only arrays, so that contract cannot be met and its rows are arrays.</para>
        /// </remarks>
        bool IsSliced(ClrPhysType physType)
        {
            return physType.Format == JavaRowFormat.SCALAR
                && ((java.lang.Class)typeof(object[])).isAssignableFrom(elementType)
                && getRowType().getFieldCount() == 1
                && (table.unwrap(typeof(ScannableTable)) != null
                    || table.unwrap(typeof(FilterableTable)) != null
                    || table.unwrap(typeof(ProjectableFilterableTable)) != null
                    || table.unwrap(typeof(Table)) is IClrScannableTable or IClrCursorTable or IClrQueryableTable);
        }

        /// <summary>
        /// Returns a synchronous open of a cursor of the given row type over a linq4j <c>Enumerable</c>.
        /// </summary>
        /// <param name="element">The CLR row type of the cursor.</param>
        /// <param name="source">An expression of type <c>Enumerable</c> yielding rows of that type.</param>
        /// <returns>An expression evaluating to the opened cursor.</returns>
        static Expression FromJava(Type element, Expression source)
        {
            return Expression.Call(null, ClrCursorBuiltInMethod.FromJava.MakeGenericMethod(element), source);
        }

        /// <summary>
        /// Returns an awaiting open of a cursor of the given row type over a linq4j <c>Enumerable</c>.
        /// </summary>
        /// <param name="implementor">The implementor, whose token parameter the open receives.</param>
        /// <param name="element">The CLR row type of the cursor.</param>
        /// <param name="source">An expression of type <c>Enumerable</c> yielding rows of that type.</param>
        /// <returns>An expression evaluating to a task of the opened cursor.</returns>
        static Expression FromJavaAsync(ClrCursorRelImplementor implementor, Type element, Expression source)
        {
            return ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.FromJavaAsync.MakeGenericMethod(element), source);
        }

        /// <summary>
        /// Converts the value of a table's expression to a linq4j <see cref="Enumerable"/>: an array or an
        /// <c>Iterable</c> is wrapped, and a <see cref="Queryable"/> is read through <c>asEnumerable</c>.
        /// Mirrors <c>EnumerableTableScan.toEnumerable</c>.
        /// </summary>
        /// <param name="expression">The table's expression, of an array, <c>Iterable</c>, <c>Queryable</c> or <c>Enumerable</c> type.</param>
        /// <returns>An expression of type <see cref="Enumerable"/> over the same elements.</returns>
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

            // a Queryable is also an Enumerable, but its operators build expressions; asEnumerable makes them
            // evaluate directly, as Calcite does here
            if (typeof(Queryable).IsAssignableFrom(type))
                return Expression.Call(expression, QueryableAsEnumerable);

            return expression;
        }

        /// <summary>
        /// Returns the linq4j expression reading field <paramref name="i"/> of a table row, converting an
        /// array or multiset of structs to a list of lists. Mirrors <c>EnumerableTableScan.fieldExpression</c>.
        /// </summary>
        /// <param name="row">The row parameter.</param>
        /// <param name="i">The field ordinal.</param>
        /// <param name="physType">The output physical type.</param>
        /// <param name="format">The table's row format.</param>
        /// <returns>The linq4j expression for the field's value, typed as the output physical type's field.</returns>
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

                    // a consumer does not know a struct element's class, so each element is converted to a List
                    // and the collection becomes a List<List>
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
        /// Returns the row format of the table's element type. Mirrors <c>EnumerableTableScan.format</c>.
        /// </summary>
        /// <returns><c>LIST</c> for a row of no fields, <c>SCALAR</c> or <c>ARRAY</c> for an object array, <c>ROW</c> for a <see cref="Row"/>,
        /// <c>SCALAR</c> for a single field of an object, primitive, number or string type, and <c>CUSTOM</c> otherwise.</returns>
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
        /// <param name="rowType">The row type to inspect.</param>
        /// <returns><see langword="true"/> if at least one field is of type <c>ARRAY</c> or <c>MULTISET</c>.</returns>
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

        static readonly System.Reflection.MethodInfo ScanMethod = typeof(IClrScannableTable).GetMethod(nameof(IClrScannableTable.Scan))
            ?? throw new System.InvalidOperationException($"'{nameof(IClrScannableTable.Scan)}' is missing.");

        /// <summary>
        /// <see cref="IClrScannableTable.ScanAsync"/>.
        /// </summary>
        static readonly System.Reflection.MethodInfo ScanAsyncMethod = typeof(IClrScannableTable).GetMethod(nameof(IClrScannableTable.ScanAsync))
            ?? throw new System.InvalidOperationException($"'{nameof(IClrScannableTable.ScanAsync)}' is missing.");

        /// <summary>
        /// <see cref="IClrCursorTable.Open"/>.
        /// </summary>
        static readonly System.Reflection.MethodInfo OpenMethod = typeof(IClrCursorTable).GetMethod(nameof(IClrCursorTable.Open))
            ?? throw new System.InvalidOperationException($"'{nameof(IClrCursorTable.Open)}' is missing.");

        /// <summary>
        /// <see cref="IClrCursorTable.OpenAsync"/>.
        /// </summary>
        static readonly System.Reflection.MethodInfo OpenAsyncMethod = typeof(IClrCursorTable).GetMethod(nameof(IClrCursorTable.OpenAsync))
            ?? throw new System.InvalidOperationException($"'{nameof(IClrCursorTable.OpenAsync)}' is missing.");

        static readonly System.Reflection.MethodInfo AsList = ClrTypes.Resolve(BuiltInMethod.AS_LIST.method);

        static readonly System.Reflection.MethodInfo AsEnumerable = ClrTypes.Resolve(BuiltInMethod.AS_ENUMERABLE.method);

        static readonly System.Reflection.MethodInfo AsEnumerable2 = ClrTypes.Resolve(BuiltInMethod.AS_ENUMERABLE2.method);

        static readonly System.Reflection.MethodInfo QueryableAsEnumerable = ClrTypes.Resolve(BuiltInMethod.QUERYABLE_AS_ENUMERABLE.method);

    }

}
