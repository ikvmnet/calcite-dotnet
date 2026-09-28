using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="TableFunctionScan"/> in the <see cref="ClrCursorConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableTableFunctionScan</c>, which handles two kinds of call and dispatches between
    /// them on <c>isImplementorDefined</c>. A window table function (<c>TUMBLE</c>, <c>HOP</c>,
    /// <c>SESSION</c>) is implemented by Calcite's generator, which takes the input as a linq4j
    /// <c>Enumerable</c>. A user-defined table function is a call, translated by Calcite's row expression
    /// translator, that returns a linq4j <c>Enumerable</c>, as the schema SPI defines it.
    ///
    /// <para>Calcite's generators cannot await, so the awaiting implementation of a window table function
    /// opens its input by blocking, and reading its rows blocks wherever the input would await.</para>
    /// </remarks>
    public class ClrCursorTableFunctionScan : TableFunctionScan, ClrCursorRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traits">The node's traits.</param>
        /// <param name="inputs">The inputs, a list of <see cref="org.apache.calcite.rel.RelNode"/>; a window
        /// table function has one.</param>
        /// <param name="elementType">The element type of the collection the function returns, or
        /// <see langword="null"/>.</param>
        /// <param name="rowType">The output row type.</param>
        /// <param name="call">The function call.</param>
        /// <param name="columnMappings">How output columns derive from input columns, or
        /// <see langword="null"/>.</param>
        public ClrCursorTableFunctionScan(
            RelOptCluster cluster, RelTraitSet traits, java.util.List inputs, java.lang.reflect.Type elementType,
            RelDataType rowType, RexNode call, java.util.Set columnMappings) :
            base(cluster, traits, com.google.common.collect.ImmutableList.of(), inputs, call, elementType, rowType, columnMappings)
        {

        }

        /// <inheritdoc />
        public override TableFunctionScan copy(
            RelTraitSet traitSet, java.util.List inputs, RexNode rexCall, java.lang.reflect.Type elementType,
            RelDataType rowType, java.util.Set columnMappings)
        {
            return new ClrCursorTableFunctionScan(getCluster(), traitSet, inputs, elementType, rowType, rexCall, columnMappings);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (IsImplementorDefined((RexCall)getCall()))
                return TvfImplementorBasedImplement(implementor, pref);

            return DefaultTableFunctionImplement(implementor);
        }

        /// <inheritdoc />
        /// <remarks>
        /// A window table function's input is opened by blocking, because Calcite's generator reads it as a
        /// linq4j <c>Enumerable</c>; nodes above this one still await. A user-defined table function has no
        /// input to await, so its synchronous open is used.
        /// </remarks>
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (IsImplementorDefined((RexCall)getCall()))
                return TvfImplementorBasedImplementAsync(implementor, pref);

            return implementor.Awaited(DefaultTableFunctionImplement(implementor));
        }

        /// <summary>
        /// Returns whether the call is a window table function that <c>RexImpTable</c> implements, rather than
        /// a user-defined table function. Mirrors <c>EnumerableTableFunctionScan.isImplementorDefined</c>.
        /// </summary>
        /// <param name="call">The table function call.</param>
        /// <returns><see langword="true"/> if the operator is a <c>SqlWindowTableFunction</c> with an implementor in <c>RexImpTable</c>.</returns>
        internal static bool IsImplementorDefined(RexCall call)
        {
            return call.getOperator() is org.apache.calcite.sql.SqlWindowTableFunction window
                && RexImpTable.INSTANCE.get(window) != null;
        }

        /// <summary>
        /// Implements a window table function, whose rows are its input's with the window bounds appended.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableTableFunctionScan.tvfImplementorBasedImplement</c>. The window is built by
        /// <c>RexToLixTranslator.translateTableFunction</c>, which takes the input as a linq4j expression
        /// yielding an <c>Enumerable</c> and returns another.
        ///
        /// <para>Calcite's generated code declares <c>_input</c> twice: once for the input, and again as the
        /// parameter of the lambda from <c>EnumUtils.tumblingWindowSelector</c>, which shadows it. The
        /// translator resolves parameters by name within a lambda to reproduce that.</para>
        /// </remarks>
        /// <param name="implementor">The implementor, through which the input is visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to the input.</param>
        /// <returns>The synchronous open of the window's rows.</returns>
        ClrCursorResult TvfImplementorBasedImplement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInputs().get(0);
            var result = implementor.VisitChild(this, 0, child, pref);

            // the input is passed as a sequence over its opener, so the generator's enumerator() opens it at
            // this node's open, as in linq4j, and a second enumerator() opens it again
            return TvfImplementorBasedWindow(implementor, pref, result.PhysType, result.Format,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.AsEnumerable.MakeGenericMethod(result.PhysType.RowType),
                    implementor.Opener(result)));
        }

        /// <summary>
        /// Implements a window table function over the input's awaiting open.
        /// </summary>
        /// <remarks>
        /// A linq4j <c>Enumerable</c> cannot await, so the input's awaiting open is converted with
        /// <see cref="ClrCursorRelImplementor.Pulled"/> and the window's synchronous open with
        /// <see cref="ClrCursorRelImplementor.Awaited"/>.
        /// </remarks>
        /// <param name="implementor">The implementor, through which the input is visited.</param>
        /// <param name="pref">The row representation the parent prefers; passed on to the input.</param>
        /// <returns>The awaiting open of the window's rows.</returns>
        ClrCursorAsyncResult TvfImplementorBasedImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var child = (ClrCursorRel)getInputs().get(0);
            var result = implementor.VisitChildAsync(this, 0, child, pref);

            return implementor.Awaited(
                TvfImplementorBasedWindow(implementor, pref, result.PhysType, result.Format,
                    Expression.Call(null,
                        ClrCursorBuiltInMethod.AsEnumerable.MakeGenericMethod(result.PhysType.RowType),
                        implementor.Opener(implementor.Pulled(result)))));
        }

        /// <summary>
        /// Builds the window table function over the input's rows, for both implementations.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="pref">The row representation the parent prefers.</param>
        /// <param name="inputPhysType">The input's physical type.</param>
        /// <param name="inputFormat">The input's row format.</param>
        /// <param name="pulled">The input's rows as an <see cref="System.Collections.Generic.IEnumerable{T}"/>
        /// of its physical row type, which opens the input when enumerated.</param>
        /// <returns>The synchronous open of the window's rows, in a physical type chosen from <paramref name="pref"/> and the input's format.</returns>
        ClrCursorResult TvfImplementorBasedWindow(ClrCursorRelImplementor implementor, ClrCursorPrefer pref, ClrPhysType inputPhysType, JavaRowFormat inputFormat, Expression pulled)
        {
            var typeFactory = implementor.TypeFactory;
            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.Prefer(inputFormat));

            var sourceType = inputPhysType.RowType;
            var source = Expression.Call(null, ClrCursorBuiltInMethod.ToJava.MakeGenericMethod(sourceType), pulled);

            var input_ = J.Expressions.parameter((java.lang.Class)typeof(org.apache.calcite.linq4j.Enumerable), "_input");
            var inputParameter = Expression.Parameter(typeof(org.apache.calcite.linq4j.Enumerable), "_input");
            implementor.Translator.Bind(input_, inputParameter);

            var block = new J.BlockBuilder();
            block.add(
                RexToLixTranslator.translateTableFunction(
                    typeFactory,
                    implementor.Conformance,
                    block,
                    DataContext.ROOT,
                    (RexCall)getCall(),
                    input_,
                    PhysTypeImpl.of(typeFactory, inputPhysType.RelRowType, inputPhysType.Format, false),
                    PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false)));

            var windowed = Expression.Block(
                typeof(org.apache.calcite.linq4j.Enumerable),
                [inputParameter],
                Expression.Assign(inputParameter, source),
                implementor.Translator.TranslateBody(block.toBlock(), typeof(org.apache.calcite.linq4j.Enumerable)));

            var rowType = physType.RowType;

            return implementor.Result(physType,
                Expression.Call(null, ClrCursorBuiltInMethod.FromJava.MakeGenericMethod(rowType), windowed));
        }

        /// <summary>
        /// Implements a user-defined table function by translating the call. Mirrors
        /// <c>EnumerableTableFunctionScan.defaultTableFunctionImplement</c>.
        /// </summary>
        /// <param name="implementor">The implementor, whose translator the generated block is translated with.</param>
        /// <returns>The synchronous open of a cursor over the <c>Enumerable</c> the translated call returns.</returns>
        ClrCursorResult DefaultTableFunctionImplement(ClrCursorRelImplementor implementor)
        {
            var typeFactory = implementor.TypeFactory;

            // the row format follows Calcite's choice; an element type that is not an array is read as CUSTOM
            var elementType = getElementType();
            JavaRowFormat format;
            if (elementType == null)
                format = JavaRowFormat.ARRAY;
            else if (getRowType().getFieldCount() == 1 && IsQueryable())
                format = JavaRowFormat.SCALAR;
            else if (elementType is java.lang.Class clazz && ((java.lang.Class)typeof(object[])).isAssignableFrom(clazz))
                format = JavaRowFormat.ARRAY;
            else
                format = JavaRowFormat.CUSTOM;

            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), format, false);

            var block = new J.BlockBuilder();
            var translator = RexToLixTranslator
                .forAggregation((JavaTypeFactory)getCluster().getTypeFactory(), block, null, implementor.Conformance)
                .setCorrelates(implementor.AllCorrelateVariables);

            block.add(ClrEnumUtils.Translate(translator, getCall(), null));

            var rowType = physType.RowType;

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrCursorBuiltInMethod.FromJava.MakeGenericMethod(rowType),
                    implementor.Translator.TranslateBody(block.toBlock(), typeof(org.apache.calcite.linq4j.Enumerable))));
        }

        /// <summary>
        /// Returns whether the call is to a <see cref="TableFunctionImpl"/> whose method returns a
        /// <see cref="QueryableTable"/>.
        /// </summary>
        /// <returns><see langword="true"/> if the function's method returns a <see cref="QueryableTable"/>; <see langword="false"/> for any other call.</returns>
        bool IsQueryable()
        {
            if (getCall() is not RexCall call)
                return false;

            if (call.getOperator() is not org.apache.calcite.sql.validate.SqlUserDefinedTableFunction udtf)
                return false;

            if (udtf.getFunction() is not TableFunctionImpl function)
                return false;

            return ((java.lang.Class)typeof(QueryableTable)).isAssignableFrom(function.method.getReturnType());
        }

    }

}
