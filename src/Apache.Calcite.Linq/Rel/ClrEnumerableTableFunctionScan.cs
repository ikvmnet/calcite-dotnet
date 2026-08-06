using System.Linq.Expressions;

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

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="TableFunctionScan"/> in the <see cref="ClrEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Two different things wear this node. A user-defined table function is a call that yields a sequence,
    /// and translating the call is the whole of it — there is no loop and nothing to compose, so what a Rex
    /// translator gives back is taken as it is. A window table function — TUMBLE, HOP, SESSION — is instead a
    /// generator of Calcite's that <i>takes</i> the input sequence.
    ///
    /// <para>Both are here, one method each, as Calcite has one each and dispatches between them on
    /// <c>isImplementorDefined</c>.</para>
    ///
    /// <para>Both ends are linq4j, because a table function returns Calcite's own <c>Enumerable</c>: a schema
    /// defines it that way, exactly as it defines a table's <c>getExpression(Queryable.class)</c>.</para>
    /// </remarks>
    public class ClrEnumerableTableFunctionScan : TableFunctionScan, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="inputs"></param>
        /// <param name="elementType"></param>
        /// <param name="rowType"></param>
        /// <param name="call"></param>
        /// <param name="columnMappings"></param>
        public ClrEnumerableTableFunctionScan(
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
            return new ClrEnumerableTableFunctionScan(getCluster(), traitSet, inputs, elementType, rowType, rexCall, columnMappings);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (IsImplementorDefined((RexCall)getCall()))
                return TvfImplementorBasedImplement(implementor, pref);

            return DefaultTableFunctionImplement(implementor);
        }

        /// <summary>
        /// Returns whether the call is one <c>RexImpTable</c> implements — TUMBLE, HOP or SESSION — rather
        /// than one the schema defines.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        internal static bool IsImplementorDefined(RexCall call)
        {
            return call.getOperator() is org.apache.calcite.sql.SqlWindowTableFunction window
                && RexImpTable.INSTANCE.get(window) != null;
        }

        /// <summary>
        /// Implements a window table function, whose rows are its input's with the window bounds appended.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="pref"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>tvfImplementorBasedImplement</c>. Everything that builds the window is
        /// Calcite's and works in linq4j — <c>RexToLixTranslator.translateTableFunction</c>, which is public,
        /// down to the <c>TableFunctionCallImplementor</c> for the operator — and all of it takes a linq4j
        /// expression yielding an <c>Enumerable</c> and gives one back. So the sequence crosses into Java for
        /// the length of that call and comes back, which is the rule this convention holds everywhere: a node
        /// holds linq4j where a generator of Calcite's takes one.
        ///
        /// <para>The two <c>_input</c>s are not a mistake. Calcite's implementor builds the watermark column
        /// against a parameter it makes itself, and <c>EnumUtils.tumblingWindowSelector</c> makes the lambda's
        /// parameter separately; both are named <c>_input</c> and Janino resolves the name, so the lambda's
        /// shadows the local. That is what <c>LixToClrTranslator</c>'s scope by name is for, and this is the
        /// node that needs it.</para>
        /// </remarks>
        ClrEnumerableResult TvfImplementorBasedImplement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrEnumerableRel)getInputs().get(0);
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.Prefer(result.Format));

            var sourceType = result.PhysType.RowType();
            var source = Expression.Call(null, ClrBuiltInMethod.ToJava.MakeGenericMethod(sourceType), result.Expression);

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
                    result.PhysType,
                    physType));

            var windowed = Expression.Block(
                typeof(org.apache.calcite.linq4j.Enumerable),
                [inputParameter],
                Expression.Assign(inputParameter, source),
                implementor.Translator.TranslateBody(block.toBlock(), typeof(org.apache.calcite.linq4j.Enumerable)));

            var rowType = physType.RowType();

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType), windowed));
        }

        /// <summary>
        /// Implements a table function the schema defines, which is a call yielding a sequence.
        /// </summary>
        /// <param name="implementor"></param>
        /// <returns></returns>
        ClrEnumerableResult DefaultTableFunctionImplement(ClrEnumerableRelImplementor implementor)
        {
            var typeFactory = implementor.TypeFactory;

            // a user-specified element type that is not an array is not supported, which is Calcite's limit
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

            var physType = PhysTypeImpl.of(typeFactory, getRowType(), format, false);

            var block = new J.BlockBuilder();
            var translator = RexToLixTranslator
                .forAggregation((JavaTypeFactory)getCluster().getTypeFactory(), block, null, implementor.Conformance)
                .setCorrelates(implementor.AllCorrelateVariables);

            block.add(ClrEnumUtils.Translate(translator, getCall(), null));

            var rowType = physType.RowType();

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType),
                    implementor.Translator.TranslateBody(block.toBlock(), typeof(org.apache.calcite.linq4j.Enumerable))));
        }

        /// <summary>
        /// Returns whether the function yields a <see cref="QueryableTable"/>.
        /// </summary>
        /// <returns></returns>
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
