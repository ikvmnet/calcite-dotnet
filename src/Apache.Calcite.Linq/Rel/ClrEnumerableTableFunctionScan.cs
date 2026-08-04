using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.validate;

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
    /// <para>Only the first is here. A window table function is refused by the rule, so the planner leaves
    /// it in <c>EnumerableConvention</c> and the converters carry the rows — see <c>TODO.md</c> for what
    /// stopped the other path.</para>
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
            return ImplementTableFunction(implementor);
        }

        /// <summary>
        /// Implements a table function the schema defines, which is a call yielding a sequence.
        /// </summary>
        /// <param name="implementor"></param>
        /// <returns></returns>
        ClrEnumerableResult ImplementTableFunction(ClrEnumerableRelImplementor implementor)
        {
            var typeFactory = implementor.TypeFactory;
            var physType = PhysTypeImpl.of(typeFactory, getRowType(), Format(), false);

            var block = new J.BlockBuilder();
            var translator = RexToLixTranslator
                .forAggregation(typeFactory, block, null, implementor.Conformance)
                .setCorrelates(implementor.AllCorrelateVariables);

            block.add(ClrEnumUtils.Translate(translator, getCall(), null));

            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            return implementor.Result(physType,
                Expression.Call(null,
                    ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType),
                    implementor.Translator.TranslateBody(block.toBlock(), typeof(org.apache.calcite.linq4j.Enumerable))));
        }

        /// <summary>
        /// Returns how a row of a table function's result is represented.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// A user-specified element type that is not an array is not supported, which is Calcite's limit
        /// rather than one added here.
        /// </remarks>
        JavaRowFormat Format()
        {
            var elementType = getElementType();

            if (elementType == null)
                return JavaRowFormat.ARRAY;

            if (getRowType().getFieldCount() == 1 && IsQueryable())
                return JavaRowFormat.SCALAR;

            if (elementType is java.lang.Class clazz && ((java.lang.Class)typeof(object[])).isAssignableFrom(clazz))
                return JavaRowFormat.ARRAY;

            return JavaRowFormat.CUSTOM;
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
