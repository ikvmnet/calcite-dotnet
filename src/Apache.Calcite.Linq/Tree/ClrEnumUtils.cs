using System;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// The parts of <c>EnumUtils</c> a join needs.
    /// </summary>
    /// <remarks>
    /// These two are ported rather than reused, because Calcite declares them package private and nothing
    /// outside its own package can call them. They are the same code against this convention's implementor.
    /// </remarks>
    public static class ClrEnumUtils
    {

        /// <summary>
        /// Returns the lambda that builds an output row from a row of each input.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="joinType"></param>
        /// <param name="physType"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite has a second, compact form of this for a row of many fields, because the generated method
        /// would otherwise exceed what a Java class file allows (CALCITE-3094). An expression tree has no such
        /// limit, so there is one form here and it is the one Calcite uses everywhere else.
        /// </remarks>
        public static LambdaExpression JoinSelector(ClrEnumerableRelImplementor implementor, JoinRelType joinType, PhysType physType, PhysType left, PhysType right)
        {
            var outputFieldCount = physType.getRowType().getFieldCount();
            var inputs = new[] { left, right };

            var parameters = new ParameterExpression[2];
            var expressions = new java.util.ArrayList();

            for (int ord = 0; ord < inputs.Length; ord++)
            {
                var inputPhysType = inputs[ord].makeNullable(joinType.generatesNullsOn(ord));

                // a Function always takes boxed arguments, so a row that is a primitive is boxed here
                var javaRowType = J.Primitive.box(inputPhysType.getJavaRowType());
                var row = J.Expressions.parameter(javaRowType, ord == 0 ? "left" : "right");

                parameters[ord] = Expression.Parameter(TypeResolver.Resolve(javaRowType), ord == 0 ? "left" : "right");
                implementor.Translator.Bind(row, parameters[ord]);

                // a semi join returns the left input alone, so the fields run out before the inputs do
                if (expressions.size() == outputFieldCount)
                    break;

                var fieldCount = inputPhysType.getRowType().getFieldCount();
                for (int i = 0; i < fieldCount; i++)
                {
                    var expression = inputPhysType.fieldReference(row, i, physType.getJavaFieldType(expressions.size()));

                    if (joinType.generatesNullsOn(ord))
                        expression = J.Expressions.condition(
                            J.Expressions.equal(row, J.Expressions.constant(null)),
                            J.Expressions.constant(null),
                            expression);

                    expressions.add(expression);
                }
            }

            var rowType = TypeResolver.Resolve(physType.getJavaRowType());

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(parameters[0].Type, parameters[1].Type, rowType),
                implementor.Translator.Translate(physType.record(expressions)),
                parameters);
        }

        /// <summary>
        /// Returns the lambda that tests the part of a join condition that is not an equality.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="rexBuilder"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="leftPhysType"></param>
        /// <param name="rightPhysType"></param>
        /// <param name="condition"></param>
        /// <returns></returns>
        public static LambdaExpression GeneratePredicate(ClrEnumerableRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, PhysType leftPhysType, PhysType rightPhysType, RexNode condition)
        {
            var left_ = J.Expressions.parameter(leftPhysType.getJavaRowType(), "left");
            var right_ = J.Expressions.parameter(rightPhysType.getJavaRowType(), "right");

            var leftParameter = Expression.Parameter(TypeResolver.Resolve(leftPhysType.getJavaRowType()), "left");
            var rightParameter = Expression.Parameter(TypeResolver.Resolve(rightPhysType.getJavaRowType()), "right");
            implementor.Translator.Bind(left_, leftParameter);
            implementor.Translator.Bind(right_, rightParameter);

            var program = new RexProgramBuilder(
                implementor.TypeFactory.builder()
                    .addAll(left.getRowType().getFieldList())
                    .addAll(right.getRowType().getFieldList())
                    .build(),
                rexBuilder);
            program.addCondition(condition);

            var inputs = new java.util.LinkedHashMap();
            inputs.put(left_, leftPhysType);
            inputs.put(right_, rightPhysType);

            var builder = new J.BlockBuilder();
            builder.add(
                J.Expressions.return_(null,
                    RexToLixTranslator.translateCondition(
                        program.getProgram(),
                        implementor.TypeFactory,
                        builder,
                        new RexToLixTranslator.InputGetterImpl(inputs),
                        implementor.AllCorrelateVariables,
                        implementor.Conformance)));

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftParameter.Type, rightParameter.Type, typeof(bool)),
                implementor.Translator.TranslateBody(builder.toBlock(), typeof(bool)),
                leftParameter,
                rightParameter);
        }

    }

}
