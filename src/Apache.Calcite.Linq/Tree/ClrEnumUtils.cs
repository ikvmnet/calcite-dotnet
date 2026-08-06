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
    static class ClrEnumUtils
    {

        /// <summary>
        /// Returns the Java class of a relational type, or an object array where it has no class.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static java.lang.reflect.Type JavaClass(org.apache.calcite.adapter.java.JavaTypeFactory typeFactory, org.apache.calcite.rel.type.RelDataType type)
        {
            var clazz = typeFactory.getJavaClass(type);

            return clazz is java.lang.Class ? clazz : (java.lang.Class)typeof(object[]);
        }

        /// <summary>
        /// Returns the types of the fields an aggregate call reads.
        /// </summary>
        /// <param name="inputRowType"></param>
        /// <param name="argList"></param>
        /// <returns></returns>
        public static java.util.List FieldRowTypes(org.apache.calcite.rel.type.RelDataType inputRowType, java.util.List argList)
        {
            return FieldRowTypes(inputRowType, null, argList);
        }

        /// <summary>
        /// Returns the types of the fields an aggregate call reads, where an index past the input's own fields
        /// names one of a window's constants.
        /// </summary>
        /// <param name="inputRowType"></param>
        /// <param name="extraInputs">The constants, or null where the caller has none.</param>
        /// <param name="argList"></param>
        /// <returns></returns>
        public static java.util.List FieldRowTypes(org.apache.calcite.rel.type.RelDataType inputRowType, java.util.List? extraInputs, java.util.List argList)
        {
            var inputFields = inputRowType.getFieldList();
            var types = new java.util.ArrayList(argList.size());

            for (int i = 0; i < argList.size(); i++)
            {
                var arg = ((java.lang.Integer)argList.get(i)).intValue();

                types.add(arg < inputFields.size()
                    ? ((org.apache.calcite.rel.type.RelDataTypeField)inputFields.get(arg)).getType()
                    : ((RexNode)(extraInputs ?? throw new ArgumentNullException(nameof(extraInputs))).get(arg - inputFields.size())).getType());
            }

            return types;
        }

        /// <summary>
        /// Returns the Java classes of a list of relational types.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="inputTypes"></param>
        /// <returns></returns>
        public static java.util.List FieldTypes(org.apache.calcite.adapter.java.JavaTypeFactory typeFactory, java.util.List inputTypes)
        {
            var types = new java.util.ArrayList(inputTypes.size());

            for (int i = 0; i < inputTypes.size(); i++)
                types.add(JavaClass(typeFactory, (org.apache.calcite.rel.type.RelDataType)inputTypes.get(i)));

            return types;
        }

        /// <summary>
        /// Translates one row expression.
        /// </summary>
        /// <param name="translator"></param>
        /// <param name="node"></param>
        /// <param name="storageType">The type the value is wanted as, or null for whatever it comes out as.</param>
        /// <returns></returns>
        /// <remarks>
        /// Every <c>translate</c> of <c>RexToLixTranslator</c> is package private and only the list forms are
        /// public. <c>translateList(operands, storageTypes)</c> is <c>translate(operand, storageType)</c> once
        /// per element, so a list of one is the same call by a reachable name.
        /// </remarks>
        public static J.Expression Translate(RexToLixTranslator translator, RexNode node, java.lang.reflect.Type? storageType)
        {
            var nodes = new java.util.ArrayList(1);
            nodes.add(node);

            var storageTypes = new java.util.ArrayList(1);
            storageTypes.add(storageType);

            return (J.Expression)translator.translateList(nodes, storageTypes).get(0);
        }


        /// <summary>
        /// Boxes a sequence whose rows are a primitive, and returns it unchanged otherwise.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <returns></returns>
        /// <remarks>
        /// A join selector takes its rows boxed, because Calcite builds it against a linq4j Function whose
        /// arguments are erased to Object, and because an outer join compares a row to null, which a primitive
        /// cannot be. The sequence has to agree: a delegate is typed where the Java interface was not.
        /// </remarks>
        public static System.Linq.Expressions.Expression BoxRows(PhysType physType, System.Linq.Expressions.Expression source)
        {
            var boxed = ClrTypes.Resolve(J.Primitive.box(physType.getJavaRowType()));

            // what the sequence holds, not what the physical type calls a field: a node hands its rows up
            // boxed already, and only a sequence built inside this one can still be carrying a primitive
            var rowType = source.Type.IsGenericType && source.Type.GetGenericTypeDefinition() == typeof(System.Collections.Generic.IEnumerable<>)
                ? source.Type.GetGenericArguments()[0]
                : boxed;

            if (rowType == boxed || rowType.IsValueType == false)
                return source;

            var row = System.Linq.Expressions.Expression.Parameter(rowType, "row");

            return System.Linq.Expressions.Expression.Call(null,
                Runtime.ClrBuiltInMethod.Select.MakeGenericMethod(rowType, boxed),
                source,
                System.Linq.Expressions.Expression.Lambda(JavaCast.To(row, boxed), row));
        }

        /// <summary>
        /// Returns the linq4j join type a relational one means.
        /// </summary>
        /// <param name="joinType"></param>
        /// <returns></returns>
        /// <remarks>
        /// EnumUtils.toLinq4jJoinType is package private, and is one name against another.
        /// </remarks>
        public static org.apache.calcite.linq4j.JoinType ToLinq4jJoinType(JoinRelType joinType)
        {
            return joinType.name() switch
            {
                nameof(JoinRelType.INNER) => org.apache.calcite.linq4j.JoinType.INNER,
                nameof(JoinRelType.LEFT) => org.apache.calcite.linq4j.JoinType.LEFT,
                nameof(JoinRelType.RIGHT) => org.apache.calcite.linq4j.JoinType.RIGHT,
                nameof(JoinRelType.FULL) => org.apache.calcite.linq4j.JoinType.FULL,
                nameof(JoinRelType.SEMI) => org.apache.calcite.linq4j.JoinType.SEMI,
                nameof(JoinRelType.ANTI) => org.apache.calcite.linq4j.JoinType.ANTI,
                nameof(JoinRelType.LEFT_MARK) => org.apache.calcite.linq4j.JoinType.LEFT_MARK,
                _ => throw new System.NotSupportedException($"There is no linq4j join type for {joinType.name()}.")
            };
        }

        /// <summary>
        /// Returns the lambda that appends a mark join's marker to a row of its input.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="resultPhysType"></param>
        /// <param name="inputPhysType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The counterpart of <c>EnumUtils.markJoinSelector</c>. A mark join returns the left row with one
        /// column added, and that column is three-valued: true where some right row matched, false where none
        /// did, null where a comparison was unknown. So the marker is a <c>java.lang.Boolean</c> and not a
        /// <see cref="bool"/>, and the row is boxed as every other join here boxes it.
        /// </remarks>
        public static LambdaExpression MarkJoinSelector(ClrEnumerableRelImplementor implementor, PhysType resultPhysType, PhysType inputPhysType)
        {
            var javaRowType = J.Primitive.box(inputPhysType.getJavaRowType());
            var input_ = J.Expressions.parameter(javaRowType, "input");
            var marker_ = J.Expressions.parameter((java.lang.Class)typeof(java.lang.Boolean), "marker");

            var inputParameter = Expression.Parameter(ClrTypes.Resolve(javaRowType), "input");
            var markerParameter = Expression.Parameter(typeof(java.lang.Boolean), "marker");
            implementor.Translator.Bind(input_, inputParameter);
            implementor.Translator.Bind(marker_, markerParameter);

            var expressions = new java.util.ArrayList();
            var inputFieldCount = inputPhysType.getRowType().getFieldCount();
            for (int i = 0; i < inputFieldCount; i++)
                expressions.add(inputPhysType.fieldReference(input_, i));

            expressions.add(marker_);

            var rowType = ClrTypes.Resolve(resultPhysType.getJavaRowType());

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(inputParameter.Type, markerParameter.Type, rowType),
                implementor.Translator.Translate(resultPhysType.record(expressions)),
                inputParameter,
                markerParameter);
        }

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

                parameters[ord] = Expression.Parameter(ClrTypes.Resolve(javaRowType), ord == 0 ? "left" : "right");
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

            var rowType = ClrTypes.Resolve(physType.getJavaRowType());

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
            return GeneratePredicate(implementor, rexBuilder, left, right, leftPhysType, rightPhysType, condition, false);
        }

        /// <summary>
        /// Returns the lambda that tests a join condition, answering three-valued where
        /// <paramref name="nullable"/> is set.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="rexBuilder"></param>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="leftPhysType"></param>
        /// <param name="rightPhysType"></param>
        /// <param name="condition"></param>
        /// <param name="nullable">Whether an unknown comparison answers null rather than false.</param>
        /// <returns></returns>
        /// <remarks>
        /// A mark join needs the third value: its marker is null where a comparison was unknown, which is what
        /// makes <c>x IN (…)</c> answer UNKNOWN rather than FALSE. Every other join here folds unknown into
        /// false. That is <c>Predicate2</c> against <c>NullablePredicate2</c> in Calcite, and
        /// <see cref="bool"/> against <c>java.lang.Boolean</c> here.
        /// </remarks>
        public static LambdaExpression GeneratePredicate(ClrEnumerableRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, PhysType leftPhysType, PhysType rightPhysType, RexNode condition, bool nullable)
        {
            var left_ = J.Expressions.parameter(leftPhysType.getJavaRowType(), "left");
            var right_ = J.Expressions.parameter(rightPhysType.getJavaRowType(), "right");

            // the rows arrive boxed, because the sequence a join runs over is boxed for the selector, so the
            // predicate takes them boxed and unboxes on the way in
            var leftParameter = Expression.Parameter(ClrTypes.Resolve(J.Primitive.box(leftPhysType.getJavaRowType())), "left");
            var rightParameter = Expression.Parameter(ClrTypes.Resolve(J.Primitive.box(rightPhysType.getJavaRowType())), "right");

            var leftRow = Expression.Variable(ClrTypes.Resolve(leftPhysType.getJavaRowType()), "leftRow");
            var rightRow = Expression.Variable(ClrTypes.Resolve(rightPhysType.getJavaRowType()), "rightRow");
            implementor.Translator.Bind(left_, leftRow);
            implementor.Translator.Bind(right_, rightRow);

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
                        implementor.Conformance,
                        nullable)));

            var resultType = nullable ? typeof(java.lang.Boolean) : typeof(bool);

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(leftParameter.Type, rightParameter.Type, resultType),
                Expression.Block(resultType, [leftRow, rightRow],
                    Expression.Assign(leftRow, JavaCast.To(leftParameter, leftRow.Type)),
                    Expression.Assign(rightRow, JavaCast.To(rightParameter, rightRow.Type)),
                    implementor.Translator.TranslateBody(builder.toBlock(), resultType)),
                leftParameter,
                rightParameter);
        }

    }

}
