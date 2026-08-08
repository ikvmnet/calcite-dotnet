using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
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
        public static LambdaExpression MarkJoinSelector(IClrRelImplementor implementor, ClrPhysType resultPhysType, ClrPhysType inputPhysType)
        {
            // a Function always takes boxed arguments, so a row that is a primitive is boxed here
            var input = Expression.Parameter(inputPhysType.RowType, "input");
            var marker = Expression.Parameter(typeof(java.lang.Boolean), "marker");

            var expressions = new List<Expression>();
            var inputFieldCount = inputPhysType.RelRowType.getFieldCount();
            for (int i = 0; i < inputFieldCount; i++)
                expressions.Add(inputPhysType.FieldReference(input, i));

            expressions.Add(marker);

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(input.Type, marker.Type, resultPhysType.RowType),
                resultPhysType.Record(expressions),
                input,
                marker);
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
        public static LambdaExpression JoinSelector(IClrRelImplementor implementor, JoinRelType joinType, ClrPhysType physType, ClrPhysType left, ClrPhysType right)
        {
            var outputFieldCount = physType.RelRowType.getFieldCount();
            var inputs = new[] { left, right };

            var parameters = new ParameterExpression[2];
            var expressions = new List<Expression>();

            for (int ord = 0; ord < inputs.Length; ord++)
            {
                var inputPhysType = inputs[ord].MakeNullable(joinType.generatesNullsOn(ord));

                // a Function always takes boxed arguments, so a row that is a primitive is boxed here
                var row = Expression.Parameter(inputPhysType.RowType, ord == 0 ? "left" : "right");
                parameters[ord] = row;

                // a semi join returns the left input alone, so the fields run out before the inputs do
                if (expressions.Count == outputFieldCount)
                    continue;

                var fieldCount = inputPhysType.RelRowType.getFieldCount();
                for (int i = 0; i < fieldCount; i++)
                {
                    var expression = inputPhysType.FieldReference(row, i, physType.FieldType(expressions.Count));

                    // the whole row is null on the side an outer join generates nulls for, so every field of
                    // it is, and the storage type asked for above is a box for exactly that reason
                    if (joinType.generatesNullsOn(ord))
                        expression = NullIfNull(row, expression);

                    expressions.Add(expression);
                }
            }

            return Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(parameters[0].Type, parameters[1].Type, physType.RowType),
                physType.Record(expressions),
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
        public static LambdaExpression GeneratePredicate(IClrRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, ClrPhysType leftPhysType, ClrPhysType rightPhysType, RexNode condition)
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
        public static LambdaExpression GeneratePredicate(IClrRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, ClrPhysType leftPhysType, ClrPhysType rightPhysType, RexNode condition, bool nullable)
        {
            // the condition is Rex, so it is translated by Calcite against Calcite's own physical types;
            // those are built here rather than carried, because this is the only thing in a join that needs one
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftPhysType.RelRowType, leftPhysType.Format, false);
            var rightCalcite = PhysTypeImpl.of(implementor.TypeFactory, rightPhysType.RelRowType, rightPhysType.Format, false);

            var left_ = J.Expressions.parameter(leftCalcite.getJavaRowType(), "left");
            var right_ = J.Expressions.parameter(rightCalcite.getJavaRowType(), "right");

            // the rows arrive boxed, because the sequence a join runs over is boxed for the selector, so the
            // predicate takes them boxed and unboxes on the way in
            var leftParameter = Expression.Parameter(leftPhysType.RowType, "left");
            var rightParameter = Expression.Parameter(rightPhysType.RowType, "right");

            // unboxed, because the linq4j parameter the Rex condition is built against is the Java row
            // class and the two have to be the same type for the binding to mean anything
            var leftRow = Expression.Variable(ClrTypes.Resolve(leftCalcite.getJavaRowType()), "leftRow");
            var rightRow = Expression.Variable(ClrTypes.Resolve(rightCalcite.getJavaRowType()), "rightRow");
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
            inputs.put(left_, leftCalcite);
            inputs.put(right_, rightCalcite);

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
                    Expression.Assign(leftRow, ClrEnumUtils.Convert(leftParameter, leftRow.Type)),
                    Expression.Assign(rightRow, ClrEnumUtils.Convert(rightParameter, rightRow.Type)),
                    implementor.Translator.TranslateBody(builder.toBlock(), resultType)),
                leftParameter,
                rightParameter);
        }


        /// <summary>
        /// Returns an expression yielding <paramref name="expression"/> as <paramref name="type"/>.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public static Expression Convert(Expression expression, Type type)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(type);

            if (expression.Type == type)
                return expression;

            if (type == typeof(void))
                return expression;

            var fromPrimitive = ClrPrimitive.Of(expression.Type);
            var toPrimitive = ClrPrimitive.Of(type);
            var fromBox = ClrPrimitive.OfBox(expression.Type);

            // int to Integer, and int to Long by way of long, exactly as Java widens before it boxes
            if (fromPrimitive != null && ClrPrimitive.OfBox(type) is J.Primitive toBox)
                return Box(Number(expression, toBox), toBox);

            // Integer to int, and Integer to long by way of int
            if (fromBox != null && toPrimitive != null)
                return Number(Unbox(expression, fromBox), toPrimitive);

            // int to Object, Number, Comparable: box first, then the reference conversion is ordinary
            if (fromPrimitive != null && type.IsValueType == false)
                return Expression.Convert(Box(expression, fromPrimitive), type);

            // Object to int, which Java reads as a cast to the box class followed by an unboxing call. A
            // straight Convert would emit unbox.any and demand a boxed CLR int that is never the value here.
            if (expression.Type.IsValueType == false && toPrimitive != null)
                return Unbox(Expression.Convert(expression, ClrTypes.FromClass(toPrimitive.boxClass)), toPrimitive);

            if (fromPrimitive != null && toPrimitive != null)
                return Widen(expression, type);

            return Expression.Convert(expression, type);
        }
        /// <summary>
        /// Returns an expression yielding <paramref name="expression"/> as <paramref name="toType"/>, reading
        /// it as <paramref name="fromType"/> where that differs from the type it already has.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="fromType">The type the value is to be read as, or null for the one it has.</param>
        /// <param name="toType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumUtils.convert(operand, fromType, toType)</c>. The only caller that gives a
        /// <paramref name="fromType"/> of its own is <c>PhysType.fieldReference</c>, and it gives one only for
        /// <c>java.sql.Date</c>, <c>Time</c> and <c>Timestamp</c> — the three types a row stores as an int or a
        /// long rather than as themselves. That is the whole of what this adds over the two argument form.
        ///
        /// <para>The conversion is taken only where the expression really has the type named. Where a row holds
        /// its fields as objects the access is an <see cref="object"/>, and Calcite is in the same position: it
        /// names <c>SqlFunctions.toInt(Date)</c> and Janino resolves <c>toInt(Object)</c> from the source text,
        /// because a linq4j call has a recorded method that is only advisory. Falling through to the two
        /// argument form is that resolution.</para>
        /// </remarks>
        public static Expression Convert(Expression expression, Type? fromType, Type toType)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(toType);

            if (fromType != null && fromType != expression.Type && fromType.IsAssignableFrom(expression.Type))
            {
                var method = InternalOf(fromType, toType);
                if (method != null)
                    return Expression.Call(null, method, Convert(expression, fromType));
            }

            return Convert(expression, toType);
        }

        /// <summary>
        /// Returns the method reading a datetime as the value a row stores it as, or null where there is none.
        /// </summary>
        /// <param name="fromType"></param>
        /// <param name="toType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumUtils.toInternal</c>, which is private. A <c>Date</c> and a <c>Time</c> are stored as an int
        /// and a <c>Timestamp</c> as a long, and each has a second method for the boxed form, which carries a
        /// null through.
        /// </remarks>
        static MethodInfo? InternalOf(Type fromType, Type toType)
        {
            if (fromType == typeof(java.sql.Date))
                return toType == typeof(int) ? DateToInt : toType == typeof(java.lang.Integer) ? DateToIntOptional : null;

            if (fromType == typeof(java.sql.Time))
                return toType == typeof(int) ? TimeToInt : toType == typeof(java.lang.Integer) ? TimeToIntOptional : null;

            if (fromType == typeof(java.sql.Timestamp))
                return toType == typeof(long) ? TimestampToLong : toType == typeof(java.lang.Long) ? TimestampToLongOptional : null;

            return null;
        }

        /// <summary>
        /// The members Calcite names through <c>BuiltInMethod</c> for a datetime as a row stores it.
        /// </summary>
        static readonly MethodInfo DateToInt = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATE_TO_INT.method);

        /// <inheritdoc cref="DateToInt" />
        static readonly MethodInfo DateToIntOptional = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATE_TO_INT_OPTIONAL.method);

        /// <inheritdoc cref="DateToInt" />
        static readonly MethodInfo TimeToInt = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.TIME_TO_INT.method);

        /// <inheritdoc cref="DateToInt" />
        static readonly MethodInfo TimeToIntOptional = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.TIME_TO_INT_OPTIONAL.method);

        /// <inheritdoc cref="DateToInt" />
        static readonly MethodInfo TimestampToLong = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.TIMESTAMP_TO_LONG.method);

        /// <inheritdoc cref="DateToInt" />
        static readonly MethodInfo TimestampToLongOptional = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.TIMESTAMP_TO_LONG_OPTIONAL.method);

        /// <summary>
        /// Returns an expression yielding null where the given value is null, and the given body otherwise.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="body"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>Expressions.condition(Expressions.equal(value, NULL), NULL, body)</c>, which the accessors of
        /// <c>PhysTypeImpl</c> write once per key field.
        ///
        /// <para>A value that is a primitive is never null, and comparing one to null is not Java at all.
        /// Calcite writes the comparison anyway and relies on <c>OptimizeShuttle</c> to take it out again
        /// before Janino sees it. There is no shuttle over an expression tree, and the CLR would convert the
        /// null to a primitive and throw, so the test is not written where it cannot hold.</para>
        /// </remarks>
        public static Expression NullIfNull(Expression value, Expression body)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(body);

            if (value.Type.IsValueType)
                return body;

            return Expression.Condition(
                Expression.Equal(value, Expression.Constant(null, value.Type)),
                Expression.Constant(null, body.Type),
                body,
                body.Type);
        }

        /// <summary>
        /// Returns the expression yielding a collator for a collation, or null where the collation has none.
        /// </summary>
        /// <param name="collation"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumUtils.generateCollatorExpression</c>, which is public and answers in linq4j.
        /// <c>PhysTypeImpl</c> reaches it by a static import, so it lives here rather than there.
        /// </remarks>
        public static Expression? GenerateCollatorExpression(org.apache.calcite.sql.SqlCollation? collation)
        {
            var collator = collation?.getCollator();
            if (collator == null)
                return null;

            var locale = collation!.getLocale();

            return Expression.Call(null, GenerateCollator,
                Expression.New(LocaleConstructor,
                    Expression.Constant(locale.getLanguage()),
                    Expression.Constant(locale.getCountry()),
                    Expression.Constant(locale.getVariant())),
                Expression.Constant(collator.getStrength()));
        }

        /// <summary>
        /// <c>Utilities.generateCollator</c>, and the locale it takes.
        /// </summary>
        static readonly MethodInfo GenerateCollator = typeof(org.apache.calcite.runtime.Utilities)
            .GetMethod("generateCollator", BindingFlags.Public | BindingFlags.Static)!;

        /// <inheritdoc cref="GenerateCollator" />
        static readonly System.Reflection.ConstructorInfo LocaleConstructor = typeof(java.util.Locale)
            .GetConstructor([typeof(string), typeof(string), typeof(string)])!;

        /// <summary>
        /// Converts between two primitives, and returns <paramref name="expression"/> when they agree.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Number(Expression expression, J.Primitive primitive)
        {
            return Widen(expression, ClrTypes.FromClass(primitive.primitiveClass));
        }

        /// <summary>
        /// Converts between two primitives.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// Java's byte is signed and IKVM stores it in a CLR byte, which is not. Widening one therefore has to
        /// go by way of an sbyte, or -1 becomes 255 the moment it is promoted to an int.
        /// </remarks>
        static Expression Widen(Expression expression, Type type)
        {
            if (expression.Type == type)
                return expression;

            if (expression.Type == typeof(byte))
                expression = Expression.Convert(expression, typeof(sbyte));

            return expression.Type == type ? expression : Expression.Convert(expression, type);
        }

        /// <summary>
        /// Boxes a primitive into its Java box class.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Box(Expression expression, J.Primitive primitive)
        {
            var box = ClrTypes.FromClass(primitive.boxClass);
            var valueOf = box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [expression.Type], null)
                ?? throw new NotSupportedException($"'{box}' has no valueOf for '{expression.Type}'.");

            return Expression.Call(null, valueOf, expression);
        }

        /// <summary>
        /// Unboxes a Java box class into its primitive.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="primitive"></param>
        /// <returns></returns>
        static Expression Unbox(Expression expression, J.Primitive primitive)
        {
            // intValue, booleanValue, charValue, and so on for each of the eight
            var name = primitive.primitiveName + "Value";
            var value = expression.Type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, [], null)
                ?? throw new NotSupportedException($"'{expression.Type}' has no {name}().");

            return Expression.Call(expression, value);
        }
    }

}
