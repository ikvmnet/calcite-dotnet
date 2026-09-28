using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Ports of <c>EnumUtils</c> members used by this convention's nodes, most of which Calcite declares
    /// package private, plus the value conversions expression trees need where Java converts implicitly.
    /// </summary>
    static class ClrEnumUtils
    {

        /// <summary>
        /// Returns the Java class of a relational type, or <c>Object[]</c> if the type factory gives no class.
        /// Mirrors <c>EnumUtils.javaClass</c>.
        /// </summary>
        /// <param name="typeFactory">The type factory that maps the relational type.</param>
        /// <param name="type">The relational type.</param>
        /// <returns>The type factory's class for <paramref name="type"/> if it is a <c>java.lang.Class</c>, otherwise <c>Object[]</c>.</returns>
        public static java.lang.reflect.Type JavaClass(org.apache.calcite.adapter.java.JavaTypeFactory typeFactory, org.apache.calcite.rel.type.RelDataType type)
        {
            var clazz = typeFactory.getJavaClass(type);

            return clazz is java.lang.Class ? clazz : (java.lang.Class)typeof(object[]);
        }

        /// <summary>
        /// Returns the relational types of the listed input fields. Mirrors <c>EnumUtils.fieldRowTypes</c>.
        /// </summary>
        /// <param name="inputRowType">The input row type.</param>
        /// <param name="argList">The field ordinals, a list of <c>java.lang.Integer</c>; each must be an input field.</param>
        /// <returns>A list of <see cref="org.apache.calcite.rel.type.RelDataType"/>, one per ordinal, in order.</returns>
        public static java.util.List FieldRowTypes(org.apache.calcite.rel.type.RelDataType inputRowType, java.util.List argList)
        {
            return FieldRowTypes(inputRowType, null, argList);
        }

        /// <summary>
        /// Returns the relational types of the listed fields, where an ordinal past the input's fields names
        /// one of <paramref name="extraInputs"/>, such as a window's constants. Mirrors
        /// <c>EnumUtils.fieldRowTypes</c>.
        /// </summary>
        /// <param name="inputRowType">The input row type.</param>
        /// <param name="extraInputs">Further inputs as row expressions, or <see langword="null"/>.</param>
        /// <param name="argList">The field ordinals, a list of integers.</param>
        /// <exception cref="ArgumentNullException">An ordinal is past the input's fields and
        /// <paramref name="extraInputs"/> is <see langword="null"/>.</exception>
        /// <returns>A list of <see cref="org.apache.calcite.rel.type.RelDataType"/>, one per ordinal, in order.</returns>
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
        /// Returns the Java classes of a list of relational types, as <see cref="JavaClass"/> gives them.
        /// Mirrors <c>EnumUtils.fieldTypes</c>.
        /// </summary>
        /// <param name="typeFactory">The type factory that maps each relational type.</param>
        /// <param name="inputTypes">A list of <see cref="org.apache.calcite.rel.type.RelDataType"/>.</param>
        /// <returns>A list of Java types, one per element of <paramref name="inputTypes"/>, in order.</returns>
        public static java.util.List FieldTypes(org.apache.calcite.adapter.java.JavaTypeFactory typeFactory, java.util.List inputTypes)
        {
            var types = new java.util.ArrayList(inputTypes.size());

            for (int i = 0; i < inputTypes.size(); i++)
                types.add(JavaClass(typeFactory, (org.apache.calcite.rel.type.RelDataType)inputTypes.get(i)));

            return types;
        }

        /// <summary>
        /// Translates one row expression to linq4j.
        /// </summary>
        /// <param name="translator">The translator.</param>
        /// <param name="node">The row expression.</param>
        /// <param name="storageType">The type wanted, or <see langword="null"/> for the expression's own.</param>
        /// <returns>The translated expression.</returns>
        /// <remarks>
        /// <c>RexToLixTranslator.translate</c> is package private. <c>translateList(operands, storageTypes)</c>
        /// calls it once per element, so a list of one is the same call.
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
        /// Returns the linq4j join type of the same name. Mirrors the package private
        /// <c>EnumUtils.toLinq4jJoinType</c>.
        /// </summary>
        /// <exception cref="NotSupportedException">The join type has no linq4j counterpart.</exception>
        /// <param name="joinType">The relational join type.</param>
        /// <returns>The linq4j <c>JoinType</c> with the same name.</returns>
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
        /// Returns a lambda that appends a mark join's marker to a left row.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumUtils.markJoinSelector</c>. The marker is three-valued, true where a right row
        /// matched, false where none did and null where the condition was unknown, so it is a
        /// <c>java.lang.Boolean</c>. The row parameter is the boxed row type, as in every join.
        /// </remarks>
        /// <param name="implementor">The implementor; not read, and kept so the signature matches the other selectors.</param>
        /// <param name="resultPhysType">The physical type of the output row, which is the left row's fields followed by the marker.</param>
        /// <param name="inputPhysType">The physical type of the left row.</param>
        /// <returns>A lambda taking the boxed left row and a <c>java.lang.Boolean</c> marker, and returning the output row.</returns>
        public static LambdaExpression MarkJoinSelector(ClrCursorRelImplementor implementor, ClrPhysType resultPhysType, ClrPhysType inputPhysType)
        {
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
        /// Returns a lambda that builds an output row from a left and a right row.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumUtils.joinSelector</c>. For rows with many fields Calcite switches to
        /// <c>joinSelectorCompact</c> to keep generated Java methods under the class file size limit; an
        /// expression tree has no such limit, so only the ordinary form is ported. The row parameters are the
        /// inputs' boxed row types, so the side an outer join generates nulls on can be null.
        /// </remarks>
        /// <param name="implementor">The implementor; not read, and kept so the signature matches the other selectors.</param>
        /// <param name="joinType">The join type; for a semi or anti join only the left row's fields are output.</param>
        /// <param name="physType">The physical type of the output row.</param>
        /// <param name="left">The physical type of the left row.</param>
        /// <param name="right">The physical type of the right row.</param>
        /// <returns>A lambda taking the boxed left and right rows and returning the output row.</returns>
        public static LambdaExpression JoinSelector(ClrCursorRelImplementor implementor, JoinRelType joinType, ClrPhysType physType, ClrPhysType left, ClrPhysType right)
        {
            var outputFieldCount = physType.RelRowType.getFieldCount();
            var inputs = new[] { left, right };

            var parameters = new ParameterExpression[2];
            var expressions = new List<Expression>();

            for (int ord = 0; ord < inputs.Length; ord++)
            {
                var inputPhysType = inputs[ord].MakeNullable(joinType.generatesNullsOn(ord));

                var row = Expression.Parameter(inputPhysType.RowType, ord == 0 ? "left" : "right");
                parameters[ord] = row;

                // a semi or anti join outputs only the left row, so the right contributes no fields
                if (expressions.Count == outputFieldCount)
                    continue;

                var fieldCount = inputPhysType.RelRowType.getFieldCount();
                for (int i = 0; i < fieldCount; i++)
                {
                    var expression = inputPhysType.FieldReference(row, i, physType.FieldType(expressions.Count));

                    // on the side an outer join generates nulls for, the whole row may be null, and then so is
                    // each of its fields
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
        /// Returns a <c>Func&lt;left, right, bool&gt;</c> lambda testing a join condition, in which an unknown
        /// result is false. Mirrors <c>EnumUtils.generatePredicate</c>.
        /// </summary>
        /// <param name="implementor">The implementor that translates the condition.</param>
        /// <param name="rexBuilder">The builder used while translating the condition.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="leftPhysType">The physical type of a left row.</param>
        /// <param name="rightPhysType">The physical type of a right row.</param>
        /// <param name="condition">The join condition, over the left fields followed by the right fields.</param>
        /// <returns>A lambda taking a left and a right row and returning <see langword="true"/> only where the condition is true.</returns>
        public static LambdaExpression GeneratePredicate(ClrCursorRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, ClrPhysType leftPhysType, ClrPhysType rightPhysType, RexNode condition)
        {
            return GeneratePredicate(implementor, rexBuilder, left, right, leftPhysType, rightPhysType, condition, false);
        }

        /// <summary>
        /// Returns a lambda testing a join condition over a left and a right row. Mirrors
        /// <c>EnumUtils.generatePredicate</c>.
        /// </summary>
        /// <param name="implementor">The implementor.</param>
        /// <param name="rexBuilder">The row expression builder.</param>
        /// <param name="left">The left input.</param>
        /// <param name="right">The right input.</param>
        /// <param name="leftPhysType">The left input's physical type.</param>
        /// <param name="rightPhysType">The right input's physical type.</param>
        /// <param name="condition">The condition, over the concatenated input fields.</param>
        /// <param name="nullable">Whether an unknown result is null, giving a <c>java.lang.Boolean</c> result,
        /// rather than false, giving a <see cref="bool"/>.</param>
        /// <returns>A lambda over the two inputs' boxed row types.</returns>
        /// <remarks>
        /// A mark join passes <paramref name="nullable"/> <see langword="true"/>, so that its marker is null
        /// where the condition is unknown and <c>x IN (...)</c> can answer <c>UNKNOWN</c>.
        /// </remarks>
        public static LambdaExpression GeneratePredicate(ClrCursorRelImplementor implementor, RexBuilder rexBuilder, RelNode left, RelNode right, ClrPhysType leftPhysType, ClrPhysType rightPhysType, RexNode condition, bool nullable)
        {
            // Calcite's translator needs Calcite physical types for the inputs
            var leftCalcite = PhysTypeImpl.of(implementor.TypeFactory, leftPhysType.RelRowType, leftPhysType.Format, false);
            var rightCalcite = PhysTypeImpl.of(implementor.TypeFactory, rightPhysType.RelRowType, rightPhysType.Format, false);

            var left_ = J.Expressions.parameter(leftCalcite.getJavaRowType(), "left");
            var right_ = J.Expressions.parameter(rightCalcite.getJavaRowType(), "right");

            // the lambda takes the boxed rows the join's cursors carry
            var leftParameter = Expression.Parameter(leftPhysType.RowType, "left");
            var rightParameter = Expression.Parameter(rightPhysType.RowType, "right");

            // and unboxes them into variables of the Java row class, the type the linq4j parameters are bound to
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
                        nullable,
                        implementor.RexImplementorTable)));

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
        /// Returns an expression converting <paramref name="expression"/> to <paramref name="type"/> as Java
        /// would: boxing and unboxing through the Java box classes, widening primitives, and reading an
        /// <see cref="object"/> or <see cref="string"/> as a <c>java.lang.Number</c> through
        /// <c>SqlFunctions.toBigDecimal</c>.
        /// </summary>
        /// <param name="expression">The value.</param>
        /// <param name="type">The type wanted; <see cref="void"/> returns the expression unchanged.</param>
        /// <returns>The converted expression, or <paramref name="expression"/> if no conversion is needed.</returns>
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

            // an object or string wanted as a Number is converted with toBigDecimal, as EnumUtils.convert
            // does, so a non-numeric value fails with a message naming it rather than a cast exception
            if (type == typeof(java.lang.Number) && (expression.Type == typeof(object) || expression.Type == typeof(string)))
                return Expression.Condition(
                    Expression.Equal(expression, Expression.Constant(null, expression.Type)),
                    Expression.Constant(null, typeof(java.math.BigDecimal)),
                    Expression.Call(null, ToBigDecimal, Convert(expression, typeof(object))),
                    typeof(java.math.BigDecimal));

            // int to Integer, and int to Long by way of long: Java widens before it boxes
            if (fromPrimitive != null && ClrPrimitive.OfBox(type) is J.Primitive toBox)
                return Box(Number(expression, toBox), toBox);

            // Integer to int, and Integer to long by way of int
            if (fromBox != null && toPrimitive != null)
                return Number(Unbox(expression, fromBox), toPrimitive);

            // int to Object, Number or Comparable: box, then convert the reference
            if (fromPrimitive != null && type.IsValueType == false)
                return Expression.Convert(Box(expression, fromPrimitive), type);

            // Object to int: cast to the Java box class, then call its unboxing method. Expression.Convert
            // would emit unbox.any, which requires a boxed CLR int, and the value is a Java box
            if (expression.Type.IsValueType == false && toPrimitive != null)
                return Unbox(Expression.Convert(expression, ClrTypes.FromClass(toPrimitive.boxClass)), toPrimitive);

            if (fromPrimitive != null && toPrimitive != null)
                return Widen(expression, type);

            return Expression.Convert(expression, type);
        }
        /// <summary>
        /// Returns an expression converting <paramref name="expression"/>, read as <paramref name="fromType"/>,
        /// to <paramref name="toType"/>.
        /// </summary>
        /// <param name="expression">The value.</param>
        /// <param name="fromType">The type to read the value as, or <see langword="null"/> for its own type.</param>
        /// <param name="toType">The type wanted.</param>
        /// <returns>The converted expression.</returns>
        /// <remarks>
        /// Mirrors <c>EnumUtils.convert(operand, fromType, toType)</c>. The only case this adds over
        /// <see cref="Convert(Expression, Type)"/> is a <c>java.sql.Date</c>, <c>Time</c> or <c>Timestamp</c>
        /// read as the <see cref="int"/> or <see cref="long"/> a row stores it as. That conversion is applied
        /// only where the expression's static type is assignable to <paramref name="fromType"/>; an
        /// <see cref="object"/> field falls through to the two-argument form, matching the overload Janino
        /// resolves for Calcite from the generated source.
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
        /// Returns the method converting a date, time or timestamp to its internal representation, or
        /// <see langword="null"/> if there is none for the pair of types.
        /// </summary>
        /// <remarks>
        /// Mirrors the private <c>EnumUtils.toInternal</c>. A <c>Date</c> or <c>Time</c> is stored as an
        /// <see cref="int"/> and a <c>Timestamp</c> as a <see cref="long"/>; the boxed variant of each method
        /// passes a null through.
        /// </remarks>
        /// <param name="fromType">The external type: <c>java.sql.Date</c>, <c>Time</c> or <c>Timestamp</c>.</param>
        /// <param name="toType">The internal type: <see cref="int"/>, <see cref="long"/> or their Java boxes.</param>
        /// <returns>The <c>BuiltInMethod</c> conversion for the pair, or <see langword="null"/>.</returns>
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
        /// <c>SqlFunctions.toBigDecimal(Object)</c>.
        /// </summary>
        static readonly MethodInfo ToBigDecimal = typeof(org.apache.calcite.runtime.SqlFunctions).GetMethod(nameof(org.apache.calcite.runtime.SqlFunctions.toBigDecimal), [typeof(object)]) ?? throw new java.lang.NoSuchMethodError("SqlFunctions.toBigDecimal(Object)");

        /// <summary>
        /// <c>BuiltInMethod</c> members converting a date, time or timestamp to its internal representation.
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
        /// Returns an expression that is null if <paramref name="value"/> is null and
        /// <paramref name="body"/> otherwise, with a primitive body boxed.
        /// </summary>
        /// <param name="value">The value tested for null.</param>
        /// <param name="body">The result where <paramref name="value"/> is not null.</param>
        /// <returns>The conditional expression, or <paramref name="body"/> if <paramref name="value"/> is of a
        /// value type.</returns>
        /// <remarks>
        /// The counterpart of Calcite's <c>value == null ? null : body</c>. Where the value is a primitive,
        /// Calcite writes the test anyway and <c>OptimizeShuttle</c> removes it; no such pass runs over an
        /// expression tree, so the test is omitted here.
        /// </remarks>
        public static Expression NullIfNull(Expression value, Expression body)
        {
            ArgumentNullException.ThrowIfNull(value);
            ArgumentNullException.ThrowIfNull(body);

            if (value.Type.IsValueType)
                return body;

            // Java types `c ? null : body` with a primitive body as its box (JLS 15.25), which Calcite relies
            // on, for example where an outer join's selector reads an int field of a CUSTOM row
            var type = ClrPrimitive.Box(body.Type);

            return Expression.Condition(
                Expression.Equal(value, Expression.Constant(null, value.Type)),
                Expression.Constant(null, type),
                Convert(body, type),
                type);
        }

        /// <summary>
        /// Returns an expression creating the <c>java.text.Collator</c> for a SQL collation, or
        /// <see langword="null"/> if the collation has none.
        /// </summary>
        /// <param name="collation">The SQL collation, or <see langword="null"/>.</param>
        /// <returns>The collator expression, or <see langword="null"/>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumUtils.generateCollatorExpression</c>, which returns linq4j.
        /// </remarks>
        public static Expression? GenerateCollatorExpression(org.apache.calcite.sql.SqlCollation? collation)
        {
            if (collation == null || collation.getCollator() is not java.text.Collator collator)
                return null;

            var locale = collation.getLocale();

            return Expression.Call(null, GenerateCollator,
                Expression.New(LocaleConstructor,
                    Expression.Constant(locale.getLanguage()),
                    Expression.Constant(locale.getCountry()),
                    Expression.Constant(locale.getVariant())),
                Expression.Constant(collator.getStrength()));
        }

        /// <summary>
        /// <c>Utilities.generateCollator</c>.
        /// </summary>
        static readonly MethodInfo GenerateCollator = typeof(org.apache.calcite.runtime.Utilities)
            
            .GetMethod("generateCollator", BindingFlags.Public | BindingFlags.Static)
            ?? throw new InvalidOperationException("Utilities has no generateCollator().");

        /// <summary>
        /// The <c>java.util.Locale(String, String, String)</c> constructor.
        /// </summary>
        static readonly System.Reflection.ConstructorInfo LocaleConstructor = typeof(java.util.Locale)
            
            .GetConstructor([typeof(string), typeof(string), typeof(string)])
            ?? throw new InvalidOperationException("java.util.Locale has no (String, String, String) constructor.");

        /// <summary>
        /// Converts a primitive to another primitive type, returning the expression unchanged if it already
        /// has that type.
        /// </summary>
        /// <param name="expression">An expression of a primitive type.</param>
        /// <param name="primitive">The Java primitive to convert to.</param>
        /// <returns>An expression of the CLR type of <paramref name="primitive"/>.</returns>
        static Expression Number(Expression expression, J.Primitive primitive)
        {
            return Widen(expression, ClrTypes.FromClass(primitive.primitiveClass));
        }

        /// <summary>
        /// Converts a primitive to another primitive type.
        /// </summary>
        /// <remarks>
        /// IKVM represents Java's signed <c>byte</c> as the unsigned CLR <see cref="byte"/>, so a byte is
        /// converted through <see cref="sbyte"/> to keep its sign.
        /// </remarks>
        /// <param name="expression">An expression of a primitive type.</param>
        /// <param name="type">The CLR primitive type to convert to.</param>
        /// <returns>An expression of <paramref name="type"/>, or <paramref name="expression"/> itself if it already has that type.</returns>
        static Expression Widen(Expression expression, Type type)
        {
            if (expression.Type == type)
                return expression;

            if (expression.Type == typeof(byte))
                expression = Expression.Convert(expression, typeof(sbyte));

            return expression.Type == type ? expression : Expression.Convert(expression, type);
        }

        /// <summary>
        /// Boxes a primitive into its Java box class with the box's <c>valueOf</c>.
        /// </summary>
        /// <param name="expression">An expression of a primitive type.</param>
        /// <param name="primitive">The Java primitive whose box class to use.</param>
        /// <returns>A call to the box class's <c>valueOf</c>.</returns>
        static Expression Box(Expression expression, J.Primitive primitive)
        {
            var box = ClrTypes.FromClass(primitive.boxClass);
            var valueOf = box.GetMethod("valueOf", BindingFlags.Public | BindingFlags.Static, null, [expression.Type], null)
                ?? throw new NotSupportedException($"'{box}' has no valueOf for '{expression.Type}'.");

            return Expression.Call(null, valueOf, expression);
        }

        /// <summary>
        /// Unboxes a Java box into a primitive with the box's <c>intValue</c>, <c>longValue</c> and so on.
        /// </summary>
        /// <param name="expression">An expression of a Java box type.</param>
        /// <param name="primitive">The Java primitive to unbox to.</param>
        /// <returns>A call to the box's value method for <paramref name="primitive"/>, such as <c>intValue()</c>.</returns>
        static Expression Unbox(Expression expression, J.Primitive primitive)
        {
            var name = primitive.primitiveName + "Value";
            var value = expression.Type.GetMethod(name, BindingFlags.Public | BindingFlags.Instance, null, [], null)
                ?? throw new NotSupportedException($"'{expression.Type}' has no {name}().");

            return Expression.Call(expression, value);
        }
    }

}
