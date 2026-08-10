using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Function;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="ClrPhysType"/>.
    /// </summary>
    /// <remarks>
    /// <c>PhysTypeImpl</c>, member for member, in <see cref="Expression"/>.
    ///
    /// <para>The format arrives already through <c>JavaRowFormat.optimize</c>, and every question about
    /// how a row is laid out is the format's to answer.</para>
    /// </remarks>
    public class ClrPhysTypeImpl : ClrPhysType
    {

        readonly JavaTypeFactory typeFactory;
        readonly RelDataType rowType;
        readonly JavaRowFormat format;
        readonly java.lang.reflect.Type javaRowType;
        readonly Type javaRowClass;
        readonly Type[] fieldClasses;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <param name="javaRowType">The type factory's name for a row, resolved to the CLR type behind it.</param>
        /// <param name="format">Already through <c>JavaRowFormat.optimize</c>.</param>
        /// <remarks>
        /// <c>PhysTypeImpl</c>'s constructor, which takes the row class rather than deriving it, because
        /// <c>makeNullable</c> boxes the one it has instead of asking the format again.
        /// </remarks>
        ClrPhysTypeImpl(JavaTypeFactory typeFactory, RelDataType rowType, java.lang.reflect.Type javaRowType, JavaRowFormat format)
        {
            this.typeFactory = typeFactory ?? throw new ArgumentNullException(nameof(typeFactory));
            this.rowType = rowType ?? throw new ArgumentNullException(nameof(rowType));
            this.javaRowType = javaRowType ?? throw new ArgumentNullException(nameof(javaRowType));
            this.format = format ?? throw new ArgumentNullException(nameof(format));

            javaRowClass = ClrPrimitive.Box(ClrTypes.Resolve(javaRowType));

            var fields = rowType.getFieldList();
            fieldClasses = new Type[fields.size()];
            for (int i = 0; i < fieldClasses.Length; i++)
            {
                // a field whose type has no class of its own is an object array, which is what a struct is
                var fieldType = typeFactory.getJavaClass(((RelDataTypeField)fields.get(i)).getType());
                fieldClasses[i] = ClrTypes.Resolve(fieldType is java.lang.Class ? fieldType : (java.lang.Class)typeof(object[]));
            }
        }

        /// <summary>
        /// Returns the physical type of a row, optimising the format for the row type.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <param name="format"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.of</c>.
        /// </remarks>
        public static ClrPhysType Of(JavaTypeFactory typeFactory, RelDataType rowType, JavaRowFormat format)
        {
            return Of(typeFactory, rowType, format, true);
        }

        /// <summary>
        /// Returns the physical type of a row.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <param name="format"></param>
        /// <param name="optimize">Whether the format is to be optimised for the row type.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.of</c>. <c>optimize</c> is Calcite's own, called rather than written again.
        /// </remarks>
        public static ClrPhysType Of(JavaTypeFactory typeFactory, RelDataType rowType, JavaRowFormat format, bool optimize)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);
            ArgumentNullException.ThrowIfNull(format);

            if (optimize)
                format = format.optimize(rowType);

            return new ClrPhysTypeImpl(typeFactory, rowType, format.JavaRowType(typeFactory, rowType), format);
        }

        /// <summary>
        /// Returns the physical type of a row whose type is a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="javaRowClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.of(typeFactory, javaRowClass)</c>, which is package private. The row type is rebuilt
        /// from the fields of the record and the format is left unoptimised, exactly as it does, because an
        /// accumulator of one field is still a record.
        /// </remarks>
        public static ClrPhysType Of(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(javaRowClass);

            var builder = typeFactory.builder();

            if (javaRowClass is J.Types.RecordType recordType)
            {
                var fields = recordType.getRecordFields();
                for (int i = 0; i < fields.size(); i++)
                {
                    var field = (J.Types.RecordField)fields.get(i);
                    builder.add(field.getName(), typeFactory.createType(field.getType()));
                }
            }

            // the row class given rather than one derived back out of the row type: this is the record the
            // state slots were declared against, and the emitter keys a type on reference
            return new ClrPhysTypeImpl(typeFactory, builder.build(), javaRowClass, JavaRowFormat.CUSTOM);
        }

        /// <inheritdoc />
        public JavaRowFormat Format => format;

        /// <inheritdoc />
        public JavaTypeFactory TypeFactory => typeFactory;

        /// <inheritdoc />
        public RelDataType RelRowType => rowType;

        /// <inheritdoc />
        public Type RowType => javaRowClass;

        /// <inheritdoc />
        public java.lang.reflect.Type JavaRowType => javaRowType;

        /// <inheritdoc />
        public Type FieldType(int field) => format.JavaFieldClass(typeFactory, rowType, field);

        /// <inheritdoc />
        public Type FieldClass(int field) => fieldClasses[field];

        /// <inheritdoc />
        public bool FieldNullable(int field) => ((RelDataTypeField)rowType.getFieldList().get(field)).getType().isNullable();

        /// <inheritdoc />
        public ClrPhysType Project(java.util.List integers, JavaRowFormat format)
        {
            return Project(integers, false, format);
        }

        /// <inheritdoc />
        public ClrPhysType Project(java.util.List integers, bool indicator, JavaRowFormat format)
        {
            ArgumentNullException.ThrowIfNull(integers);
            ArgumentNullException.ThrowIfNull(format);

            var builder = typeFactory.builder();
            for (int i = 0; i < integers.size(); i++)
                builder.add((RelDataTypeField)rowType.getFieldList().get(JavaLists.Int(integers, i)));

            if (indicator)
            {
                var booleanType = typeFactory.createTypeWithNullability(typeFactory.createSqlType(SqlTypeName.BOOLEAN), false);
                for (int i = 0; i < integers.size(); i++)
                    builder.add("i$" + ((RelDataTypeField)rowType.getFieldList().get(JavaLists.Int(integers, i))).getName(), booleanType);
            }

            var projectedRowType = builder.build();

            return Of(typeFactory, projectedRowType, format.optimize(projectedRowType));
        }

        /// <inheritdoc />
        public LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields)
        {
            return GenerateSelector(parameter, fields, format);
        }

        /// <inheritdoc />
        public LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(fields);
            ArgumentNullException.ThrowIfNull(targetFormat);

            // optimize target format
            targetFormat = fields.size() switch
            {
                0 => JavaRowFormat.LIST,
                1 => JavaRowFormat.SCALAR,
                _ => targetFormat
            };

            var targetPhysType = Project(fields, targetFormat);

            // a row that is the value itself is already the selection of its one field, which is what
            // Functions.identitySelector answers with
            if (format.name() == nameof(JavaRowFormat.SCALAR))
                return Expression.Lambda(parameter, parameter);

            var body = targetPhysType.Record(FieldReferences(parameter, fields));

            return Expression.Lambda(body, parameter);
        }

        /// <inheritdoc />
        public LambdaExpression GenerateSelector(ParameterExpression parameter, java.util.List fields, java.util.List usedFields, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(fields);
            ArgumentNullException.ThrowIfNull(usedFields);
            ArgumentNullException.ThrowIfNull(targetFormat);

            var targetPhysType = Project(fields, true, targetFormat);
            var expressions = new List<Expression>(fields.size() * 2);

            for (int i = 0; i < fields.size(); i++)
            {
                var field = JavaLists.Int(fields, i);

                if (JavaLists.ContainsInt(usedFields, field))
                {
                    expressions.Add(FieldReference(parameter, field));
                    continue;
                }

                // a field this grouping set does not group by carries its type's default, and the indicator
                // below says so
                var fieldClass = targetPhysType.FieldClass(i);
                var primitive = fieldClass.IsValueType ? Activator.CreateInstance(fieldClass) : null;
                expressions.Add(Expression.Constant(primitive, primitive == null ? typeof(object) : fieldClass));
            }

            for (int i = 0; i < fields.size(); i++)
                expressions.Add(Expression.Constant(JavaLists.ContainsInt(usedFields, JavaLists.Int(fields, i)) == false));

            var body = targetPhysType.Record(expressions);

            return Expression.Lambda(body, parameter);
        }

        /// <inheritdoc />
        public (Type RowType, IReadOnlyList<Expression> Expressions) Selector(ParameterExpression parameter, java.util.List fields, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(fields);
            ArgumentNullException.ThrowIfNull(targetFormat);

            // optimize target format
            targetFormat = fields.size() switch
            {
                0 => JavaRowFormat.LIST,
                1 => JavaRowFormat.SCALAR,
                _ => targetFormat
            };

            var targetPhysType = Project(fields, targetFormat);

            if (format.name() == nameof(JavaRowFormat.SCALAR))
                return (parameter.Type, [parameter]);

            return (targetPhysType.RowType, FieldReferences(parameter, fields));
        }

        /// <inheritdoc />
        public IReadOnlyList<Expression> Accessors(Expression parameter, java.util.List argList)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(argList);

            var expressions = new List<Expression>(argList.size());
            for (int i = 0; i < argList.size(); i++)
            {
                var field = JavaLists.Int(argList, i);
                expressions.Add(ClrEnumUtils.Convert(FieldReference(parameter, field), FieldClass(field)));
            }

            return expressions;
        }

        /// <inheritdoc />
        public ClrPhysType MakeNullable(bool nullable)
        {
            if (nullable == false)
                return this;

            // the row class is boxed rather than asked for again: a nullable struct has different field
            // classes and so a different synthetic record, which the rows of the input are not
            return new ClrPhysTypeImpl(
                typeFactory,
                typeFactory.createTypeWithNullability(rowType, true),
                org.apache.calcite.linq4j.tree.Primitive.box(javaRowType),
                format);
        }

        /// <inheritdoc />
        [Obsolete("Use the JavaRowFormat overload; only the row format of the expression is affected.")]
        public Expression ConvertTo(Expression expression, ClrPhysType targetPhysType)
        {
            ArgumentNullException.ThrowIfNull(targetPhysType);

            return ConvertTo(expression, targetPhysType.Format);
        }

        /// <inheritdoc />
        public Expression ConvertTo(Expression expression, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(targetFormat);

            if (format == targetFormat)
                return expression;

            var (selector, targetRowType) = Reformatter(targetFormat);

            return Expression.Call(null,
                ClrBuiltInMethod.Select.MakeGenericMethod(javaRowClass, targetRowType),
                expression,
                selector);
        }

        /// <inheritdoc />
        public Expression ConvertToAsync(Expression expression, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(targetFormat);

            if (format == targetFormat)
                return expression;

            var (selector, targetRowType) = Reformatter(targetFormat);

            return AsyncEnumerable.ClrAsyncBuiltInMethod.Call(
                AsyncEnumerable.ClrAsyncBuiltInMethod.Select.MakeGenericMethod(javaRowClass, targetRowType),
                expression,
                selector);
        }

        /// <summary>
        /// Returns the per-row selector that rewrites a row of this format into another, and the type it
        /// yields.
        /// </summary>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// What the two <c>ConvertTo</c> overloads share, which is all of the work: reformatting a row is a
        /// row's business, and which operator carries the selector over the sequence is the convention's.
        /// </remarks>
        (LambdaExpression Selector, Type TargetRowType) Reformatter(JavaRowFormat targetFormat)
        {
            var o_ = Expression.Parameter(javaRowClass, "o");
            var fieldCount = rowType.getFieldCount();

            // strict, as Calcite is: the target format is not optimised here, and a caller that wants it
            // optimised does that before it asks
            var targetPhysType = Of(typeFactory, rowType, targetFormat, false);

            // what the sequence carries is the target's row type, not whatever the record happened to build:
            // a one column row of a value is the value, and a sequence of it still carries the box
            var targetRowType = targetPhysType.RowType;
            var body = ClrEnumUtils.Convert(targetPhysType.Record(FieldReferences(o_, Util.range(fieldCount))), targetRowType);

            return (Expression.Lambda(typeof(Func<,>).MakeGenericType(javaRowClass, targetRowType), body, o_), targetRowType);
        }

        /// <inheritdoc />
        public Expression Record(IReadOnlyList<Expression> expressions)
        {
            ArgumentNullException.ThrowIfNull(expressions);

            return format.Record(javaRowClass, expressions);
        }

        /// <inheritdoc />
        public Expression? Comparer()
        {
            return format.Comparer();
        }

        /// <inheritdoc />
        public ClrPhysType Component(int fieldOrdinal)
        {
            var field = (RelDataTypeField)rowType.getFieldList().get(fieldOrdinal);
            var componentType = field.getType().getComponentType()
                ?? throw new java.lang.NullPointerException($"field.getType().getComponentType() for {field}");

            return Of(typeFactory, ToStruct(componentType), format, false);
        }

        /// <inheritdoc />
        public ClrPhysType Field(int ordinal)
        {
            var field = (RelDataTypeField)rowType.getFieldList().get(ordinal);

            return Of(typeFactory, ToStruct(field.getType()), format, false);
        }

        /// <summary>
        /// Returns a row type of one field where the given type is not already a struct.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        RelDataType ToStruct(RelDataType type)
        {
            if (type.isStruct())
                return type;

            return typeFactory.builder().add(SqlUtil.deriveAliasFromOrdinal(0), type).build();
        }

        /// <inheritdoc />
        public Expression FieldReference(Expression expression, int field)
        {
            return FieldReference(expression, field, null);
        }

        /// <inheritdoc />
        public Expression FieldReference(Expression expression, int field, Type? storageType)
        {
            ArgumentNullException.ThrowIfNull(expression);

            Type? fieldType;
            if (storageType == null)
            {
                storageType = FieldClass(field);
                fieldType = null;
            }
            else
            {
                fieldType = FieldClass(field);

                // the three types a row stores as an int or a long rather than as themselves; every other
                // field is read as whatever the row holds
                if (fieldType != typeof(java.sql.Date) && fieldType != typeof(java.sql.Time) && fieldType != typeof(java.sql.Timestamp))
                    fieldType = null;
            }

            return format.Field(expression, field, fieldType, storageType, javaRowType);
        }

        /// <summary>
        /// Returns one expression per named field.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="fields"></param>
        /// <returns></returns>
        IReadOnlyList<Expression> FieldReferences(Expression parameter, java.util.List fields)
        {
            var expressions = new Expression[fields.size()];
            for (int i = 0; i < expressions.Length; i++)
                expressions[i] = FieldReference(parameter, JavaLists.Int(fields, i));

            return expressions;
        }

        /// <inheritdoc />
        public LambdaExpression GenerateAccessor(java.util.List fields)
        {
            ArgumentNullException.ThrowIfNull(fields);

            var v1 = Expression.Parameter(javaRowClass, "v1");

            switch (fields.size())
            {
                case 0:
                    return Expression.Lambda(JavaRowFormatExtensions.ComparableEmptyList, v1);

                case 1:
                    var field0 = JavaLists.Int(fields, 0);
                    return Expression.Lambda(ClrEnumUtils.Convert(FieldReference(v1, field0), FieldClass(field0)), v1);

                default:
                    return Expression.Lambda(GetListExpression(FieldReferences(v1, fields)), v1);
            }
        }

        /// <inheritdoc />
        public LambdaExpression GenerateAccessorWithoutNulls(java.util.List fields)
        {
            ArgumentNullException.ThrowIfNull(fields);

            if (fields.size() < 2)
                return GenerateAccessor(fields);

            var v1 = Expression.Parameter(javaRowClass, "v1");
            var list = FieldReferences(v1, fields);

            // (v1.field0 == null) ? null : (v1.field1 == null) ? null : ... : FlatLists.of(...)
            var body = GetListExpression(list);
            for (int i = list.Count - 1; i >= 0; i--)
                body = ClrEnumUtils.NullIfNull(list[i], body);

            return Expression.Lambda(body, v1);
        }

        /// <inheritdoc />
        public LambdaExpression GenerateNullAwareAccessor(java.util.List fields, java.util.List nullExclusionFlags)
        {
            ArgumentNullException.ThrowIfNull(fields);
            ArgumentNullException.ThrowIfNull(nullExclusionFlags);

            if (fields.size() != nullExclusionFlags.size())
                throw new java.lang.AssertionError("fields.size() != nullExclusionFlags.size()");

            var v1 = Expression.Parameter(javaRowClass, "v1");
            if (fields.isEmpty())
                return Expression.Lambda(JavaRowFormatExtensions.ComparableEmptyList, v1);

            var list = FieldReferences(v1, fields);

            // a hash join with exactly one join key that is null-safe must still recognise a row whose key is
            // null, so a list of one is a list rather than the element itself
            var body = GetListExpressionAllowSingleElement(list);
            for (int i = list.Count - 1; i >= 0; i--)
                if (JavaLists.Bool(nullExclusionFlags, i))
                    body = ClrEnumUtils.NullIfNull(list[i], body);

            return Expression.Lambda(body, v1);
        }

        /// <summary>
        /// Returns the expression building a comparable list of two or more expressions.
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.getListExpression</c>, which is private.
        /// </remarks>
        static Expression GetListExpression(IReadOnlyList<Expression> list)
        {
            if (list.Count < 2)
                throw new java.lang.AssertionError("list.size() >= 2");

            return JavaRowFormat.LIST.Record(typeof(java.util.List), list);
        }

        /// <summary>
        /// Returns the expression building a comparable list of one or more expressions.
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.getListExpressionAllowSingleElement</c>, which is private.
        /// </remarks>
        static Expression GetListExpressionAllowSingleElement(IReadOnlyList<Expression> list)
        {
            if (list.Count == 0)
                throw new java.lang.AssertionError("list.size() > 0");

            if (list.Count == 1)
                return Expression.Call(null, JavaRowFormatExtensions.FlatListOf1, ClrEnumUtils.Convert(list[0], typeof(object)));

            return GetListExpression(list);
        }

        /// <inheritdoc />
        public (LambdaExpression Selector, Expression? Comparator) GenerateCollationKey(java.util.List collations)
        {
            ArgumentNullException.ThrowIfNull(collations);

            if (collations.size() == 1)
            {
                var collation = (RelFieldCollation)collations.get(0);
                var fieldType = rowType.getFieldList() == null || rowType.getFieldList().isEmpty()
                    ? rowType
                    : ((RelDataTypeField)rowType.getFieldList().get(collation.getFieldIndex())).getType();
                var fieldComparator = ClrEnumUtils.GenerateCollatorExpression(fieldType.getCollation());

                var parameter = Expression.Parameter(javaRowClass, "v");
                var selector = Expression.Lambda(FieldReference(parameter, collation.getFieldIndex()), parameter);

                var nullsFirst = Expression.Constant(collation.nullDirection == RelFieldCollation.NullDirection.FIRST);
                var descending = Expression.Constant(collation.getDirection() == RelFieldCollation.Direction.DESCENDING);

                var comparator = fieldComparator == null
                    ? Expression.Call(null, NullsComparator, nullsFirst, descending)
                    : Expression.Call(null, NullsComparator2, nullsFirst, descending, fieldComparator);

                return (selector, comparator);
            }

            // the key is the row itself, and the comparator walks the collations in order
            var identity = Expression.Parameter(javaRowClass, "v");

            return (
                Expression.Lambda(identity, identity),
                GenerateComparator(collations, FieldCollationCompareName, javaRowClass));
        }

        /// <inheritdoc />
        public Expression GenerateComparator(RelCollation collation)
        {
            ArgumentNullException.ThrowIfNull(collation);

            return GenerateComparator(collation.getFieldCollations(), FieldCollationCompareName, ClrPrimitive.Box(javaRowClass));
        }

        /// <inheritdoc />
        public Expression GenerateMergeJoinComparator(RelCollation collation)
        {
            ArgumentNullException.ThrowIfNull(collation);

            return GenerateComparator(collation.getFieldCollations(), MergeJoinCompareName, ClrPrimitive.Box(javaRowClass));
        }

        /// <summary>
        /// Returns the name of the <c>Utilities</c> method ordering one field of a row.
        /// </summary>
        /// <param name="fieldCollation"></param>
        /// <returns></returns>
        string FieldCollationCompareName(RelFieldCollation fieldCollation)
        {
            var index = fieldCollation.getFieldIndex();
            var nullsFirst = fieldCollation.nullDirection == RelFieldCollation.NullDirection.FIRST;
            var descending = fieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING;

            return FieldNullable(index)
                ? nullsFirst != descending ? "compareNullsFirst" : "compareNullsLast"
                : "compare";
        }

        /// <summary>
        /// Returns the name of the <c>Utilities</c> method a merge join orders one field of a row by.
        /// </summary>
        /// <param name="fieldCollation"></param>
        /// <returns></returns>
        /// <remarks>
        /// A merge join's keys are always ascending with nulls last, and two nulls are not equal.
        /// </remarks>
        string MergeJoinCompareName(RelFieldCollation fieldCollation)
        {
            if (fieldCollation.nullDirection != RelFieldCollation.NullDirection.LAST)
                throw new java.lang.AssertionError("nullDirection == LAST");
            if (fieldCollation.getDirection() != RelFieldCollation.Direction.ASCENDING)
                throw new java.lang.AssertionError("direction == ASCENDING");

            return FieldNullable(fieldCollation.getFieldIndex()) ? "compareNullsLastForMergeJoin" : "compare";
        }

        /// <summary>
        /// Returns the comparator ordering two rows by the given collations.
        /// </summary>
        /// <param name="fieldCollations"></param>
        /// <param name="compareMethodName"></param>
        /// <param name="parameterType">The type of a row as the comparator receives it.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.generateComparator</c>, whose body is
        /// <c>int c; c = Utilities.compare(v0, v1); if (c != 0) return c; … return 0;</c>.
        ///
        /// <para>Calcite declares an anonymous <c>Comparator</c> around that method, and adds a bridge method
        /// taking two objects where the row type is not itself <c>Object</c>, because a Java class needs one.
        /// The method is a lambda here and <see cref="DelegateComparator{T}"/> is the bridge, so neither has a
        /// counterpart.</para>
        /// </remarks>
        Expression GenerateComparator(java.util.List fieldCollations, Func<RelFieldCollation, string> compareMethodName, Type parameterType)
        {
            var v0 = Expression.Parameter(parameterType, "v0");
            var v1 = Expression.Parameter(parameterType, "v1");
            var c = Expression.Variable(typeof(int), "c");
            var end = Expression.Label(typeof(int), "end");

            var body = new List<Expression>();

            for (int i = 0; i < fieldCollations.size(); i++)
            {
                var fieldCollation = (RelFieldCollation)fieldCollations.get(i);
                var index = fieldCollation.getFieldIndex();

                // a NULL literal is always null, and null compared to null is 0
                if (FieldClass(index) == typeof(java.lang.Void))
                    continue;

                var fieldType = ((RelDataTypeField)rowType.getFieldList().get(index)).getType();
                var fieldComparator = ClrEnumUtils.GenerateCollatorExpression(fieldType.getCollation());

                var arg0 = FieldReference(ClrEnumUtils.Convert(v0, javaRowClass), index);
                var arg1 = FieldReference(ClrEnumUtils.Convert(v1, javaRowClass), index);

                // a field that is neither a primitive nor one of their boxes is ordered as a Comparable.
                // Calcite casts to it to pick that overload out of the source text, and the cast never runs;
                // here it would, and java.lang.Comparable is a ghost interface IKVM gives to a string without
                // the CLR type system knowing. So the overload is chosen by a method that takes an object and
                // does the conversion in compiled C#, where IKVM can emit the check.
                var asObject = ClrPrimitive.IsObject(arg0.Type);
                if (asObject)
                {
                    arg0 = ClrEnumUtils.Convert(arg0, typeof(object));
                    arg1 = ClrEnumUtils.Convert(arg1, typeof(object));
                }

                var descending = fieldCollation.getDirection() == RelFieldCollation.Direction.DESCENDING;
                var name = compareMethodName(fieldCollation);

                var arguments = fieldComparator == null
                    ? new[] { arg0, arg1 }
                    : [arg0, arg1, fieldComparator];

                var argumentTypes = new Type[arguments.Length];
                for (int j = 0; j < argumentTypes.Length; j++)
                    argumentTypes[j] = arguments[j].Type;

                var comparisons = asObject ? typeof(JavaComparisons) : typeof(org.apache.calcite.runtime.Utilities);
                var method = ClrTypes.Resolve(comparisons, asObject ? char.ToUpperInvariant(name[0]) + name[1..] : name, argumentTypes);

                body.Add(Expression.Assign(c, Expression.Call(null, method, arguments)));
                body.Add(
                    Expression.IfThen(
                        Expression.NotEqual(c, Expression.Constant(0)),
                        Expression.Return(end, descending ? Expression.Negate(c) : c)));
            }

            body.Add(Expression.Label(end, Expression.Constant(0)));

            var comparison = Expression.Lambda(
                typeof(Func<,,>).MakeGenericType(parameterType, parameterType, typeof(int)),
                Expression.Block(typeof(int), [c], body),
                v0,
                v1);

            return Expression.New(
                typeof(DelegateComparator<>).MakeGenericType(parameterType).GetConstructors()[0],
                comparison);
        }

        /// <summary>
        /// The members Calcite names through <c>BuiltInMethod</c>, resolved once against what IKVM compiled.
        /// </summary>
        static readonly MethodInfo NullsComparator = ClrTypes.Resolve(BuiltInMethod.NULLS_COMPARATOR.method);

        /// <inheritdoc cref="NullsComparator" />
        static readonly MethodInfo NullsComparator2 = ClrTypes.Resolve(BuiltInMethod.NULLS_COMPARATOR2.method);

    }

}
