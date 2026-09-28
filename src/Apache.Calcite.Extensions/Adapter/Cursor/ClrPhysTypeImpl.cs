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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="ClrPhysType"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>PhysTypeImpl</c>, including its private helpers. How a row is laid out is answered by the
    /// <see cref="JavaRowFormat"/>, through <see cref="JavaRowFormatExtensions"/>.
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
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <param name="javaRowType">The Java row type; its CLR type, boxed, becomes <see cref="RowType"/>.</param>
        /// <param name="format">The row format, used as given.</param>
        /// <remarks>
        /// Mirrors <c>PhysTypeImpl</c>'s constructor, which takes the row class rather than deriving it so that
        /// <c>makeNullable</c> can pass a boxed one.
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
                // a field type with no class of its own, such as a struct, is held as an Object[]
                var fieldType = typeFactory.getJavaClass(((RelDataTypeField)fields.get(i)).getType());
                fieldClasses[i] = ClrTypes.Resolve(fieldType is java.lang.Class ? fieldType : (java.lang.Class)typeof(object[]));
            }
        }

        /// <summary>
        /// Returns the physical type of a row, optimizing the format for the row type.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <param name="format">The preferred row format.</param>
        /// <returns>The physical type.</returns>
        /// <remarks>
        /// Mirrors <c>PhysTypeImpl.of(JavaTypeFactory, RelDataType, JavaRowFormat)</c>.
        /// </remarks>
        public static ClrPhysType Of(JavaTypeFactory typeFactory, RelDataType rowType, JavaRowFormat format)
        {
            return Of(typeFactory, rowType, format, true);
        }

        /// <summary>
        /// Returns the physical type of a row.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <param name="format">The row format.</param>
        /// <param name="optimize">Whether to optimize the format for the row type with
        /// <c>JavaRowFormat.optimize</c>.</param>
        /// <returns>The physical type.</returns>
        /// <remarks>
        /// Mirrors <c>PhysTypeImpl.of(JavaTypeFactory, RelDataType, JavaRowFormat, boolean)</c>.
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
        /// Returns the physical type of a row described by a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="javaRowClass">The row class, such as a synthetic record over accumulator state types.</param>
        /// <returns>A physical type in <see cref="JavaRowFormat.CUSTOM"/> format whose row class is
        /// <paramref name="javaRowClass"/>.</returns>
        /// <remarks>
        /// Mirrors the package private <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>. The row type is built from
        /// the record's fields, and the format is not optimized, so a record of one field stays a record.
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

            // the given row class is kept rather than derived from the row type, because the accumulator's
            // state slots were declared against this record
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

            // a SCALAR row is its one field, so the selector is the identity, as Calcite's
            // Functions.identitySelector
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

                // a field this grouping set does not group by carries its type's default; its indicator is set
                // below
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

            // as in Calcite, the existing row class is boxed rather than derived again from the nullable row
            // type, which could name a different class than the rows actually have
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
                Cursor.ClrCursorBuiltInMethod.Select.MakeGenericMethod(javaRowClass, targetRowType),
                expression,
                selector);
        }

        /// <inheritdoc />
        public Expression ConvertToAsync(Cursor.ClrCursorRelImplementor implementor, Expression expression, JavaRowFormat targetFormat)
        {
            ArgumentNullException.ThrowIfNull(implementor);
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(targetFormat);

            if (format == targetFormat)
                return expression;

            var (selector, targetRowType) = Reformatter(targetFormat);

            return Cursor.ClrCursorBuiltInMethod.CallAsync(implementor,
                Cursor.ClrCursorBuiltInMethod.SelectAsync.MakeGenericMethod(javaRowClass, targetRowType),
                expression,
                selector);
        }

        /// <summary>
        /// Returns the selector that rewrites a row of this type into the target format, and the row type it
        /// yields; shared by both <c>ConvertTo</c> overloads and <see cref="ConvertToAsync"/>.
        /// </summary>
        /// <param name="targetFormat">The row format to convert to.</param>
        /// <returns>The selector, which takes a row of this type and returns the target row, and the target's boxed row type.</returns>
        (LambdaExpression Selector, Type TargetRowType) Reformatter(JavaRowFormat targetFormat)
        {
            var o_ = Expression.Parameter(javaRowClass, "o");
            var fieldCount = rowType.getFieldCount();

            // as in Calcite, the target format is not optimized
            var targetPhysType = Of(typeFactory, rowType, targetFormat, false);

            // the selector returns the target's boxed row type, whatever type the record expression has
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
            var comparer = format.Comparer();
            if (comparer != null)
                return comparer;

            if (AnyFieldContainsStruct(rowType))
                // a struct is an Object[] or a List at run time, which compare a nested Object[] by
                // reference, so a row holding one needs deep equality for GROUP BY, DISTINCT and set operators
                return Expression.Call(null, DeepComparer);

            return null;
        }

        /// <summary>
        /// Returns whether any field of the row type holds a struct, itself or within a collection or a map.
        /// </summary>
        /// <remarks>
        /// <c>PhysTypeImpl.anyFieldContainsStruct</c>, which is private.
        /// </remarks>
        static bool AnyFieldContainsStruct(RelDataType rowType)
        {
            for (var i = rowType.getFieldList().iterator(); i.hasNext();)
                if (ContainsStruct(((RelDataTypeField)i.next()).getType()))
                    return true;

            return false;
        }

        /// <summary>
        /// Returns whether a type holds a struct, itself or within a collection or a map.
        /// </summary>
        /// <remarks>
        /// <c>PhysTypeImpl.containsStruct</c>, which is private.
        /// </remarks>
        static bool ContainsStruct(RelDataType type)
        {
            if (type.isStruct())
                return true;

            var componentType = type.getComponentType();
            if (componentType != null && ContainsStruct(componentType))
                return true;

            var keyType = type.getKeyType();
            if (keyType != null && ContainsStruct(keyType))
                return true;

            var valueType = type.getValueType();

            return valueType != null && ContainsStruct(valueType);
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
        /// Returns the type itself if it is a struct, and otherwise a struct of one field of that type.
        /// Mirrors <c>PhysTypeImpl.toStruct</c>.
        /// </summary>
        /// <param name="type">A relational type.</param>
        /// <returns><paramref name="type"/> if it is a struct, otherwise a struct with one field named by <c>SqlUtil.deriveAliasFromOrdinal(0)</c>.</returns>
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

                // only a date, time or timestamp, which a row stores as an int or a long, needs its field
                // class passed to the format; any other field is read as the row holds it
                if (fieldType != typeof(java.sql.Date) && fieldType != typeof(java.sql.Time) && fieldType != typeof(java.sql.Timestamp))
                    fieldType = null;
            }

            return format.Field(expression, field, fieldType, storageType, javaRowType);
        }

        /// <summary>
        /// Returns one field read per listed field ordinal. Mirrors <c>PhysTypeImpl.fieldReferences</c>.
        /// </summary>
        /// <param name="parameter">The row expression to read from.</param>
        /// <param name="fields">The field ordinals, a list of <c>java.lang.Integer</c>.</param>
        /// <returns>One field read per ordinal, in order.</returns>
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

            // even a single key is wrapped in a list, so that a null-safe key whose value is null still yields
            // a non-null key a hash join can match
            var body = GetListExpressionAllowSingleElement(list);
            for (int i = list.Count - 1; i >= 0; i--)
            {
                if (JavaLists.Bool(nullExclusionFlags, i) == false)
                    continue;

                var fieldType = ((RelDataTypeField)rowType.getFieldList().get(JavaLists.Int(fields, i))).getType();

                // under = a NULL never compares TRUE, and neither does a ROW holding a NULL at any depth, so
                // either makes the key null
                body = fieldType.isStruct()
                    ? Expression.Condition(
                        StructIsNullOrContainsNull(list[i], fieldType),
                        Expression.Constant(null, body.Type),
                        body,
                        body.Type)
                    : ClrEnumUtils.NullIfNull(list[i], body);
            }

            return Expression.Lambda(body, v1);
        }

        /// <summary>
        /// Returns an expression testing whether a value of a ROW type is null or holds a null field.
        /// </summary>
        /// <remarks>
        /// Mirrors the private <c>PhysTypeImpl.structIsNullOrContainsNullExpression</c>. It descends into
        /// struct fields but not into collections.
        /// </remarks>
        static Expression StructIsNullOrContainsNull(Expression e, RelDataType type)
        {
            Expression result = Expression.Equal(e, Expression.Constant(null, e.Type));

            for (int i = 0; i < type.getFieldList().size(); i++)
            {
                var field = (RelDataTypeField)type.getFieldList().get(i);
                var fieldType = field.getType();
                if (fieldType.isStruct() == false && fieldType.isNullable() == false)
                    continue;

                // structAccess accepts either run-time representation of a struct, and the short-circuit OR
                // runs it only where e is not null
                var access = Expression.Call(null, StructAccess, ClrEnumUtils.Convert(e, typeof(object)), Expression.Constant(i), Expression.Constant(field.getName()));

                result = Expression.OrElse(result, fieldType.isStruct()
                    ? StructIsNullOrContainsNull(access, fieldType)
                    : Expression.Equal(access, Expression.Constant(null, access.Type)));
            }

            return result;
        }

        /// <summary>
        /// Returns an expression building a comparable list of two or more expressions.
        /// </summary>
        /// <remarks>
        /// Mirrors the private <c>PhysTypeImpl.getListExpression</c>.
        /// </remarks>
        /// <param name="list">The element expressions; there must be at least two.</param>
        /// <returns>An expression of type <c>java.util.List</c> building the list in the <c>LIST</c> row format.</returns>
        static Expression GetListExpression(IReadOnlyList<Expression> list)
        {
            if (list.Count < 2)
                throw new java.lang.AssertionError("list.size() >= 2");

            return JavaRowFormat.LIST.Record(typeof(java.util.List), list);
        }

        /// <summary>
        /// Returns an expression building a comparable list of one or more expressions.
        /// </summary>
        /// <remarks>
        /// Mirrors the private <c>PhysTypeImpl.getListExpressionAllowSingleElement</c>.
        /// </remarks>
        /// <param name="list">The element expressions; there must be at least one.</param>
        /// <returns>An expression of type <c>java.util.List</c>: a one-element flat list, or as <see cref="GetListExpression"/> gives it.</returns>
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

            // otherwise the key is the row itself, compared field by field in collation order
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
        /// Returns the name of the <c>Utilities</c> method that compares one field under a collation.
        /// </summary>
        /// <param name="fieldCollation">The collation of the field.</param>
        /// <returns><c>compare</c> for a field that is not nullable; otherwise <c>compareNullsFirst</c> or <c>compareNullsLast</c>,
        /// chosen so that nulls come where the collation puts them once a descending result is negated.</returns>
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
        /// Returns the name of the <c>Utilities</c> method that compares one merge join key field.
        /// </summary>
        /// <remarks>
        /// A merge join's keys are ascending with nulls last, and two nulls do not compare equal.
        /// </remarks>
        /// <param name="fieldCollation">The collation of the key field; must be ascending with nulls last.</param>
        /// <returns><c>compareNullsLastForMergeJoin</c> for a nullable field, otherwise <c>compare</c>.</returns>
        string MergeJoinCompareName(RelFieldCollation fieldCollation)
        {
            if (fieldCollation.nullDirection != RelFieldCollation.NullDirection.LAST)
                throw new java.lang.AssertionError("nullDirection == LAST");
            if (fieldCollation.getDirection() != RelFieldCollation.Direction.ASCENDING)
                throw new java.lang.AssertionError("direction == ASCENDING");

            return FieldNullable(fieldCollation.getFieldIndex()) ? "compareNullsLastForMergeJoin" : "compare";
        }

        /// <summary>
        /// Returns an expression creating a comparator that orders two rows by the given field collations.
        /// </summary>
        /// <param name="fieldCollations">The field collations, a list of <see cref="RelFieldCollation"/>.</param>
        /// <param name="compareMethodName">Chooses the <c>Utilities</c> comparison method for a field.</param>
        /// <param name="parameterType">The type of a row as the comparator receives it.</param>
        /// <remarks>
        /// Mirrors <c>PhysTypeImpl.generateComparator</c>, whose body compares each field in turn and returns
        /// the first non-zero result, negated for a descending field. Calcite wraps that body in an anonymous
        /// <c>Comparator</c> with a bridge method; here it is a lambda wrapped in a
        /// <see cref="DelegateComparator{T}"/>.
        /// </remarks>
        /// <returns>An expression evaluating to a <see cref="DelegateComparator{T}"/> over <paramref name="parameterType"/>.</returns>
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

                // a field of type NULL is always null, and null compared to null is 0
                if (FieldClass(index) == typeof(java.lang.Void))
                    continue;

                var fieldType = ((RelDataTypeField)rowType.getFieldList().get(index)).getType();
                var fieldComparator = ClrEnumUtils.GenerateCollatorExpression(fieldType.getCollation());

                var arg0 = FieldReference(ClrEnumUtils.Convert(v0, javaRowClass), index);
                var arg1 = FieldReference(ClrEnumUtils.Convert(v1, javaRowClass), index);

                // Calcite casts a non-primitive field to Comparable to select a Utilities overload. IKVM makes
                // java.lang.Comparable a ghost interface of System.String that the CLR cannot cast to, so such
                // a field is passed as object to JavaComparisons, which converts it in compiled C#
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
        /// <c>BuiltInMethod</c> members, resolved to the CLR methods IKVM compiled them to.
        /// </summary>
        static readonly MethodInfo NullsComparator = ClrTypes.Resolve(BuiltInMethod.NULLS_COMPARATOR.method);

        /// <inheritdoc cref="NullsComparator" />
        static readonly MethodInfo NullsComparator2 = ClrTypes.Resolve(BuiltInMethod.NULLS_COMPARATOR2.method);

        /// <inheritdoc cref="NullsComparator" />
        static readonly MethodInfo DeepComparer = ClrTypes.Resolve(BuiltInMethod.DEEP_COMPARER.method);

        /// <inheritdoc cref="NullsComparator" />
        static readonly MethodInfo StructAccess = ClrTypes.Resolve(BuiltInMethod.STRUCT_ACCESS.method);

    }

}
