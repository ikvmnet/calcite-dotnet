using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The members of <see cref="JavaRowFormat"/> that <see cref="ClrPhysTypeImpl"/> needs, answered with CLR
    /// types and <see cref="System.Linq.Expressions"/> expressions.
    /// </summary>
    /// <remarks>
    /// <c>record</c> and <c>field</c> return linq4j, and <c>javaRowClass</c> and <c>javaFieldClass</c> are
    /// package private, so those four are ported here along with <c>comparer</c>. <c>optimize</c> is public and
    /// is called directly.
    ///
    /// <para>Calcite implements these in the bodies of its enum constants; here each format is a subclass of
    /// <see cref="Constant"/>, chosen by <see cref="Of"/> on the constant's name, because a Java enum's
    /// ordinals are not stable across versions.</para>
    /// </remarks>
    static class JavaRowFormatExtensions
    {

        /// <summary>
        /// Returns the Java type of a row of the given format, as the type factory names it.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>JavaRowFormat.javaRowClass</c>. For a <see cref="JavaRowFormat.CUSTOM"/> row this is
        /// the type factory's synthetic record type, through which fields are found by
        /// <c>Types.nthField</c>.
        /// </remarks>
        /// <param name="format">The row format.</param>
        /// <param name="typeFactory">The type factory that names the row type.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <returns>The Java type of a row, which may be a synthetic type the type factory made rather than a <c>java.lang.Class</c>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="typeFactory"/> or <paramref name="rowType"/> is <see langword="null"/>.</exception>
        public static java.lang.reflect.Type JavaRowType(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            return Of(format).JavaRowType(typeFactory, rowType);
        }

        /// <summary>
        /// Returns the CLR type of a row of the given format: <see cref="JavaRowType"/>, resolved.
        /// </summary>
        /// <param name="format">The row format.</param>
        /// <param name="typeFactory">The type factory that names the row type.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <returns>The CLR type of a row.</returns>
        public static Type JavaRowClass(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            return ClrTypes.Resolve(format.JavaRowType(typeFactory, rowType));
        }

        /// <summary>
        /// Returns the CLR type in which a row of the given format stores a field.
        /// </summary>
        /// <remarks>
        /// Mirrors the package private <c>JavaRowFormat.javaFieldClass</c>. A <see cref="JavaRowFormat.LIST"/>,
        /// <see cref="JavaRowFormat.ROW"/> or <see cref="JavaRowFormat.ARRAY"/> row stores every field as
        /// <see cref="object"/>, nullable or not.
        /// </remarks>
        /// <param name="format">The row format.</param>
        /// <param name="typeFactory">The type factory that maps the field's type.</param>
        /// <param name="rowType">The relational row type.</param>
        /// <param name="index">The field ordinal.</param>
        /// <returns>The CLR type of the stored field value.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="typeFactory"/> or <paramref name="rowType"/> is <see langword="null"/>.</exception>
        public static Type JavaFieldClass(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType, int index)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            return Of(format).JavaFieldClass(typeFactory, rowType, index);
        }

        /// <summary>
        /// Returns an expression building a row of the given format from one expression per field.
        /// </summary>
        /// <param name="format">The row format.</param>
        /// <param name="rowClass">The CLR row type, which a <see cref="JavaRowFormat.CUSTOM"/> row constructs.</param>
        /// <param name="expressions">The field values, in field order.</param>
        /// <remarks>
        /// Mirrors <c>JavaRowFormat.record</c>.
        /// </remarks>
        /// <returns>An expression whose value is the new row.</returns>
        public static Expression Record(this JavaRowFormat format, Type rowClass, IReadOnlyList<Expression> expressions)
        {
            ArgumentNullException.ThrowIfNull(rowClass);
            ArgumentNullException.ThrowIfNull(expressions);

            return Of(format).Record(rowClass, expressions);
        }

        /// <summary>
        /// Returns an expression reading one field of a row of the given format.
        /// </summary>
        /// <param name="format">The row format.</param>
        /// <param name="expression">An expression whose value is the row.</param>
        /// <param name="field">The field ordinal.</param>
        /// <param name="fromType">The type to read the stored value as before converting it, or
        /// <see langword="null"/>; see <see cref="ClrEnumUtils.Convert(Expression, Type?, Type)"/>.</param>
        /// <param name="fieldType">The type wanted.</param>
        /// <param name="javaRowType">The Java row type, through which a <see cref="JavaRowFormat.CUSTOM"/> row's
        /// fields are found.</param>
        /// <remarks>
        /// Mirrors <c>JavaRowFormat.field</c>. A <see cref="JavaRowFormat.CUSTOM"/> or
        /// <see cref="JavaRowFormat.SCALAR"/> field is returned as stored, without conversion.
        /// </remarks>
        /// <returns>An expression whose value is the field, of <paramref name="fieldType"/> except where the format returns it as stored.</returns>
        public static Expression Field(this JavaRowFormat format, Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(fieldType);
            ArgumentNullException.ThrowIfNull(javaRowType);

            return Of(format).Field(expression, field, fromType, fieldType, javaRowType);
        }

        /// <summary>
        /// Returns an expression creating the equality comparer for rows of the given format, or
        /// <see langword="null"/> if rows compare correctly by their own equality.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>JavaRowFormat.comparer</c>. Only <see cref="JavaRowFormat.ARRAY"/> has one.
        /// </remarks>
        /// <param name="format">The row format.</param>
        /// <returns>An expression whose value is the comparer, or <see langword="null"/>.</returns>
        public static Expression? Comparer(this JavaRowFormat format)
        {
            return Of(format).Comparer();
        }

        /// <summary>
        /// Returns the implementation for a <see cref="JavaRowFormat"/> constant, matched by name.
        /// </summary>
        /// <param name="format">The row format.</param>
        /// <returns>The singleton implementing <paramref name="format"/>.</returns>
        static Constant Of(JavaRowFormat format)
        {
            ArgumentNullException.ThrowIfNull(format);

            return format.name() switch
            {
                nameof(JavaRowFormat.CUSTOM) => Custom,
                nameof(JavaRowFormat.SCALAR) => Scalar,
                nameof(JavaRowFormat.LIST) => List,
                nameof(JavaRowFormat.ROW) => Row,
                nameof(JavaRowFormat.ARRAY) => Array,
                _ => throw new NotSupportedException($"There is no row format '{format.name()}'.")
            };
        }

        static readonly Constant Custom = new CustomConstant();
        static readonly Constant Scalar = new ScalarConstant();
        static readonly Constant List = new ListConstant();
        static readonly Constant Row = new RowConstant();
        static readonly Constant Array = new ArrayConstant();

        /// <summary>
        /// The behavior of one <see cref="JavaRowFormat"/> constant.
        /// </summary>
        abstract class Constant
        {

            /// <inheritdoc cref="JavaRowFormatExtensions.JavaRowType" />
            public abstract java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType);

            /// <inheritdoc cref="JavaRowFormatExtensions.JavaFieldClass" />
            public abstract Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index);

            /// <inheritdoc cref="JavaRowFormatExtensions.Record" />
            public abstract Expression Record(Type rowClass, IReadOnlyList<Expression> expressions);

            /// <inheritdoc cref="JavaRowFormatExtensions.Field" />
            public abstract Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType);

            /// <inheritdoc cref="JavaRowFormatExtensions.Comparer" />
            public virtual Expression? Comparer() => null;

        }

        /// <summary>
        /// <see cref="JavaRowFormat.CUSTOM"/>: a row that is an instance of a class with a field per column.
        /// </summary>
        sealed class CustomConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType) => typeFactory.getJavaClass(rowType);

            /// <inheritdoc />
            public override Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index)
            {
                return ClrTypes.Resolve(typeFactory.getJavaClass(((RelDataTypeField)rowType.getFieldList().get(index)).getType()));
            }

            /// <inheritdoc />
            /// <remarks>
            /// Calls the row class's public constructor whose arity is the number of fields, converting each
            /// value to the parameter's type; a row of no fields is <c>Unit.INSTANCE</c>.
            /// </remarks>
            public override Expression Record(Type rowClass, IReadOnlyList<Expression> expressions)
            {
                if (expressions.Count == 0)
                    return UnitInstance;

                var constructor = Constructor(rowClass, expressions.Count);
                var parameters = constructor.GetParameters();
                var arguments = new Expression[expressions.Count];
                for (int i = 0; i < arguments.Length; i++)
                    arguments[i] = ClrEnumUtils.Convert(expressions[i], parameters[i].ParameterType);

                return Expression.New(constructor, arguments);
            }

            /// <inheritdoc />
            public override Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
            {
                return ClrTypes.Resolve(expression, org.apache.calcite.linq4j.tree.Types.nthField(field, javaRowType));
            }

            /// <summary>
            /// Returns the row class's public constructor of the given arity.
            /// </summary>
            /// <exception cref="NotSupportedException">There is no such constructor.</exception>
            static ConstructorInfo Constructor(Type rowClass, int arity)
            {
                foreach (var constructor in rowClass.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
                    if (constructor.GetParameters().Length == arity)
                        return constructor;

                throw new NotSupportedException($"'{rowClass}' has no constructor of {arity} parameters.");
            }

        }

        /// <summary>
        /// <see cref="JavaRowFormat.SCALAR"/>: a row of one column that is the column's value.
        /// </summary>
        sealed class ScalarConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType)
            {
                var field0Type = ((RelDataTypeField)rowType.getFieldList().get(0)).getType();

                // a ROW value is held as an Object[], whatever class the type factory would give it
                return field0Type.getSqlTypeName() == SqlTypeName.ROW
                    ? (java.lang.Class)typeof(object[])
                    : typeFactory.getJavaClass(field0Type);
            }

            /// <inheritdoc />
            public override Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index)
            {
                return ClrTypes.Resolve(JavaRowType(typeFactory, rowType));
            }

            /// <inheritdoc />
            public override Expression Record(Type rowClass, IReadOnlyList<Expression> expressions)
            {
                if (expressions.Count != 1)
                    throw new NotSupportedException($"A scalar row is one expression, not {expressions.Count}.");

                return expressions[0];
            }

            /// <inheritdoc />
            public override Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
            {
                if (field != 0)
                    throw new NotSupportedException($"A scalar row has one field, so there is no field {field}.");

                return expression;
            }

        }

        /// <summary>
        /// <see cref="JavaRowFormat.LIST"/>: a row that is a comparable, immutable <c>FlatLists</c> list.
        /// </summary>
        sealed class ListConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType) => (java.lang.Class)typeof(org.apache.calcite.runtime.FlatLists.ComparableList);

            /// <inheritdoc />
            public override Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index) => typeof(object);

            /// <inheritdoc />
            public override Expression Record(Type rowClass, IReadOnlyList<Expression> expressions)
            {
                if (expressions.Count == 0)
                    return ComparableEmptyList;

                return ClrEnumUtils.Convert(FlatList(expressions), typeof(java.util.List));
            }

            /// <inheritdoc />
            public override Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
            {
                return ClrEnumUtils.Convert(
                    Expression.Call(ClrEnumUtils.Convert(expression, typeof(java.util.List)), ListGet, Expression.Constant(field)),
                    fromType, fieldType);
            }

            /// <summary>
            /// Returns a call building a comparable list of the given expressions.
            /// </summary>
            /// <remarks>
            /// Uses the <c>FlatLists.of</c> overload for two to six elements, and <c>FlatLists.copyOf</c> over
            /// an array otherwise. A list of one is not handled here, because a one-field row is
            /// <see cref="JavaRowFormat.SCALAR"/>; <see cref="ClrPhysTypeImpl.GenerateNullAwareAccessor"/> uses
            /// <see cref="FlatListOf1"/> for that case.
            /// </remarks>
            /// <param name="expressions">The element expressions; there must be at least two.</param>
            /// <returns>An expression whose value is a <c>FlatLists</c> list of the elements, in order.</returns>
            static Expression FlatList(IReadOnlyList<Expression> expressions)
            {
                if (expressions.Count is >= 2 and <= 6)
                {
                    var arguments = new Expression[expressions.Count];
                    for (int i = 0; i < arguments.Length; i++)
                        arguments[i] = ClrEnumUtils.Convert(expressions[i], typeof(object));

                    return Expression.Call(null, FlatListOf[expressions.Count - 2], arguments);
                }

                // Calcite builds a Comparable[]; IKVM compiles java.lang.Comparable in signatures as
                // IComparable, which a string implements, whereas java.lang.Comparable is only a ghost
                // interface of System.String
                var elements = new Expression[expressions.Count];
                for (int i = 0; i < elements.Length; i++)
                    elements[i] = ClrEnumUtils.Convert(expressions[i], typeof(IComparable));

                return Expression.Call(null, FlatListCopyOf, Expression.NewArrayInit(typeof(IComparable), elements));
            }

        }

        /// <summary>
        /// <see cref="JavaRowFormat.ROW"/>: a row that is an <c>org.apache.calcite.interpreter.Row</c>.
        /// </summary>
        sealed class RowConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType) => (java.lang.Class)typeof(org.apache.calcite.interpreter.Row);

            /// <inheritdoc />
            public override Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index) => typeof(object);

            /// <inheritdoc />
            public override Expression Record(Type rowClass, IReadOnlyList<Expression> expressions)
            {
                return Expression.Call(null, RowAsCopy, ObjectArray(expressions));
            }

            /// <inheritdoc />
            public override Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
            {
                return ClrEnumUtils.Convert(
                    Expression.Call(ClrEnumUtils.Convert(expression, typeof(org.apache.calcite.interpreter.Row)), RowValue, Expression.Constant(field)),
                    fromType, fieldType);
            }

        }

        /// <summary>
        /// <see cref="JavaRowFormat.ARRAY"/>: a row that is an <c>Object[]</c>.
        /// </summary>
        sealed class ArrayConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType) => (java.lang.Class)typeof(object[]);

            /// <inheritdoc />
            public override Type JavaFieldClass(JavaTypeFactory typeFactory, RelDataType rowType, int index) => typeof(object);

            /// <inheritdoc />
            public override Expression Record(Type rowClass, IReadOnlyList<Expression> expressions)
            {
                return ObjectArray(expressions);
            }

            /// <inheritdoc />
            public override Expression Field(Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
            {
                return ClrEnumUtils.Convert(
                    Expression.ArrayIndex(ClrEnumUtils.Convert(expression, typeof(object[])), Expression.Constant(field)),
                    fromType, fieldType);
            }

            /// <inheritdoc />
            /// <remarks>
            /// Arrays compare by reference, so rows of this format need <c>Functions.arrayComparer</c>.
            /// </remarks>
            public override Expression? Comparer() => Expression.Call(null, ArrayComparer);

        }

        /// <summary>
        /// Returns an <c>Object[]</c> of the expressions, each converted with
        /// <see cref="ClrEnumUtils.Convert(Expression, Type)"/>, which boxes primitives as Java boxes.
        /// </summary>
        static Expression ObjectArray(IReadOnlyList<Expression> expressions)
        {
            var elements = new Expression[expressions.Count];
            for (int i = 0; i < elements.Length; i++)
                elements[i] = ClrEnumUtils.Convert(expressions[i], typeof(object));

            return Expression.NewArrayInit(typeof(object), elements);
        }

        /// <summary>
        /// <c>BuiltInMethod</c> members, resolved from Calcite's own <c>java.lang.reflect.Method</c> to the CLR
        /// methods IKVM compiled them to, so they are the members <c>EnumerableConvention</c> calls.
        /// </summary>
        static readonly MethodInfo ListGet = ClrTypes.Resolve(BuiltInMethod.LIST_GET.method);

        /// <inheritdoc cref="ListGet" />
        static readonly MethodInfo RowValue = ClrTypes.Resolve(BuiltInMethod.ROW_VALUE.method);

        /// <inheritdoc cref="ListGet" />
        static readonly MethodInfo RowAsCopy = ClrTypes.Resolve(BuiltInMethod.ROW_AS_COPY.method);

        /// <inheritdoc cref="ListGet" />
        static readonly MethodInfo ArrayComparer = ClrTypes.Resolve(BuiltInMethod.ARRAY_COMPARER.method);

        /// <inheritdoc cref="ListGet" />
        static readonly MethodInfo FlatListCopyOf = ClrTypes.Resolve(BuiltInMethod.LIST_N.method);

        /// <inheritdoc cref="ListGet" />
        public static readonly MethodInfo FlatListOf1 = ClrTypes.Resolve(BuiltInMethod.LIST1.method);

        /// <inheritdoc cref="ListGet" />
        static readonly MethodInfo[] FlatListOf = [
            ClrTypes.Resolve(BuiltInMethod.LIST2.method),
            ClrTypes.Resolve(BuiltInMethod.LIST3.method),
            ClrTypes.Resolve(BuiltInMethod.LIST4.method),
            ClrTypes.Resolve(BuiltInMethod.LIST5.method),
            ClrTypes.Resolve(BuiltInMethod.LIST6.method)];

        /// <summary>
        /// Returns an expression reading a static member by name, as a field if there is one and otherwise as a
        /// property.
        /// </summary>
        /// <exception cref="InvalidOperationException">There is no static field or property of that name.</exception>
        /// <remarks>
        /// IKVM compiles a Java <c>static final</c> field to a property over a renamed backing field, so that
        /// reading it runs the class initializer; <c>FlatLists.COMPARABLE_EMPTY_LIST</c> has no CLR field of that
        /// name. The order matches <see cref="ClrTypes.Resolve(Expression, org.apache.calcite.linq4j.tree.PseudoField)"/>.
        /// </remarks>
        /// <param name="declaring">The type that declares the member.</param>
        /// <param name="name">The Java name of the member.</param>
        /// <returns>An expression reading the field or the property.</returns>
        static Expression StaticMember(Type declaring, string name)
        {
            const BindingFlags Static = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static;

            if (declaring.GetField(name, Static) is FieldInfo field)
                return Expression.Field(null, field);

            if (declaring.GetProperty(name, Static) is PropertyInfo property)
                return Expression.Property(null, property);

            throw new InvalidOperationException($"'{declaring}' has no static field or property '{name}'.");
        }

        /// <summary>
        /// <c>FlatLists.COMPARABLE_EMPTY_LIST</c>, the <see cref="JavaRowFormat.LIST"/> row of a type with no
        /// fields and the key of an empty key list.
        /// </summary>
        public static readonly Expression ComparableEmptyList = StaticMember(
            ClrTypes.FromClass(BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getDeclaringClass()),
            BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getName());

        /// <summary>
        /// <c>Unit.INSTANCE</c>, the <see cref="JavaRowFormat.CUSTOM"/> row of a type with no fields.
        /// </summary>
        static readonly Expression UnitInstance = StaticMember(typeof(org.apache.calcite.runtime.Unit), "INSTANCE");

    }

}
