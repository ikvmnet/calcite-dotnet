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

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The members of <see cref="JavaRowFormat"/> a <see cref="ClrPhysType"/> asks for, answered for the CLR.
    /// </summary>
    /// <remarks>
    /// The format itself stays Calcite's. Two of its members cannot be used as they are — <c>record</c> and
    /// <c>field</c> answer in linq4j — and two more are package private — <c>javaRowClass</c> and
    /// <c>javaFieldClass</c>. Those four are here. <c>optimize</c> is public and is called rather than written
    /// again, which is what keeps the decision of what a row is out of this file.
    ///
    /// <para>This is a switch on <c>name()</c> where Calcite has a class per constant, which is what a Java
    /// enum with method bodies is. Saying it the way C# would say it is a pass of its own, along with a row
    /// format belonging to this convention rather than to the other one.</para>
    ///
    /// <para>Dispatch is on <c>name()</c> against <c>nameof</c> labels, because a Java enum's ordinals are not
    /// stable across versions and its names are.</para>
    /// </remarks>
    static class JavaRowFormatExtensions
    {

        /// <summary>
        /// Returns the CLR type that represents a row of the given format.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.javaRowClass</c>, which is package private and is a lookup on the public
        /// <c>JavaTypeFactory.getJavaClass</c> for two of the formats and a constant for the other three. The
        /// type factory names a synthetic record for a row it has no class for, and that name is resolved to
        /// the emitted CLR type here, so nothing above this holds one.
        /// </remarks>
        public static Type JavaRowClass(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            return ClrTypes.Resolve(format.JavaRowType(typeFactory, rowType));
        }

        /// <summary>
        /// Returns the type the type factory names a row of the given format by.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.javaRowClass</c> as it stands, before resolving. A row of a struct is a synthetic
        /// record the factory owns, and the fields of one are reached by name through it rather than by
        /// walking CLR reflection, which is what <c>Types.nthField</c> does and what a translated field node
        /// resolves through.
        /// </remarks>
        public static java.lang.reflect.Type JavaRowType(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            ArgumentNullException.ThrowIfNull(format);
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            switch (format.name())
            {
                case nameof(JavaRowFormat.CUSTOM):
                    return typeFactory.getJavaClass(rowType);

                case nameof(JavaRowFormat.SCALAR):
                    var field0Type = ((RelDataTypeField)rowType.getFieldList().get(0)).getType();

                    // a nested ROW is always an array, whatever the field's own class would be
                    return field0Type.getSqlTypeName() == SqlTypeName.ROW
                        ? (java.lang.Class)typeof(object[])
                        : typeFactory.getJavaClass(field0Type);

                case nameof(JavaRowFormat.LIST):
                    return (java.lang.Class)typeof(org.apache.calcite.runtime.FlatLists.ComparableList);

                case nameof(JavaRowFormat.ROW):
                    return (java.lang.Class)typeof(org.apache.calcite.interpreter.Row);

                case nameof(JavaRowFormat.ARRAY):
                    return (java.lang.Class)typeof(object[]);

                default:
                    throw new NotSupportedException($"There is no row class for the row format '{format.name()}'.");
            }
        }

        /// <summary>
        /// Returns the CLR type used to store one field of a row of the given format.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.javaFieldClass</c>, which is package private. A row that holds its fields as
        /// objects stores one as an object even where the field is not nullable.
        /// </remarks>
        public static Type JavaFieldClass(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType, int index)
        {
            ArgumentNullException.ThrowIfNull(format);
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            return format.name() switch
            {
                nameof(JavaRowFormat.CUSTOM) => ClrTypes.Resolve(typeFactory.getJavaClass(((RelDataTypeField)rowType.getFieldList().get(index)).getType())),
                nameof(JavaRowFormat.SCALAR) => format.JavaRowClass(typeFactory, rowType),
                nameof(JavaRowFormat.LIST) or nameof(JavaRowFormat.ROW) or nameof(JavaRowFormat.ARRAY) => typeof(object),
                _ => throw new NotSupportedException($"There is no field class for the row format '{format.name()}'.")
            };
        }

        /// <summary>
        /// Returns an expression building a row of the given format from one expression per field.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="rowClass">The CLR type of a row, which is what a record format constructs.</param>
        /// <param name="expressions"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.record</c>, which cannot be called because it takes and returns linq4j.
        /// </remarks>
        public static Expression Record(this JavaRowFormat format, Type rowClass, IReadOnlyList<Expression> expressions)
        {
            ArgumentNullException.ThrowIfNull(format);
            ArgumentNullException.ThrowIfNull(rowClass);
            ArgumentNullException.ThrowIfNull(expressions);

            switch (format.name())
            {
                case nameof(JavaRowFormat.CUSTOM):
                    // Calcite builds a record of many fields one field at a time rather than through a
                    // constructor, which it settled on under CALCITE-1097 because Janino fails on a constructor
                    // with too many parameters. An expression tree has no such limit and no statements to put
                    // the assignments in, so the constructor is what is called.
                    if (expressions.Count == 0)
                        return Expression.Field(null, UnitInstance);

                    var constructor = Constructor(rowClass, expressions.Count);
                    var parameters = constructor.GetParameters();
                    var arguments = new Expression[expressions.Count];
                    for (int i = 0; i < arguments.Length; i++)
                        arguments[i] = ClrEnumUtils.Convert(expressions[i], parameters[i].ParameterType);

                    return Expression.New(constructor, arguments);

                case nameof(JavaRowFormat.SCALAR):
                    if (expressions.Count != 1)
                        throw new NotSupportedException($"A scalar row is one expression, not {expressions.Count}.");

                    return expressions[0];

                case nameof(JavaRowFormat.LIST):
                    if (expressions.Count == 0)
                        return Expression.Field(null, ComparableEmptyList);

                    return ClrEnumUtils.Convert(FlatList(expressions), typeof(java.util.List));

                case nameof(JavaRowFormat.ROW):
                    return Expression.Call(null, RowAsCopy, ObjectArray(expressions));

                case nameof(JavaRowFormat.ARRAY):
                    return ObjectArray(expressions);

                default:
                    throw new NotSupportedException($"There is no record for the row format '{format.name()}'.");
            }
        }

        /// <summary>
        /// Returns an expression reading one field of a row of the given format.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="expression"></param>
        /// <param name="field"></param>
        /// <param name="fromType">The field's own type where it differs from what the row holds, or null.</param>
        /// <param name="fieldType">The type the value is wanted as.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.field</c>, which cannot be called because it takes and returns linq4j.
        /// </remarks>
        public static Expression Field(this JavaRowFormat format, Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
        {
            ArgumentNullException.ThrowIfNull(format);
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(fieldType);

            switch (format.name())
            {
                case nameof(JavaRowFormat.CUSTOM):
                    return ClrTypes.Resolve(expression, org.apache.calcite.linq4j.tree.Types.nthField(field, javaRowType));

                case nameof(JavaRowFormat.SCALAR):
                    if (field != 0)
                        throw new NotSupportedException($"A scalar row has one field, so there is no field {field}.");

                    return expression;

                case nameof(JavaRowFormat.LIST):
                    return ClrEnumUtils.Convert(
                        Expression.Call(ClrEnumUtils.Convert(expression, typeof(java.util.List)), ListGet, Expression.Constant(field)),
                        fromType, fieldType);

                case nameof(JavaRowFormat.ROW):
                    return ClrEnumUtils.Convert(
                        Expression.Call(ClrEnumUtils.Convert(expression, typeof(org.apache.calcite.interpreter.Row)), RowValue, Expression.Constant(field)),
                        fromType, fieldType);

                case nameof(JavaRowFormat.ARRAY):
                    return ClrEnumUtils.Convert(
                        Expression.ArrayIndex(ClrEnumUtils.Convert(expression, typeof(object[])), Expression.Constant(field)),
                        fromType, fieldType);

                default:
                    throw new NotSupportedException($"There is no field access for the row format '{format.name()}'.");
            }
        }

        /// <summary>
        /// Returns the expression yielding a comparer for rows of the given format, or null where a row
        /// compares itself.
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.comparer</c>, which only <see cref="JavaRowFormat.ARRAY"/> answers.
        /// </remarks>
        public static Expression? Comparer(this JavaRowFormat format)
        {
            ArgumentNullException.ThrowIfNull(format);

            return format.name() == nameof(JavaRowFormat.ARRAY)
                ? Expression.Call(null, ArrayComparer)
                : null;
        }

        /// <summary>
        /// Returns the constructor of the given arity.
        /// </summary>
        static ConstructorInfo Constructor(Type rowClass, int arity)
        {
            foreach (var constructor in rowClass.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
                if (constructor.GetParameters().Length == arity)
                    return constructor;

            throw new NotSupportedException($"'{rowClass}' has no constructor of {arity} parameters.");
        }

        /// <summary>
        /// Returns an array of every expression, each as an object.
        /// </summary>
        static Expression ObjectArray(IReadOnlyList<Expression> expressions)
        {
            var elements = new Expression[expressions.Count];
            for (int i = 0; i < elements.Length; i++)
                elements[i] = ClrEnumUtils.Convert(expressions[i], typeof(object));

            return Expression.NewArrayInit(typeof(object), elements);
        }

        /// <summary>
        /// Returns the call building a comparable list of the given expressions.
        /// </summary>
        /// <param name="expressions"></param>
        /// <returns></returns>
        /// <remarks>
        /// One <c>FlatLists.of</c> overload per arity to six, and a copy of an array beyond that. A list of one
        /// is not among them, because a row of one field is <see cref="JavaRowFormat.SCALAR"/> by the time a
        /// record is built; <c>generateNullAwareAccessor</c> needs one anyway and reaches
        /// <see cref="FlatListOf1"/> for it.
        /// </remarks>
        static Expression FlatList(IReadOnlyList<Expression> expressions)
        {
            if (expressions.Count is >= 2 and <= 6)
            {
                var arguments = new Expression[expressions.Count];
                for (int i = 0; i < arguments.Length; i++)
                    arguments[i] = ClrEnumUtils.Convert(expressions[i], typeof(object));

                return Expression.Call(null, FlatListOf[expressions.Count - 2], arguments);
            }

            var elements = new Expression[expressions.Count];
            for (int i = 0; i < elements.Length; i++)
                elements[i] = ClrEnumUtils.Convert(expressions[i], typeof(java.lang.Comparable));

            return Expression.Call(null, FlatListCopyOf, Expression.NewArrayInit(typeof(java.lang.Comparable), elements));
        }

        /// <summary>
        /// The members Calcite names through <c>BuiltInMethod</c>, resolved once against what IKVM compiled.
        /// </summary>
        /// <remarks>
        /// Each is resolved from the <c>java.lang.reflect.Method</c> Calcite itself names, rather than looked
        /// up by a signature written again here, so the member is the one <c>EnumerableConvention</c> calls.
        /// </remarks>
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
        /// <c>FlatLists.COMPARABLE_EMPTY_LIST</c>, which is the row of a type with no fields.
        /// </summary>
        public static readonly FieldInfo ComparableEmptyList = ClrTypes.FromClass(BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getDeclaringClass())
            .GetField(BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getName(), BindingFlags.Public | BindingFlags.Static)!;

        /// <summary>
        /// <c>Unit.INSTANCE</c>, which is the row of a custom type with no fields.
        /// </summary>
        static readonly FieldInfo UnitInstance = typeof(org.apache.calcite.runtime.Unit)
            .GetField("INSTANCE", BindingFlags.Public | BindingFlags.Static)!;

    }

}
