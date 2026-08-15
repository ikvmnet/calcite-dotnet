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
    /// <c>javaFieldClass</c>. Those four are here, and <c>comparer</c> with them. <c>optimize</c> is public and
    /// is called rather than written again, which is what keeps the decision of what a row is out of this file.
    ///
    /// <para>Calcite writes these as the bodies of its enum constants, which is a class per format in the one
    /// place Java allows one. That is <see cref="Constant"/> and its five subclasses here, so a format's
    /// answers sit together rather than spread across a switch per member. <see cref="Of"/> is the only switch,
    /// and it is on <c>name()</c> against <c>nameof</c> labels, because a Java enum's ordinals are not stable
    /// across versions and its names are.</para>
    /// </remarks>
    static class JavaRowFormatExtensions
    {

        /// <summary>
        /// Returns the type the type factory names a row of the given format by.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.javaRowClass</c> as it stands, before resolving. A row of a struct is a synthetic
        /// record the factory owns, and the fields of one are reached by name through it rather than by walking
        /// CLR reflection, which is what <c>Types.nthField</c> does and what a translated field node resolves
        /// through.
        /// </remarks>
        public static java.lang.reflect.Type JavaRowType(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            return Of(format).JavaRowType(typeFactory, rowType);
        }

        /// <summary>
        /// Returns the CLR type that represents a row of the given format.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="typeFactory"></param>
        /// <param name="rowType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <see cref="JavaRowType"/>, resolved, so that nothing above this holds the factory's name for a row.
        /// </remarks>
        public static Type JavaRowClass(this JavaRowFormat format, JavaTypeFactory typeFactory, RelDataType rowType)
        {
            return ClrTypes.Resolve(format.JavaRowType(typeFactory, rowType));
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
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(rowType);

            return Of(format).JavaFieldClass(typeFactory, rowType, index);
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
            ArgumentNullException.ThrowIfNull(rowClass);
            ArgumentNullException.ThrowIfNull(expressions);

            return Of(format).Record(rowClass, expressions);
        }

        /// <summary>
        /// Returns an expression reading one field of a row of the given format.
        /// </summary>
        /// <param name="format"></param>
        /// <param name="expression"></param>
        /// <param name="field"></param>
        /// <param name="fromType">The field's own type where it differs from what the row holds, or null.</param>
        /// <param name="fieldType">The type the value is wanted as.</param>
        /// <param name="javaRowType">The factory's name for a row, which is how a record's fields are reached.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>JavaRowFormat.field</c>, which cannot be called because it takes and returns linq4j.
        /// </remarks>
        public static Expression Field(this JavaRowFormat format, Expression expression, int field, Type? fromType, Type fieldType, java.lang.reflect.Type javaRowType)
        {
            ArgumentNullException.ThrowIfNull(expression);
            ArgumentNullException.ThrowIfNull(fieldType);
            ArgumentNullException.ThrowIfNull(javaRowType);

            return Of(format).Field(expression, field, fromType, fieldType, javaRowType);
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
            return Of(format).Comparer();
        }

        /// <summary>
        /// Returns the members of one constant of <see cref="JavaRowFormat"/>.
        /// </summary>
        /// <param name="format"></param>
        /// <returns></returns>
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
        /// What one constant of <see cref="JavaRowFormat"/> answers.
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
        /// A row that is an instance of a class, one field per column.
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
            /// Calcite builds a record of many fields one field at a time rather than through a constructor,
            /// which it settled on under CALCITE-1097 because Janino fails on a constructor with too many
            /// parameters. An expression tree has no such limit and no statements to put the assignments in, so
            /// the constructor is what is called.
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
            /// Returns the constructor of the given arity.
            /// </summary>
            static ConstructorInfo Constructor(Type rowClass, int arity)
            {
                foreach (var constructor in rowClass.GetConstructors(BindingFlags.Public | BindingFlags.Instance))
                    if (constructor.GetParameters().Length == arity)
                        return constructor;

                throw new NotSupportedException($"'{rowClass}' has no constructor of {arity} parameters.");
            }

        }

        /// <summary>
        /// A row of one column, which is the value itself.
        /// </summary>
        sealed class ScalarConstant : Constant
        {

            /// <inheritdoc />
            public override java.lang.reflect.Type JavaRowType(JavaTypeFactory typeFactory, RelDataType rowType)
            {
                var field0Type = ((RelDataTypeField)rowType.getFieldList().get(0)).getType();

                // a nested ROW is always an array, whatever the field's own class would be
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
        /// A row that is a list, which is comparable and immutable, and so can key a lookup.
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
            /// Returns the call building a comparable list of the given expressions.
            /// </summary>
            /// <param name="expressions"></param>
            /// <returns></returns>
            /// <remarks>
            /// One <c>FlatLists.of</c> overload per arity to six, and a copy of an array beyond that. A list of
            /// one is not among them, because a row of one field is <see cref="JavaRowFormat.SCALAR"/> by the
            /// time a record is built; <c>generateNullAwareAccessor</c> needs one anyway and reaches
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

                // Calcite writes newArrayInit(Comparable.class, ...), and IKVM erases a java.lang.Comparable
                // to IComparable in every signature it compiles, copyOf's parameter included. The two differ
                // in what they accept: a string has IComparable and has java.lang.Comparable only as a ghost.
                var elements = new Expression[expressions.Count];
                for (int i = 0; i < elements.Length; i++)
                    elements[i] = ClrEnumUtils.Convert(expressions[i], typeof(IComparable));

                return Expression.Call(null, FlatListCopyOf, Expression.NewArrayInit(typeof(IComparable), elements));
            }

        }

        /// <summary>
        /// A row that is an <c>org.apache.calcite.interpreter.Row</c>.
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
        /// A row that is an object array.
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
            /// A row of this format is an array, whose own equality is by reference, so a set operation over one
            /// is wrong without this.
            /// </remarks>
            public override Expression? Comparer() => Expression.Call(null, ArrayComparer);

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
        /// Returns the expression reading a Java <c>static final</c> field.
        /// </summary>
        /// <param name="declaring"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException"></exception>
        /// <remarks>
        /// <b>IKVM does not compile one to a field of that name.</b> It emits a property, over a backing field
        /// it renames — <c>FlatLists.COMPARABLE_EMPTY_LIST</c> is a <c>COMPARABLE_EMPTY_LIST</c> property over
        /// a <c>__&lt;&gt;COMPARABLE_EMPTY_LIST</c> field — so that reading it from C# still runs the class
        /// initializer, which is what Java guarantees and what a bare field read would skip. Measured:
        /// <c>GetField("COMPARABLE_EMPTY_LIST")</c> answers nothing.
        ///
        /// <para>So a static member is read as an expression rather than held as a <see cref="FieldInfo"/>,
        /// and the two are tried in the order <see cref="ClrTypes.Resolve(Expression, J.PseudoField)"/> tries
        /// them — field first, because a CLR field of ours is a field.</para>
        /// </remarks>
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
        /// <c>FlatLists.COMPARABLE_EMPTY_LIST</c>, which is the row of a type with no fields.
        /// </summary>
        public static readonly Expression ComparableEmptyList = StaticMember(
            ClrTypes.FromClass(BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getDeclaringClass()),
            BuiltInMethod.COMPARABLE_EMPTY_LIST.field.getName());

        /// <summary>
        /// <c>Unit.INSTANCE</c>, which is the row of a custom type with no fields.
        /// </summary>
        static readonly Expression UnitInstance = StaticMember(typeof(org.apache.calcite.runtime.Unit), "INSTANCE");

    }

}
