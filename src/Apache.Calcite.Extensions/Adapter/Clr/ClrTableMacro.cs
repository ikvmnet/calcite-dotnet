using System;
using System.Reflection;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// A table macro over a .NET method that returns a table.
    /// </summary>
    /// <remarks>
    /// <c>ReflectiveSchema.MethodTableMacro</c>. Calcite builds one from a <c>java.lang.reflect.Method</c>
    /// through <c>ReflectiveFunctionBase</c>, which reads the parameters and their <c>@Parameter</c>
    /// annotations; .NET gives parameter names without an annotation and optionality with them, so the
    /// parameters are read directly and <c>ReflectiveFunctionBase</c> is not used.
    ///
    /// <para><b>An argument arrives as Calcite's value and the method wants .NET's.</b> That is the reverse
    /// of every other crossing in this adapter and it is a boundary like any other: a <c>java.lang.Integer</c>
    /// is not an <see cref="int"/> to a <c>MethodInfo.Invoke</c>. Only the types
    /// <see cref="ClrTypeMapper.IsNativelyRepresented"/> admits are accepted as parameters, so the
    /// conversion is an unboxing and nothing more; a parameter of any other type is refused when the macro is
    /// built rather than when it is called.</para>
    /// </remarks>
    public class ClrTableMacro : TableMacro
    {

        readonly object target;
        readonly MethodInfo method;
        readonly java.util.List parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="target">The object the method is called on.</param>
        /// <param name="method">A method returning a <see cref="Table"/>.</param>
        public ClrTableMacro(object target, MethodInfo method)
        {
            this.target = target ?? throw new ArgumentNullException(nameof(target));
            this.method = method ?? throw new ArgumentNullException(nameof(method));

            var list = new java.util.ArrayList();
            var infos = method.GetParameters();
            for (int i = 0; i < infos.Length; i++)
            {
                if (ClrTypeMapper.IsNativelyRepresented(infos[i].ParameterType) == false)
                    throw new NotSupportedException(
                        $"'{method.DeclaringType}.{method.Name}' takes a '{infos[i].ParameterType}', which is not a type an argument can arrive as.");

                list.add(new ClrFunctionParameter(i, infos[i]));
            }

            parameters = java.util.Collections.unmodifiableList(list);
        }

        /// <inheritdoc />
        public java.util.List getParameters() => parameters;

        /// <inheritdoc />
        public TranslatableTable apply(java.util.List arguments)
        {
            var infos = method.GetParameters();
            var values = new object?[infos.Length];
            for (int i = 0; i < values.Length; i++)
                values[i] = Argument(arguments.get(i), infos[i].ParameterType);

            var table = method.Invoke(target, values)
                ?? throw new java.lang.IllegalStateException($"'{method.Name}' returned null.");

            return table as TranslatableTable
                ?? throw new NotSupportedException(
                    $"'{method.Name}' returned a '{table.GetType()}', which is not a TranslatableTable, so a macro cannot expand to it.");
        }

        /// <summary>
        /// Returns one argument as the .NET value the method takes.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// The arguments are the values Calcite computed, so a number is a <c>java.lang.Number</c> and a
        /// string is a <c>java.lang.String</c>, which IKVM has already made a <see cref="string"/>. Unboxing
        /// through <c>Number</c> rather than through a cast to a particular box is what lets a literal
        /// <c>1</c> — which Calcite may have made an <c>Integer</c>, a <c>Long</c> or a <c>BigDecimal</c>
        /// depending on where it came from — reach a parameter of any width.
        /// </remarks>
        static object? Argument(object? value, Type type)
        {
            if (value == null)
                return null;

            var underlying = Nullable.GetUnderlyingType(type) ?? type;

            if (value is java.lang.Number number)
            {
                if (underlying == typeof(int)) return number.intValue();
                if (underlying == typeof(long)) return number.longValue();
                if (underlying == typeof(short)) return number.shortValue();
                if (underlying == typeof(double)) return number.doubleValue();
                if (underlying == typeof(float)) return number.floatValue();
            }

            if (value is java.lang.Boolean boolean && underlying == typeof(bool))
                return boolean.booleanValue();

            if (underlying.IsInstanceOfType(value))
                return value;

            throw new NotSupportedException($"'{value.GetType()}' is not a '{type}'.");
        }

        /// <inheritdoc />
        public override string ToString() => $"ClrTableMacro({method})";

    }

    /// <summary>
    /// One parameter of a <see cref="ClrTableMacro"/>.
    /// </summary>
    /// <remarks>
    /// <c>ReflectiveFunctionBase.ParameterListBuilder</c>'s parameter, over a <see cref="ParameterInfo"/>.
    /// .NET names a parameter always, where Java names one only under a <c>@Parameter</c> annotation and
    /// falls back to <c>a0</c>, <c>a1</c>; and it states optionality in the signature, where Java states it
    /// in the annotation.
    /// </remarks>
    public class ClrFunctionParameter : FunctionParameter
    {

        readonly int ordinal;
        readonly ParameterInfo parameter;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <param name="parameter"></param>
        public ClrFunctionParameter(int ordinal, ParameterInfo parameter)
        {
            this.ordinal = ordinal;
            this.parameter = parameter ?? throw new ArgumentNullException(nameof(parameter));
        }

        /// <inheritdoc />
        public int getOrdinal() => ordinal;

        /// <inheritdoc />
        public string getName() => parameter.Name ?? $"a{ordinal}";

        /// <inheritdoc />
        public RelDataType getType(RelDataTypeFactory typeFactory)
        {
            return ClrTypeMapper.ColumnType((JavaTypeFactory)typeFactory, parameter.ParameterType)
                ?? throw new NotSupportedException($"'{parameter.ParameterType}' has no SQL type.");
        }

        /// <inheritdoc />
        public bool isOptional() => parameter.IsOptional;

    }

}
