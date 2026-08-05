using System;
using System.Linq.Expressions;
using System.Reflection;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Builds the <see cref="Expression"/> that reads the field a linq4j <see cref="J.MemberExpression"/> names.
    /// </summary>
    /// <remarks>
    /// linq4j does not require a field to be a real one. It reads the length of an array as a field, and a
    /// synthetic record's fields belong to a type that has no reflection behind it until one is emitted.
    /// </remarks>
    static class FieldResolver
    {

        const BindingFlags All = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;

        /// <summary>
        /// Returns an expression reading <paramref name="field"/> of <paramref name="target"/>, which is null
        /// for a static field.
        /// </summary>
        /// <param name="target"></param>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static Expression Resolve(Expression? target, J.PseudoField field)
        {
            ArgumentNullException.ThrowIfNull(field);

            // an array's length is a field in Java and a property in the CLR
            if (field is J.ArrayLengthRecordField)
                return Expression.ArrayLength(target ?? throw new NotSupportedException("An array length needs an array."));

            var declaring = TypeResolver.Resolve(field.getDeclaringClass());
            var name = field.getName();

            var info = declaring.GetField(name, All);
            if (info != null)
                return Expression.Field(info.IsStatic ? null : target, info);

            // IKVM exposes a .NET property to Java as a field of the same name, so a linq4j tree reaching one
            // of ours reads it the same way it reads a Java field
            var property = declaring.GetProperty(name, All);
            if (property != null)
                return Expression.Property(property.GetMethod?.IsStatic == true ? null : target, property);

            throw new NotSupportedException($"'{declaring}' has no field or property '{name}'.");
        }

    }

}
