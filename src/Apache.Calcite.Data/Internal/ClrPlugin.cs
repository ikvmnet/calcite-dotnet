using System;
using System.Reflection;
using System.Threading;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Resolves a plugin named in the connection string, such as the <c>TypeSystem</c> key's type system, to
    /// an instance.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The .NET counterpart of Avatica's <c>AvaticaUtils.instantiatePlugin</c>, covering the same cases: a
    /// type with a public parameterless constructor, a type with a public static <c>INSTANCE</c> member, a
    /// named static member of a type, and a thread-local holding any of those.
    /// </para>
    /// <para>
    /// The type is a .NET type name resolved by <see cref="Type.GetType(string)"/>, which searches only this
    /// assembly and the core library, so any other type is written with its assembly:
    /// <c>Namespace.Type, Assembly</c>. A Java class is named by its IKVM projection and the assembly that
    /// holds it.
    /// </para>
    /// <para>
    /// A static member is written <c>[Namespace.Type, Assembly]::Member</c>, the convention PowerShell and
    /// MSBuild property functions use (<c>[System.Math]::PI</c>). The brackets hold an unmodified type name,
    /// including assembly and generic arguments. Calcite's <c>Namespace.Type#MEMBER</c> form is also
    /// accepted.
    /// </para>
    /// <para>
    /// The member may be a field, a property or a parameterless method, tried in that order. The property
    /// case is needed for Java classes, because IKVM exposes a Java <c>static final</c> field as a property.
    /// </para>
    /// </remarks>
    static class ClrPlugin
    {

        /// <summary>
        /// Resolves <paramref name="name"/> to an instance of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The plugin's type.</typeparam>
        /// <param name="name">The name from the connection string, or <see langword="null"/>.</param>
        /// <param name="fallback">The instance to return where nothing is named.</param>
        /// <returns>The resolved plugin.</returns>
        /// <exception cref="InvalidOperationException">The name does not resolve to an instance of
        /// <typeparamref name="T"/>, or its constructor throws.</exception>
        /// <remarks>
        /// A type named without a member is resolved by its static <c>INSTANCE</c> or <c>Instance</c> member
        /// where it has one, and otherwise by its public parameterless constructor.
        /// </remarks>
        public static T Resolve<T>(string? name, T fallback)
            where T : class
        {
            if (string.IsNullOrWhiteSpace(name))
                return fallback;

            var (typeName, memberName) = Split(name);
            var type = Type.GetType(typeName) ?? throw new InvalidOperationException(
                $"'{name}' does not name a .NET type: '{typeName}' was not found. A type outside " +
                $"{typeof(ClrPlugin).Assembly.GetName().Name} carries its assembly, as in 'Namespace.Type, Assembly'.");

            if (memberName is not null)
            {
                if (TryStaticMember(type, memberName, out var named) == false)
                    throw new InvalidOperationException(
                        $"'{name}' does not name a member: '{type.FullName}' has no public static field, " +
                        $"property or parameterless method '{memberName}'.");

                return Cast<T>(name, Unwrap(named));
            }

            // a singleton named by its type alone: Avatica spells the member INSTANCE and .NET Instance
            if (TryStaticMember(type, "INSTANCE", out var instance) || TryStaticMember(type, "Instance", out instance))
                return Cast<T>(name, Unwrap(instance));

            if (typeof(T).IsAssignableFrom(type) == false)
                throw new InvalidOperationException(
                    $"'{name}' names '{type.FullName}', which is not a {typeof(T).FullName}.");

            var constructor = type.GetConstructor(Type.EmptyTypes) ?? throw new InvalidOperationException(
                $"'{name}' names '{type.FullName}', which has neither a public parameterless constructor " +
                $"nor a static Instance member.");

            try
            {
                return Cast<T>(name, constructor.Invoke(null));
            }
            catch (TargetInvocationException e)
            {
                throw new InvalidOperationException(
                    $"'{name}' names '{type.FullName}', whose constructor threw: {e.InnerException?.Message ?? e.Message}", e);
            }
        }

        /// <summary>
        /// Splits a name into its type and, where one is named, its static member.
        /// </summary>
        /// <param name="name">The name from the connection string.</param>
        /// <returns>The type name, and the member name or <see langword="null"/>.</returns>
        /// <exception cref="InvalidOperationException">The name opens with <c>[</c> and has no <c>]::</c>.</exception>
        static (string TypeName, string? MemberName) Split(string name)
        {
            name = name.Trim();

            // [Namespace.Type, Assembly]::Member. A leading '[' cannot begin a .NET type name — the
            // brackets of an array or of a generic argument list always follow one — so it decides the
            // form on its own. The close is sought from the end, a generic argument list holding ']' too.
            if (name.StartsWith('['))
            {
                var i = name.LastIndexOf("]::", StringComparison.Ordinal);
                if (i < 0)
                    throw new InvalidOperationException(
                        $"'{name}' opens with '[', so it is read as '[Namespace.Type, Assembly]::Member', " +
                        $"but nothing closes it with ']::'.");

                return (name.Substring(1, i - 1).Trim(), name.Substring(i + 3).Trim());
            }

            // Namespace.Type#MEMBER, Calcite's own syntax, read as written
            var hash = name.IndexOf('#');
            if (hash >= 0)
                return (name.Substring(0, hash).Trim(), name.Substring(hash + 1).Trim());

            return (name, null);
        }

        /// <summary>
        /// Reads a public static member, trying field, then property, then parameterless method.
        /// </summary>
        /// <param name="type">The type holding the member.</param>
        /// <param name="name">The member name.</param>
        /// <param name="value">The member's value, where found.</param>
        /// <returns><see langword="true"/> where the member exists.</returns>
        static bool TryStaticMember(Type type, string name, out object? value)
        {
            var field = type.GetField(name, BindingFlags.Public | BindingFlags.Static);
            if (field is not null)
            {
                value = field.GetValue(null);
                return true;
            }

            var property = type.GetProperty(name, BindingFlags.Public | BindingFlags.Static);
            if (property is not null)
            {
                value = property.GetValue(null);
                return true;
            }

            var method = type.GetMethod(name, BindingFlags.Public | BindingFlags.Static, null, Type.EmptyTypes, null);
            if (method is not null && method.ReturnType != typeof(void))
            {
                value = method.Invoke(null, null);
                return true;
            }

            value = null;
            return false;
        }

        /// <summary>
        /// Unwraps a Java <c>ThreadLocal</c> or a .NET <see cref="ThreadLocal{T}"/>, as Avatica unwraps a
        /// thread-local plugin.
        /// </summary>
        /// <param name="value">The member's value.</param>
        /// <returns>The value held where <paramref name="value"/> is a thread-local, otherwise
        /// <paramref name="value"/>.</returns>
        static object? Unwrap(object? value)
        {
            if (value is java.lang.ThreadLocal javaLocal)
                return javaLocal.get();

            var type = value?.GetType();
            if (type is not null && type.IsGenericType && type.GetGenericTypeDefinition() == typeof(ThreadLocal<>))
                return type.GetProperty("Value")?.GetValue(value);

            return value;
        }

        /// <summary>
        /// Casts a resolved value to the plugin's type.
        /// </summary>
        /// <typeparam name="T">The plugin's type.</typeparam>
        /// <param name="name">The name from the connection string, for the message.</param>
        /// <param name="value">The resolved value.</param>
        /// <returns>The value.</returns>
        /// <exception cref="InvalidOperationException">The value is null or not a <typeparamref name="T"/>.</exception>
        static T Cast<T>(string name, object? value)
            where T : class
        {
            return value as T ?? throw new InvalidOperationException(
                $"'{name}' resolved to {value?.GetType().FullName ?? "null"}, which is not a {typeof(T).FullName}.");
        }

    }

}
