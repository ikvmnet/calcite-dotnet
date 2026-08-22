using System;
using System.Reflection;
using System.Threading;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Resolves a plugin named by a connection string option — a type system, a parser factory — to an
    /// instance of it.
    /// </summary>
    /// <remarks>
    /// This is <c>AvaticaUtils.instantiatePlugin</c>'s reach, expressed the way .NET writes such things.
    /// Avatica reaches four things: a class with a public no-argument constructor, a class carrying a
    /// public static <c>INSTANCE</c> field, a named static field of a class, and a thread-local holding
    /// any of those. Each has a form here.
    ///
    /// <para>The type is an ordinary .NET type name, resolved by <see cref="Type.GetType(string)"/> —
    /// which searches this assembly and the core library and nowhere else, so a type from anywhere else
    /// carries its assembly, <c>Namespace.Type, Assembly</c>. That is how a Java-defined plugin is named
    /// too, through its IKVM projection and the assembly holding it.</para>
    ///
    /// <para>A static member is named <c>[Namespace.Type, Assembly]::Member</c>. .NET has no expression
    /// of its own for a type and one of its static members in a single string — <c>Type.GetType</c>'s
    /// grammar covers types alone, and the XML documentation id (<c>F:</c>, <c>P:</c>, <c>M:</c>) is an
    /// identifier for documentation tooling that nothing resolves — but it does have a convention, the
    /// one PowerShell and MSBuild property functions both use: <c>[System.Math]::PI</c>. That is the
    /// form taken here, because a .NET user has read it before. The brackets are what make it parse:
    /// they hold an unmodified .NET type name, assembly and generic arguments and all, and the member
    /// sits outside them where no part of it can be mistaken for a namespace.</para>
    ///
    /// <para>The member may be a field, a property or a parameterless method, tried in that order. A
    /// field is Avatica's only form; a Java <c>static final</c> surfaces as a CLR property under IKVM, so
    /// nothing Calcite ships is reachable without the second; and a .NET type is as likely to hand out
    /// its instance from a method as from a field.</para>
    ///
    /// <para>Calcite's own <c>Namespace.Type#MEMBER</c> is read as well, and is not the convention. It
    /// costs one branch, and it is what a connection string copied out of Calcite's documentation says;
    /// refusing it would fail in a way that reads like the type is missing.</para>
    /// </remarks>
    static class ClrPlugin
    {

        /// <summary>
        /// Resolves <paramref name="name"/> to an instance of <typeparamref name="T"/>.
        /// </summary>
        /// <typeparam name="T">The plugin's type.</typeparam>
        /// <param name="name">The name from the connection string, or <see langword="null"/>.</param>
        /// <param name="fallback">The instance to answer where nothing is named.</param>
        /// <returns>The resolved plugin.</returns>
        /// <exception cref="InvalidOperationException">Where the name does not resolve.</exception>
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

            // a type that hands out a singleton is named by the type alone, which is Avatica's convention
            // and .NET's both; only the casing differs, so both spellings are read
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
        /// <exception cref="InvalidOperationException">Where the bracketed form is not closed.</exception>
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
        /// Unwraps a thread-local holder, which Avatica's plugin path reaches and which both runtimes
        /// express.
        /// </summary>
        /// <param name="value">The member's value.</param>
        /// <returns>The value held, where the value is a thread-local.</returns>
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
        /// <exception cref="InvalidOperationException">Where the value is of another type.</exception>
        static T Cast<T>(string name, object? value)
            where T : class
        {
            return value as T ?? throw new InvalidOperationException(
                $"'{name}' resolved to {value?.GetType().FullName ?? "null"}, which is not a {typeof(T).FullName}.");
        }

    }

}
