using System;
using System.Linq.Expressions;
using System.Reflection;

using JavaType = java.lang.reflect.Type;
using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Resolves the Java types, methods and fields a linq4j expression tree names to the CLR types, methods
    /// and members IKVM compiled for them.
    /// </summary>
    /// <remarks>
    /// The counterpart of linq4j's <c>Types</c> for a tree that runs as a CLR expression tree rather than as
    /// Java source. A linq4j call's recorded method is advisory: Janino writes the call out as source and the
    /// Java compiler chooses the overload and the receiver's method from the argument and receiver types.
    /// <see cref="Rebind"/> and <see cref="RebindReceiver"/> make the same choice here.
    /// </remarks>
    public static class ClrTypes
    {

        /// <summary>
        /// Resolves a Java reflection type to its CLR type.
        /// </summary>
        /// <param name="type">A <c>java.lang.Class</c>, a Calcite synthetic record type, a parameterized type
        /// or a generic array type.</param>
        /// <returns>The CLR type. A synthetic record type resolves to a CLR class emitted for it.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="type"/> is <see langword="null"/>.</exception>
        /// <exception cref="NotSupportedException"><paramref name="type"/> is of another kind.</exception>
        public static Type Resolve(JavaType type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return type switch
            {
                java.lang.Class c => FromClass(c),
                org.apache.calcite.jdbc.JavaTypeFactoryImpl.SyntheticRecordType r => SyntheticRecordEmitter.ClassDecl(r),
                java.lang.reflect.ParameterizedType p => FromParameterizedType(p),
                java.lang.reflect.GenericArrayType g => Resolve(g.getGenericComponentType()).MakeArrayType(),
                _ => throw new NotSupportedException($"Cannot resolve a CLR type for '{type}' ({type.GetType()}).")
            };
        }

        /// <summary>
        /// Resolves a Java class to its CLR type.
        /// </summary>
        /// <param name="clazz">The class.</param>
        /// <returns>The CLR type; <see cref="object"/> for <c>java.lang.Object</c>.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="clazz"/> is <see langword="null"/>.</exception>
        /// <exception cref="NotSupportedException">No CLR type backs the class.</exception>
        public static Type FromClass(java.lang.Class clazz)
        {
            ArgumentNullException.ThrowIfNull(clazz);

            // IKVM has a java.lang.Object type of its own, but every signature it compiles uses System.Object,
            // which is what a tree naming Object means
            if (clazz == ObjectClass)
                return typeof(object);

            return ikvm.runtime.Util.getInstanceTypeFromClass(clazz)
                ?? throw new NotSupportedException($"No CLR type backs the Java class '{clazz.getName()}'.");
        }

        /// <summary>
        /// <c>java.lang.Object</c>, which <see cref="FromClass"/> maps to <see cref="object"/>.
        /// </summary>
        static readonly java.lang.Class ObjectClass = (java.lang.Class)typeof(java.lang.Object);

        /// <summary>
        /// Resolves a parameterized Java type to a closed CLR generic type, or to the raw type where that is
        /// not generic.
        /// </summary>
        /// <param name="type">The parameterized Java type.</param>
        /// <returns>The raw type resolved, closed over the resolved type arguments if it is a generic definition.</returns>
        static Type FromParameterizedType(java.lang.reflect.ParameterizedType type)
        {
            var raw = Resolve(type.getRawType());

            // IKVM compiles Java's erased types, so Enumerable<Employee> is the non-generic Enumerable and the
            // type arguments linq4j carries are dropped
            if (raw.IsGenericTypeDefinition == false)
                return raw;

            var args = type.getActualTypeArguments();
            var resolved = new Type[args.Length];
            for (int i = 0; i < args.Length; i++)
                resolved[i] = Resolve(args[i]);

            return raw.MakeGenericType(resolved);
        }

        const BindingFlags All = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Static | BindingFlags.Instance;

        /// <summary>
        /// Resolves a Java method to the CLR method of the same name and parameter types.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <returns>The CLR method.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="method"/> is <see langword="null"/>.</exception>
        /// <exception cref="NotSupportedException">No CLR method has that name and those parameter
        /// types.</exception>
        public static MethodInfo Resolve(java.lang.reflect.Method method)
        {
            return TryResolve(method)
                ?? throw new NotSupportedException($"No CLR method matches '{method}'.");
        }

        /// <summary>
        /// Resolves a Java method to the CLR method of the same name and parameter types, or returns
        /// <see langword="null"/>.
        /// </summary>
        /// <param name="method">The method.</param>
        /// <returns>The CLR method, or <see langword="null"/> if there is no exact match.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="method"/> is <see langword="null"/>.</exception>
        /// <remarks>
        /// <see langword="null"/> does not mean the member cannot be called. IKVM compiles some Java methods
        /// under another name or onto a static <c>Helper</c> class, and a ghost interface declares none; a
        /// caller can then call the member through a delegate from <c>ikvm.runtime.Util.getDelegateFromMethod</c>
        /// instead of emitting a direct call.
        /// </remarks>
        public static MethodInfo? TryResolve(java.lang.reflect.Method method)
        {
            ArgumentNullException.ThrowIfNull(method);

            var declaring = ClrTypes.FromClass(method.getDeclaringClass());

            var parameterTypes = method.getParameterTypes();
            var parameters = new Type[parameterTypes.Length];
            for (int i = 0; i < parameterTypes.Length; i++)
                parameters[i] = ClrTypes.FromClass(parameterTypes[i]);

            return declaring.GetMethod(method.getName(), All, null, parameters, null);
        }

        /// <summary>
        /// Returns the public static method of the given name on the given type that accepts the given
        /// argument types.
        /// </summary>
        /// <param name="declaring">The type to search.</param>
        /// <param name="name">The method name.</param>
        /// <param name="arguments">The argument types.</param>
        /// <returns>The method whose parameters are exactly <paramref name="arguments"/>, else the first to
        /// which every argument is assignable.</returns>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        /// <exception cref="NotSupportedException">No overload accepts the arguments.</exception>
        /// <remarks>
        /// The counterpart of <c>Types.lookupMethod</c>, used where Calcite names a class and a method, as in
        /// <c>Expressions.call(Utilities.class, "compareNullsFirst", args)</c>. Assignable means what
        /// <c>Types.assignableFrom</c> means: a subclass, or a widening between two primitives. Unlike in
        /// <see cref="Rebind"/>, a box and its primitive do not match each other, so that a
        /// <c>java.lang.Integer</c> binds <c>hash(int, Object)</c> as it does in Calcite.
        /// </remarks>
        public static MethodInfo Resolve(Type declaring, string name, Type[] arguments)
        {
            ArgumentNullException.ThrowIfNull(declaring);
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(arguments);

            var exact = declaring.GetMethod(name, BindingFlags.Public | BindingFlags.Static, null, arguments, null);
            if (exact != null)
                return exact;

            foreach (var candidate in declaring.GetMethods(BindingFlags.Public | BindingFlags.Static))
                if (candidate.Name == name && AllAssignable(candidate, arguments))
                    return candidate;

            throw new NotSupportedException($"No overload of {name} on {declaring} accepts the given arguments.");
        }

        /// <summary>
        /// Returns the overload of <paramref name="method"/> that accepts the given argument types, which is
        /// not always the method a linq4j call records.
        /// </summary>
        /// <param name="method">The method the call records.</param>
        /// <param name="arguments">The argument types.</param>
        /// <returns><paramref name="method"/> if it accepts the arguments or an argument is statically
        /// <see cref="object"/>; otherwise the most specific overload of the same name that accepts them, or
        /// <paramref name="method"/> if none does.</returns>
        /// <remarks>
        /// Janino writes a call out as <c>target.name(args)</c> and the Java compiler chooses the overload, so
        /// the recorded method is only the one the tree's builder named. <c>EnumerableWindow</c>, for example,
        /// records <c>BINARY_SEARCH5_UPPER</c>, which takes five parameters, and passes six arguments. A box
        /// and its primitive count as matching, because the caller converts each argument to its parameter.
        /// </remarks>
        public static MethodInfo Rebind(MethodInfo method, Type[] arguments)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(arguments);

            if (Accepts(method, arguments))
                return method;

            // an argument that is statically object fits every overload, so there is nothing to choose on
            foreach (var argument in arguments)
                if (argument == typeof(object))
                    return method;

            MethodInfo? best = null;

            foreach (var candidate in method.DeclaringType!.GetMethods(All))
            {
                if (candidate.Name != method.Name || candidate.IsStatic != method.IsStatic)
                    continue;
                if (Accepts(candidate, arguments) == false)
                    continue;

                // the most specific of those that fit, judged by the first parameter
                if (best == null || best.GetParameters()[0].ParameterType.IsAssignableFrom(candidate.GetParameters()[0].ParameterType))
                    best = candidate;
            }

            return best ?? method;
        }

        /// <summary>
        /// Returns the method of the receiver's own type that a call to <paramref name="method"/> binds to
        /// in Java.
        /// </summary>
        /// <param name="method">The method the call records.</param>
        /// <param name="receiver">The receiver's type.</param>
        /// <param name="arguments">The argument types.</param>
        /// <returns><paramref name="method"/> if it is static or declared on a type the receiver is assignable
        /// to; otherwise the receiver's instance method of that name that accepts the arguments, or
        /// <paramref name="method"/> if there is none.</returns>
        /// <remarks>
        /// The receiver-side counterpart of <see cref="Rebind"/>. Calcite writes <c>multiMap.size()</c> against
        /// <c>BuiltInMethod.COLLECTION_SIZE</c>, but a <c>SortedMultiMap</c> is a <c>Map</c> and not a
        /// <c>Collection</c>; Java binds <c>Map.size()</c> from the receiver's type.
        /// </remarks>
        public static MethodInfo RebindReceiver(MethodInfo method, Type receiver, Type[] arguments)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(receiver);

            if (method.IsStatic || method.DeclaringType!.IsAssignableFrom(receiver))
                return method;

            foreach (var candidate in receiver.GetMethods(All))
                if (candidate.Name == method.Name && candidate.IsStatic == false && Accepts(candidate, arguments))
                    return candidate;

            return method;
        }

        /// <summary>
        /// Returns whether every argument is assignable to the parameter it would be passed as.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>Types.allAssignable</c> over <c>Types.assignableFrom</c>: a subclass, or a widening
        /// between two primitives. Calcite's varargs case does not arise, since IKVM compiles a Java varargs
        /// method as one taking the array.
        /// </remarks>
        /// <param name="method">The candidate method.</param>
        /// <param name="arguments">The argument types, in order.</param>
        /// <returns><see langword="true"/> if the counts match and each argument is assignable to its parameter.</returns>
        static bool AllAssignable(MethodInfo method, Type[] arguments)
        {
            var parameters = method.GetParameters();
            if (parameters.Length != arguments.Length)
                return false;

            for (int i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i].ParameterType;
                if (parameter.IsAssignableFrom(arguments[i]))
                    continue;

                var to = ClrPrimitive.Of(parameter);
                var from = ClrPrimitive.Of(arguments[i]);
                if (to != null && from != null && to.assignableFrom(from))
                    continue;

                return false;
            }

            return true;
        }

        /// <summary>
        /// Returns whether every argument fits the parameter it would be passed as.
        /// </summary>
        /// <remarks>
        /// An argument fits if it is assignable to the parameter or is the parameter's box or primitive, since
        /// the caller converts each argument to its parameter. So <c>SqlFunctions.greater(int, int)</c>
        /// accepts an <c>int</c> and a <c>java.lang.Integer</c>, and <see cref="Rebind"/> keeps it.
        /// </remarks>
        /// <param name="method">The candidate method.</param>
        /// <param name="arguments">The argument types, in order.</param>
        /// <returns><see langword="true"/> if the counts match and each argument fits its parameter.</returns>
        static bool Accepts(MethodInfo method, Type[] arguments)
        {
            var parameters = method.GetParameters();
            if (parameters.Length != arguments.Length)
                return false;

            for (int i = 0; i < parameters.Length; i++)
            {
                var parameter = parameters[i].ParameterType;
                if (parameter.IsAssignableFrom(arguments[i]))
                    continue;
                if (ClrPrimitive.Box(parameter) == ClrPrimitive.Box(arguments[i]))
                    continue;

                return false;
            }

            return true;
        }

        /// <summary>
        /// Returns an expression reading <paramref name="field"/> of <paramref name="target"/>.
        /// </summary>
        /// <param name="target">The object to read from, or <see langword="null"/> for a static field.</param>
        /// <param name="field">The linq4j field.</param>
        /// <returns>A field, property or array length expression.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="field"/> is <see langword="null"/>.</exception>
        /// <exception cref="NotSupportedException">The declaring type has no field or property of that name,
        /// or an array length has no target.</exception>
        /// <remarks>
        /// Where there is no CLR field of the name, a property of the name is read. IKVM compiles a Java
        /// <c>static final</c> field as a property, and exposes a .NET property to Java as a field.
        /// </remarks>
        public static Expression Resolve(Expression? target, J.PseudoField field)
        {
            ArgumentNullException.ThrowIfNull(field);

            // an array's length is a field in Java and a property in the CLR
            if (field is J.ArrayLengthRecordField)
                return Expression.ArrayLength(target ?? throw new NotSupportedException("An array length needs an array."));

            var declaring = ClrTypes.Resolve(field.getDeclaringClass());
            var name = field.getName();

            var info = declaring.GetField(name, All);
            if (info != null)
                return Expression.Field(info.IsStatic ? null : target, info);

            // a Java static final field compiled by IKVM, or a .NET property seen from Java as a field
            var property = declaring.GetProperty(name, All);
            if (property != null)
                return Expression.Property(property.GetMethod?.IsStatic == true ? null : target, property);

            throw new NotSupportedException($"'{declaring}' has no field or property '{name}'.");
        }

    }

}
