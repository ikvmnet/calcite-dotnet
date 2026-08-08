using System;
using System.Linq.Expressions;
using System.Reflection;

using JavaType = java.lang.reflect.Type;
using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Reflection over the types, methods and fields a linq4j tree names, answered for the CLR.
    /// </summary>
    /// <remarks>
    /// What <c>Types</c> is in linq4j, for a tree that will run here rather than be compiled as Java.
    /// Calcite asks it what runtime class a <c>Type</c> stands for, and which method or field a name and a
    /// signature resolve to; the answers are the same questions, and the runtime is the difference.
    ///
    /// <para><c>Types.toClass</c> can answer with the class it was handed, a Java type already being a Java
    /// runtime type. Nothing here can: every answer crosses from what Calcite described to what IKVM
    /// compiled.</para>
    ///
    /// <para>Methods are the exception, and are mostly not here. A linq4j call carries a
    /// <c>java.lang.reflect.Method</c>, which names the member exactly, and IKVM can be asked which CLR
    /// method it compiled for it — so <c>JavaDelegates</c> answers that question and translation uses it.
    /// What is left here is the search a caller needs when it must have a <see cref="MethodInfo"/> to emit a
    /// direct call, or when Calcite named a class and a method and no <c>Method</c> at all.</para>
    /// </remarks>
    public static class ClrTypes
    {

        /// <summary>
        /// Resolves a Java reflection type to its CLR type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static Type Resolve(JavaType type)
        {
            ArgumentNullException.ThrowIfNull(type);

            return type switch
            {
                java.lang.Class c => FromClass(c),
                org.apache.calcite.jdbc.JavaTypeFactoryImpl.SyntheticRecordType r => SyntheticRecordEmitter.Emit(r),
                java.lang.reflect.ParameterizedType p => FromParameterizedType(p),
                java.lang.reflect.GenericArrayType g => Resolve(g.getGenericComponentType()).MakeArrayType(),
                _ => throw new NotSupportedException($"Cannot resolve a CLR type for '{type}' ({type.GetType()}).")
            };
        }

        /// <summary>
        /// Resolves a Java class to its CLR type.
        /// </summary>
        /// <param name="clazz"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static Type FromClass(java.lang.Class clazz)
        {
            ArgumentNullException.ThrowIfNull(clazz);

            // IKVM keeps a java.lang.Object of its own for the class object and for `new Object()`, but every
            // signature it compiles uses System.Object -- java.util.Objects.equals takes two of those, and
            // java.util.List.get returns one. A tree naming Object means the one in the signatures.
            if (clazz == ObjectClass)
                return typeof(object);

            return ikvm.runtime.Util.getInstanceTypeFromClass(clazz)
                ?? throw new NotSupportedException($"No CLR type backs the Java class '{clazz.getName()}'.");
        }

        /// <summary>
        /// <c>java.lang.Object</c>, which does not resolve the way every other class does.
        /// </summary>
        static readonly java.lang.Class ObjectClass = (java.lang.Class)typeof(java.lang.Object);

        /// <summary>
        /// Resolves a parameterized Java type to a closed CLR generic type.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        static Type FromParameterizedType(java.lang.reflect.ParameterizedType type)
        {
            var raw = Resolve(type.getRawType());

            // Java erases its generics and IKVM compiles what is left, so Enumerable<Employee> is Enumerable.
            // linq4j still carries the arguments, and they have nowhere to go.
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
        /// Returns the CLR method of the same name and signature as a Java method.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// This is a search, and it is here for the callers that need a <see cref="MethodInfo"/> in order to
        /// emit a direct call — the handful of Calcite methods this convention names itself, which are read
        /// once into static fields and then called on every row. Translating a linq4j tree does not use it:
        /// <c>JavaDelegates</c> puts the member to IKVM instead, which answers rather than agrees.
        ///
        /// <para>It asks one question — is there a method of this name whose parameters are exactly these —
        /// and refuses when the answer is no. It used to ask four more, each a reconstruction of something
        /// IKVM had done: a name differing in case, a receiver moved to a static <c>Helper</c> class, a walk
        /// of base interfaces matching parameters by assignability. Across all 597 of Calcite's
        /// <c>BuiltInMethod</c>s those four resolved three — <c>String.toUpperCase</c>, <c>Object.toString</c>
        /// and <c>Comparable.compareTo</c> — and each of the three is a method handle's to answer.</para>
        /// </remarks>
        public static MethodInfo Resolve(java.lang.reflect.Method method)
        {
            ArgumentNullException.ThrowIfNull(method);

            var declaring = ClrTypes.FromClass(method.getDeclaringClass());

            var parameterTypes = method.getParameterTypes();
            var parameters = new Type[parameterTypes.Length];
            for (int i = 0; i < parameterTypes.Length; i++)
                parameters[i] = ClrTypes.FromClass(parameterTypes[i]);

            return declaring.GetMethod(method.getName(), All, null, parameters, null)
                ?? throw new NotSupportedException($"No CLR method matches '{method}'.");
        }

        /// <summary>
        /// Returns the method of the given name on the given type that accepts the given arguments.
        /// </summary>
        /// <param name="declaring"></param>
        /// <param name="name"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// Calcite names a class and a method and nothing more — <c>Expressions.call(Utilities.class,
        /// "compareNullsFirst", args)</c> — and lets javac choose the overload from the argument expressions of
        /// the source it emits. There is no <c>java.lang.reflect.Method</c> to unreflect, so choosing is done
        /// here rather than by IKVM, and <c>ClrPhysTypeImpl</c>'s comparator generation is what asks.
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
                if (candidate.Name == name && Accepts(candidate, arguments))
                    return candidate;

            throw new NotSupportedException($"No overload of {name} on {declaring} accepts the given arguments.");
        }

        /// <summary>
        /// Returns whether every argument fits the parameter it would be passed as.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <remarks>
        /// Fitting includes the box and the primitive of one another, because the call being composed converts
        /// each argument to its parameter anyway and that conversion is one of the ones it makes. Counting it
        /// as a misfit would pass over an overload that suits perfectly well for one that only looks like it
        /// does.
        /// </remarks>
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

            var declaring = ClrTypes.Resolve(field.getDeclaringClass());
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
