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
    /// <para><c>Types.toClass</c> can answer with the class it was handed, a Java type already being a
    /// Java runtime type. Nothing here can: every answer crosses from what Calcite described to what IKVM
    /// compiled, and a linq4j call's recorded method is advisory besides — Janino resolves the overload
    /// from the source it writes, so an overload named against one signature and passed another has to be
    /// resolved again here.</para>
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
        /// Assembly IKVM compiles the Java class library into, and so where a remapped class keeps the methods
        /// its CLR counterpart does not have.
        /// </summary>
        static readonly Assembly JavaAssembly = typeof(java.lang.Class).Assembly;

        /// <summary>
        /// Resolves a Java method to its CLR method.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static MethodInfo Resolve(java.lang.reflect.Method method)
        {
            ArgumentNullException.ThrowIfNull(method);

            var declaring = ClrTypes.FromClass(method.getDeclaringClass());
            var name = method.getName();

            var parameterTypes = method.getParameterTypes();
            var parameters = new Type[parameterTypes.Length];
            for (int i = 0; i < parameterTypes.Length; i++)
                parameters[i] = ClrTypes.FromClass(parameterTypes[i]);

            return declaring.GetMethod(name, All, null, parameters, null)
                ?? Search(declaring, name, parameters, StringComparison.Ordinal)
                ?? FromRemappedClass(method.getDeclaringClass(), declaring, name, parameters)
                // a remapped class keeps its methods but not their Java names: java.lang.Comparable is
                // System.IComparable, whose method is CompareTo. Last, because a name that differs by more
                // than case is a resolution to fail on rather than to guess at
                ?? Search(declaring, name, parameters, StringComparison.OrdinalIgnoreCase)
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
        /// The same resolution as <see cref="Rebind"/>, where there is no method to start from. Calcite names
        /// a class and a method and nothing more — <c>Expressions.call(Utilities.class, "compareNullsFirst",
        /// args)</c> — and lets javac choose the overload from the argument expressions of the source it emits.
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
        /// Returns the overload that accepts the given arguments, which is not always the one a linq4j call
        /// names.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <remarks>
        /// A linq4j tree records a Method, but Janino never uses it: the tree is written out as source and the
        /// Java compiler resolves the overload from the argument expressions. So the recorded method is only
        /// what the code that built the tree happened to name. Calcite writes Linq4j.asEnumerable(list) against
        /// BuiltInMethod.AS_ENUMERABLE, whose parameter is an array, and Java binds the List overload.
        /// </remarks>
        public static MethodInfo Rebind(MethodInfo method, Type[] arguments)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(arguments);

            if (Accepts(method, arguments))
                return method;

            // an argument that is statically an object fits every overload, so there is nothing to choose on
            // and the method the tree names is the only information there is
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

                // the most specific of those that fit, which is what Java would choose
                if (best == null || best.GetParameters()[0].ParameterType.IsAssignableFrom(candidate.GetParameters()[0].ParameterType))
                    best = candidate;
            }

            return best ?? method;
        }

        /// <summary>
        /// Returns the method of the receiver's own type, which is not always the one a linq4j call names.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="receiver"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <remarks>
        /// The same reason as <see cref="Rebind"/>, in the other position. Calcite writes multiMap.size()
        /// against BuiltInMethod.COLLECTION_SIZE, and a SortedMultiMap is a Map rather than a Collection; Java
        /// binds Map.size() from the receiver in the source text and never looks at the named method.
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
        /// Returns whether every argument fits the parameter it would be passed as.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        static bool Accepts(MethodInfo method, Type[] arguments)
        {
            var parameters = method.GetParameters();
            if (parameters.Length != arguments.Length)
                return false;

            for (int i = 0; i < parameters.Length; i++)
                if (parameters[i].ParameterType.IsAssignableFrom(arguments[i]) == false)
                    return false;

            return true;
        }

        /// <summary>
        /// Finds a method that a remapped Java class keeps outside the CLR type it was remapped onto.
        /// </summary>
        /// <param name="clazz"></param>
        /// <param name="declaring"></param>
        /// <param name="name"></param>
        /// <param name="parameters"></param>
        /// <returns></returns>
        /// <remarks>
        /// IKVM remaps a handful of Java classes onto the CLR type that already means the same thing, so
        /// <c>java.lang.String</c> is <see cref="string"/>. The methods only Java has cannot go there, and
        /// land on a static <c>Helper</c> class taking the receiver as the first parameter:
        /// <c>String.toUpperCase()</c> is <c>java.lang.StringHelper.toUpperCase(string)</c>.
        ///
        /// <para>The returned method is therefore static where the Java one was not, and a caller composing a
        /// call must pass the receiver as the first argument rather than as the call's target.</para>
        /// </remarks>
        static MethodInfo? FromRemappedClass(java.lang.Class clazz, Type declaring, string name, Type[] parameters)
        {
            var helper = JavaAssembly.GetType(clazz.getName() + "Helper");
            if (helper == null)
                return null;

            var withReceiver = new Type[parameters.Length + 1];
            withReceiver[0] = declaring;
            parameters.CopyTo(withReceiver, 1);

            return helper.GetMethod(name, All, null, withReceiver, null)
                ?? helper.GetMethod(name, All, null, parameters, null)
                ?? Search(helper, name, withReceiver, StringComparison.Ordinal)
                ?? Search(helper, name, parameters, StringComparison.Ordinal);
        }

        /// <summary>
        /// Finds a method by name and parameter types when an exact binder match fails.
        /// </summary>
        /// <param name="declaring"></param>
        /// <param name="name"></param>
        /// <param name="parameters"></param>
        /// <param name="comparison"></param>
        /// <returns></returns>
        /// <remarks>
        /// A method the binder will not match on exact types is still reachable by walking the candidates:
        /// an interface method is declared on a base interface rather than the one asked about, and a
        /// method IKVM generates for a Java class can differ from the erased signature Java reports.
        /// </remarks>
        static MethodInfo? Search(Type declaring, string name, Type[] parameters, StringComparison comparison)
        {
            var found = SearchDeclared(declaring, name, parameters, comparison);
            if (found != null)
                return found;

            // an interface does not report the members it inherits, and IKVM leaves a Java interface empty
            // when a CLR interface already carries its method: java.lang.Comparable extends System.IComparable
            // and declares nothing itself
            if (declaring.IsInterface)
                foreach (var inherited in declaring.GetInterfaces())
                    if ((found = SearchDeclared(inherited, name, parameters, comparison)) != null)
                        return found;

            return null;
        }

        /// <summary>
        /// Finds a method declared on one type by name and parameter types.
        /// </summary>
        /// <param name="declaring"></param>
        /// <param name="name"></param>
        /// <param name="parameters"></param>
        /// <param name="comparison"></param>
        /// <returns></returns>
        static MethodInfo? SearchDeclared(Type declaring, string name, Type[] parameters, StringComparison comparison)
        {
            MethodInfo? found = null;

            foreach (var candidate in declaring.GetMethods(All))
            {
                if (string.Equals(candidate.Name, name, comparison) == false)
                    continue;

                var candidateParameters = candidate.GetParameters();
                if (candidateParameters.Length != parameters.Length)
                    continue;

                var match = true;
                for (int i = 0; i < parameters.Length; i++)
                {
                    if (candidateParameters[i].ParameterType.IsAssignableFrom(parameters[i]) == false)
                    {
                        match = false;
                        break;
                    }
                }

                if (match == false)
                    continue;

                // an ambiguous name and arity is a resolution this cannot make, and silently taking the first
                // would put the wrong method in the tree
                if (found != null)
                    throw new NotSupportedException($"'{name}' on '{declaring}' is ambiguous for the given parameter types.");

                found = candidate;
            }

            return found;
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
