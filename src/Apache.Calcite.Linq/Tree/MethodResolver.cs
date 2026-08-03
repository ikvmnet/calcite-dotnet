using System;
using System.Reflection;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Resolves the <see cref="java.lang.reflect.Method"/> that a linq4j call carries to the
    /// <see cref="MethodInfo"/> the translated <see cref="System.Linq.Expressions.Expression"/> calls.
    /// </summary>
    /// <remarks>
    /// The other direction of this is already established: <c>AdoToEnumerableConverter</c> reaches a .NET
    /// method from a linq4j tree with <c>((Class)typeof(X)).getDeclaredMethod(...)</c>. Both directions work
    /// because IKVM compiles a Java class to a CLR type whose methods keep their Java names and parameter
    /// types, so a method is found by the name and signature it already has.
    ///
    /// <para>The exception is a class IKVM remaps onto a CLR type that already exists, such as
    /// <c>java.lang.String</c> onto <see cref="string"/>. See <see cref="FromRemappedClass"/>.</para>
    /// </remarks>
    public static class MethodResolver
    {

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

            var declaring = TypeResolver.FromClass(method.getDeclaringClass());
            var name = method.getName();

            var parameterTypes = method.getParameterTypes();
            var parameters = new Type[parameterTypes.Length];
            for (int i = 0; i < parameterTypes.Length; i++)
                parameters[i] = TypeResolver.FromClass(parameterTypes[i]);

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

    }

}
