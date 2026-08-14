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

                // the opposite direction to the one above: a synthetic record names a type that has to be
                // emitted, and this names one that already exists and only had to be described by ordinal
                // because Java cannot see a .NET property
                Adapter.Clr.ClrRecordType r => r.ClrType,
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
        /// Resolves a Java method to its CLR method.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        public static MethodInfo Resolve(java.lang.reflect.Method method)
        {
            return TryResolve(method)
                ?? throw new NotSupportedException($"No CLR method matches '{method}'.");
        }

        /// <summary>
        /// Resolves a Java method to the CLR method of that name and signature, or answers
        /// <see langword="null"/> where there is none.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        /// <remarks>
        /// One question, asked once: is there a method of this name whose parameters are exactly these. Where
        /// there is, it is the method IKVM compiled and nothing further needs deciding; where there is not,
        /// there is no second question worth asking here. A name that differs, a receiver that moved to a
        /// static <c>Helper</c>, a ghost interface that declares nothing — those were four more searches, and
        /// each was a reconstruction of what IKVM did rather than an answer from it. Across all 597 of
        /// Calcite's <c>BuiltInMethod</c>s they resolved three: <c>String.toUpperCase</c>,
        /// <c>Object.toString</c> and <c>Comparable.compareTo</c>.
        ///
        /// <para>Answering <see langword="null"/> is therefore not a claim that no such method exists. It says
        /// the CLR type system cannot be asked, and the question goes to IKVM instead — <c>JavaDelegates</c>
        /// unreflects the member into a method handle, which is IKVM's own resolution of it and cannot be
        /// wrong. A caller that can invoke a delegate rather than emit a call should do that.</para>
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
        /// A linq4j tree records a Method, but Janino never uses it: <c>MethodCallExpression</c> writes itself
        /// out as <c>target.name(args)</c> and the Java compiler resolves the overload from the argument
        /// expressions. So the recorded method is only what the code that built the tree happened to name.
        ///
        /// <para>It is very nearly always the method to call, and measuring says how nearly: across the whole
        /// test suite one call in the plans this convention builds names a method that is not the one, and it
        /// is Calcite's own — <c>EnumerableWindow</c> names <c>BINARY_SEARCH5_UPPER</c>, whose method takes
        /// five parameters, and passes it six arguments. Janino writes the name and javac binds the six-parameter
        /// overload. Nothing derived from the recorded method can find that, a method handle over it included:
        /// the handle would take five arguments. Choosing needs the candidates of that name, which is what this
        /// walks.</para>
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
        /// <remarks>
        /// Fitting includes the box and the primitive of one another, because the call being composed converts
        /// each argument to its parameter anyway and that conversion is one of the ones it makes. Counting it
        /// as a misfit sends <see cref="Rebind"/> looking for another method where there was nothing wrong
        /// with this one: <c>SqlFunctions.greater(int, int)</c> handed an <c>int</c> and a
        /// <c>java.lang.Integer</c> is the method Calcite named and the method whose return type the tree
        /// carries, and the second argument wants unboxing rather than a different overload.
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

            // a Java `static final` field is not a CLR field of that name. IKVM emits a property over a
            // backing field it renames to __<>NAME, so that reading it still runs the class initializer the
            // way Java guarantees, and a tree naming such a field lands here with nothing to find.
            // `Unit.INSTANCE` -- what Calcite's own CUSTOM.record answers for a row of no fields -- is one;
            // JavaRowFormatExtensions.StaticMember measured that and FlatLists.COMPARABLE_EMPTY_LIST both.
            //
            // it is *not* that a .NET property is visible to Java as a field, which this said for a while.
            // Measured: a class whose members are properties answers nothing at all from getFields(). A
            // property is a get_/set_ method pair to Java and its backing field is private under a mangled
            // name, so nothing reaches here by naming one -- a row whose fields are .NET properties has to
            // name them through a Types.RecordType rather than a Class to be reachable by name.
            var property = declaring.GetProperty(name, All);
            if (property != null)
                return Expression.Property(property.GetMethod?.IsStatic == true ? null : target, property);

            throw new NotSupportedException($"'{declaring}' has no field or property '{name}'.");
        }

    }

}
