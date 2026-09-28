using System;

using IKVM.Runtime;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Creates a delegate that calls a Java method or constructor, as resolved by IKVM rather than looked up
    /// by name.
    /// </summary>
    /// <remarks>
    /// The CLR method IKVM compiles for a Java member cannot always be found from the Java name and erased
    /// signature: a remapped class keeps its Java methods on a static <c>Helper</c> class, a ghost interface
    /// declares none, and a name can differ in case. <c>ikvm.runtime.Util.getDelegateFromMethod</c>, which
    /// requires IKVM 8.16.0 or later, binds to the member IKVM itself would call. It takes the delegate type
    /// as given, so this class builds the canonical <c>MH</c>/<c>MHV</c> type for the signature with
    /// <see cref="CreateDelegateType"/>, a port of IKVM's internal <c>MethodHandleUtil.CreateDelegateType</c>.
    /// </remarks>
    static class JavaDelegates
    {

        /// <summary>
        /// Number of parameters a canonical delegate carries before the rest are packed into containers.
        /// </summary>
        const int MaxArity = 8;

        static readonly Type MHA = typeof(MHA<,,,,,,,>);

        static readonly Type[] MHVTypes = [
            typeof(MHV),
            typeof(MHV<>),
            typeof(MHV<,>),
            typeof(MHV<,,>),
            typeof(MHV<,,,>),
            typeof(MHV<,,,,>),
            typeof(MHV<,,,,,>),
            typeof(MHV<,,,,,,>),
            typeof(MHV<,,,,,,,>)];

        static readonly Type?[] MHTypes = [
            null,
            typeof(MH<>),
            typeof(MH<,>),
            typeof(MH<,,>),
            typeof(MH<,,,>),
            typeof(MH<,,,,>),
            typeof(MH<,,,,,>),
            typeof(MH<,,,,,,>),
            typeof(MH<,,,,,,,>),
            typeof(MH<,,,,,,,,>)];

        /// <summary>
        /// Returns a delegate that calls the given method or constructor, taking the receiver first where it
        /// has one.
        /// </summary>
        /// <param name="executable">The method or constructor.</param>
        /// <returns>A delegate of the canonical IKVM type for the member's signature.</returns>
        /// <remarks>
        /// Every reference-typed parameter and return is typed <see cref="object"/>, and every primitive keeps
        /// its own type, so that a primitive is never passed as a CLR-boxed value where Java expects its own
        /// box. A constructor returns <see cref="object"/>. A member that is not public must be made
        /// accessible with <c>setAccessible(true)</c> first.
        /// </remarks>
        public static Delegate FromMethod(java.lang.reflect.Executable executable)
        {
            ArgumentNullException.ThrowIfNull(executable);

            var parameterClasses = executable.getParameterTypes();
            var receiver = executable is java.lang.reflect.Method && (executable.getModifiers() & java.lang.reflect.Modifier.STATIC) == 0 ? 1 : 0;

            var types = new Type[parameterClasses.Length + receiver];
            if (receiver == 1)
                types[0] = typeof(object);
            for (int i = 0; i < parameterClasses.Length; i++)
                types[i + receiver] = Erase(parameterClasses[i]);

            var returnType = executable is java.lang.reflect.Method method ? Erase(method.getReturnType()) : typeof(object);

            return ikvm.runtime.Util.getDelegateFromMethod(CreateDelegateType(types, returnType), executable);
        }

        /// <summary>
        /// Returns a delegate that reads the given field, taking the receiver where it has one.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        /// <remarks>
        /// The signature is chosen as <see cref="FromMethod(java.lang.reflect.Executable)"/> chooses it. The
        /// handle is <c>unreflectGetter</c>'s, so access is checked as it checks it, and a field that is not
        /// public is the caller's to mark accessible.
        /// </remarks>
        public static Delegate FromGetter(java.lang.reflect.Field field)
        {
            ArgumentNullException.ThrowIfNull(field);

            var types = (field.getModifiers() & java.lang.reflect.Modifier.STATIC) == 0 ? new[] { typeof(object) } : [];
            var handle = java.lang.invoke.MethodHandles.publicLookup().unreflectGetter(field);

            return ikvm.runtime.Util.getDelegateFromMethodHandle(CreateDelegateType(types, Erase(field.getType())), handle);
        }

        /// <summary>
        /// Returns the CLR type a parameter or return of the given class has in the delegate: the primitive
        /// itself, or <see cref="object"/>.
        /// </summary>
        /// <param name="clazz">The Java class of the parameter or return.</param>
        /// <returns>The CLR primitive type for a primitive class, otherwise <see cref="object"/>.</returns>
        static Type Erase(java.lang.Class clazz)
        {
            return clazz.isPrimitive() ? Linq4j.Tree.ClrTypes.FromClass(clazz) : typeof(object);
        }

        /// <summary>
        /// Returns the canonical delegate type IKVM builds for a signature.
        /// </summary>
        /// <remarks>
        /// Mirrors IKVM's <c>MethodHandleUtil.CreateDelegateType</c>. Past eight parameters the tail is packed
        /// into nested <see cref="MHA"/> containers, seven at a time.
        /// </remarks>
        /// <param name="types">The erased parameter types, in order.</param>
        /// <param name="returnType">The erased return type, or <see cref="void"/>.</param>
        /// <returns>The delegate type IKVM uses for that signature.</returns>
        static Type CreateDelegateType(Type[] types, Type returnType)
        {
            if (types.Length == 0 && returnType == typeof(void))
                return MHVTypes[0];

            if (types.Length > MaxArity)
            {
                var arity = types.Length;
                var remainder = (arity - 8) % 7;
                var count = (arity - 8) / 7;
                if (remainder == 0)
                {
                    remainder = 7;
                    count--;
                }

                var last = MHA.MakeGenericType(SubArray(types, types.Length - 8, 8));
                for (int i = 0; i < count; i++)
                {
                    var temp = SubArray(types, types.Length - 8 - 7 * (i + 1), 8);
                    temp[7] = last;
                    last = MHA.MakeGenericType(temp);
                }

                types = SubArray(types, 0, remainder + 1);
                types[remainder] = last;
            }

            if (returnType == typeof(void))
                return MHVTypes[types.Length].MakeGenericType(types);

            types = [.. types, returnType];
            return MHTypes[types.Length]!.MakeGenericType(types);
        }

        /// <summary>
        /// Returns a range of an array.
        /// </summary>
        /// <param name="array">The source array.</param>
        /// <param name="start">The index of the first element to copy.</param>
        /// <param name="length">The number of elements to copy.</param>
        /// <returns>A new array of the <paramref name="length"/> elements starting at <paramref name="start"/>.</returns>
        static Type[] SubArray(Type[] array, int start, int length)
        {
            var result = new Type[length];
            Array.Copy(array, start, result, 0, length);
            return result;
        }

    }

}
