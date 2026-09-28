using System;

using IKVM.Runtime;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Creates a delegate that calls a Java method, resolved by IKVM rather than searched for by name.
    /// </summary>
    /// <remarks>
    /// A <c>java.lang.reflect.Method</c> is a Java answer, and the CLR method IKVM compiled for it is not
    /// always reachable from the name and the erased signature Java reports: a remapped class keeps its Java
    /// methods on a static <c>Helper</c> class, a ghost interface declares nothing at all, and a name can
    /// differ in case. <see cref="Linq4j.Tree.ClrTypes"/> reconstructs those, and a reconstruction is a guess.
    /// A method handle is not: <c>unreflect</c> is IKVM's own resolution of the same member, and a delegate
    /// over the handle calls whatever IKVM would have called.
    ///
    /// <para>The delegate itself is <c>ikvm.runtime.Util.getDelegateFromMethod</c>, which IKVM has made
    /// public since 8.16.0. What is left here is choosing the delegate type, which that method takes as
    /// given: the canonical <c>MH</c>/<c>MHV</c> type for the method's own signature, built by
    /// <see cref="CreateDelegateType"/> — <c>MethodHandleUtil.CreateDelegateType</c> ported, because IKVM
    /// keeps that one internal.</para>
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
        /// <param name="executable"></param>
        /// <returns></returns>
        /// <remarks>
        /// The signature is the method's own, with every reference type left as <see cref="object"/> and every
        /// primitive kept as itself. Keeping the primitives is the point: a primitive passed as an object is a
        /// <c>java.lang.Integer</c> rather than a boxed CLR int, and the two are not the same value. Leaving
        /// the references as objects costs nothing — a reference conversion either way — and keeps the
        /// signature to types that are certainly the ones IKVM signs with, which a ghost interface is not.
        ///
        /// <para>Access is checked as <c>Lookup.unreflect</c> checks it. Nothing a linq4j tree names can be out
        /// of its reach — Janino compiles that tree as Java source in an anonymous package, so every member it
        /// reaches is public on a public class — and a member that is not is the caller's to mark
        /// accessible.</para>
        ///
        /// <para>The type asked for is the canonical one, but IKVM binds a second delegate of it over its own
        /// rather than handing its own back, so every call through this is one delegate call more than it
        /// has to be.</para>
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
        /// Returns the type a value of the given class crosses a delegate boundary as.
        /// </summary>
        /// <param name="clazz"></param>
        /// <returns></returns>
        static Type Erase(java.lang.Class clazz)
        {
            return clazz.isPrimitive() ? Linq4j.Tree.ClrTypes.FromClass(clazz) : typeof(object);
        }

        /// <summary>
        /// Returns the canonical delegate type IKVM builds for a signature.
        /// </summary>
        /// <param name="types"></param>
        /// <param name="returnType"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>MethodHandleUtil.CreateDelegateType</c>. Past eight parameters the tail is packed into nested
        /// <see cref="MHA"/> containers, seven at a time.
        /// </remarks>
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
        /// <param name="array"></param>
        /// <param name="start"></param>
        /// <param name="length"></param>
        /// <returns></returns>
        static Type[] SubArray(Type[] array, int start, int length)
        {
            var result = new Type[length];
            Array.Copy(array, start, result, 0, length);
            return result;
        }

    }

}
