using System;
using System.Reflection;

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
    /// <para>This is <c>ikvm.runtime.Util.getDelegateFromMethodHandle</c>, written against what IKVM 8.15.0
    /// makes public. <c>ByteCodeHelper.GetDelegateForInvokeExact&lt;T&gt;</c> is not "give me a delegate of
    /// type T" — it hands back the canonical <c>MH</c>/<c>MHV</c> delegate IKVM built for the
    /// handle's own method type and throws <c>WrongMethodTypeException</c> if T is not that type, and the two
    /// members that would say which type that is are internal. So the canonical type is rebuilt here by
    /// <see cref="CreateDelegateType"/>, which is <c>MethodHandleUtil.CreateDelegateType</c> ported, and the
    /// method type the requested delegate stands for is asked of <c>ByteCodeHelper.LoadMethodType</c> rather
    /// than derived. A rebuild that is wrong fails loudly in <c>GetDelegateForInvokeExact</c> rather than
    /// binding something else.</para>
    ///
    /// <para>When IKVM ships the method this class goes away, and each caller becomes one call to it.</para>
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

        static readonly MethodInfo LoadMethodType = typeof(ByteCodeHelper).GetMethod(nameof(ByteCodeHelper.LoadMethodType))
            ?? throw new InvalidOperationException($"'{nameof(ByteCodeHelper.LoadMethodType)}' is missing from {nameof(ByteCodeHelper)}.");

        static readonly MethodInfo GetDelegateForInvokeExact = typeof(ByteCodeHelper).GetMethod(nameof(ByteCodeHelper.GetDelegateForInvokeExact))
            ?? throw new InvalidOperationException($"'{nameof(ByteCodeHelper.GetDelegateForInvokeExact)}' is missing from {nameof(ByteCodeHelper)}.");

        /// <summary>
        /// Returns a delegate of the given type that calls the given method or constructor.
        /// </summary>
        /// <param name="delegateType"></param>
        /// <param name="executable"></param>
        /// <returns></returns>
        /// <remarks>
        /// Access is checked as <c>Lookup.unreflect</c> checks it, against the public lookup. Nothing a linq4j
        /// tree names can be out of its reach — Janino compiles that tree as Java source in an anonymous
        /// package, so every member it reaches is public on a public class — and a member that is not is the
        /// caller's to mark accessible, as it is for the IKVM method this stands in for.
        /// </remarks>
        public static Delegate FromMethod(Type delegateType, java.lang.reflect.Executable executable)
        {
            ArgumentNullException.ThrowIfNull(delegateType);
            ArgumentNullException.ThrowIfNull(executable);

            var lookup = java.lang.invoke.MethodHandles.publicLookup();
            var handle = executable is java.lang.reflect.Constructor constructor
                ? lookup.unreflectConstructor(constructor)
                : lookup.unreflect((java.lang.reflect.Method)executable);

            return FromMethodHandle(delegateType, handle);
        }

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
        /// <para>The type built here is the canonical one, so the delegate returned is IKVM's own rather than
        /// a second delegate wrapping it.</para>
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

            return FromMethod(CreateDelegateType(types, returnType), executable);
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
        /// Returns a delegate of the given type that invokes the given method handle.
        /// </summary>
        /// <param name="delegateType"></param>
        /// <param name="methodHandle"></param>
        /// <returns></returns>
        /// <remarks>
        /// The handle is adapted to the delegate's own signature, which is what performs the conversions a
        /// call needs — boxing, primitive widening, receiver binding — and what rejects a handle that cannot
        /// be called through this delegate.
        /// </remarks>
        public static Delegate FromMethodHandle(Type delegateType, java.lang.invoke.MethodHandle methodHandle)
        {
            ArgumentNullException.ThrowIfNull(delegateType);
            ArgumentNullException.ThrowIfNull(methodHandle);

            var invoke = Invoke(delegateType);

            foreach (var parameter in invoke.GetParameters())
                if (parameter.ParameterType.IsByRef || parameter.ParameterType.IsPointer)
                    throw new ArgumentException($"'{delegateType}' has a by-ref or pointer parameter.", nameof(delegateType));

            // what the delegate's signature is as a method type is IKVM's answer rather than one derived here,
            // and adapting the handle to it is what makes the delegate callable
            var methodType = (java.lang.invoke.MethodType)LoadMethodType.MakeGenericMethod(delegateType).Invoke(null, null)!;
            methodHandle = methodHandle.asType(methodType).asFixedArity();

            // the adapted handle materializes as its canonical delegate, which by construction has exactly the
            // signature asked for; where that is the type asked for there is nothing further to do
            var canonical = CreateDelegateType(Array.ConvertAll(invoke.GetParameters(), p => p.ParameterType), invoke.ReturnType);
            var inner = (Delegate)GetDelegateForInvokeExact.MakeGenericMethod(canonical).Invoke(null, [methodHandle])!;
            if (canonical == delegateType)
                return inner;

            return Delegate.CreateDelegate(delegateType, inner, Invoke(canonical), false)
                ?? throw new ArgumentException($"Cannot create a '{delegateType}' for a method handle of type {methodType}.", nameof(delegateType));
        }

        /// <summary>
        /// Returns the <c>Invoke</c> of a delegate type.
        /// </summary>
        /// <param name="delegateType"></param>
        /// <returns></returns>
        static MethodInfo Invoke(Type delegateType)
        {
            return (delegateType.BaseType == typeof(MulticastDelegate) ? delegateType.GetMethod("Invoke") : null)
                ?? throw new ArgumentException($"'{delegateType}' is not a delegate type.", nameof(delegateType));
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
