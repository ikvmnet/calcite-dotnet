using System;
using System.Reflection;

using IKVM.Runtime;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Creates a delegate that calls a Java method, resolved by IKVM rather than searched for by name.
    /// </summary>
    /// <remarks>
    /// A <c>java.lang.reflect.Method</c> already names the member exactly, so the CLR method to call is not
    /// something to search for. <c>unreflect</c> is IKVM's own resolution of that member, and a delegate over
    /// the handle calls whatever IKVM would have called. A search by name and erased signature can only agree
    /// with that or be wrong, and there are members it cannot reach at all: a remapped class keeps its Java
    /// methods on a static <c>Helper</c> class, and a ghost interface declares nothing.
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

        static readonly MethodInfo LoadMethodType = typeof(ByteCodeHelper).GetMethod(nameof(ByteCodeHelper.LoadMethodType))!;

        static readonly MethodInfo GetDelegateForInvokeExact = typeof(ByteCodeHelper).GetMethod(nameof(ByteCodeHelper.GetDelegateForInvokeExact))!;

        /// <summary>
        /// Returns a delegate of the given type that calls the given method or constructor.
        /// </summary>
        /// <param name="delegateType"></param>
        /// <param name="executable"></param>
        /// <returns></returns>
        /// <remarks>
        /// Access is checked as <c>Lookup.unreflect</c> checks it, against the public lookup, and the public
        /// lookup cannot reach a public method of a non-public class. Calcite has those and names them:
        /// <c>BuiltInMethod.QUERYABLE_SELECT</c> is <c>ExtendedQueryable.select</c>, and
        /// <c>ExtendedQueryable</c> is not public. Such a member is retried through a copy of itself marked
        /// accessible — <c>getDeclaredMethod</c> answers with a fresh <c>Method</c> every call, so the one the
        /// caller holds is left as it was.
        /// </remarks>
        public static Delegate FromMethod(Type delegateType, java.lang.reflect.Executable executable)
        {
            ArgumentNullException.ThrowIfNull(delegateType);
            ArgumentNullException.ThrowIfNull(executable);

            return FromMethodHandle(delegateType, Unreflect(executable));
        }

        /// <summary>
        /// Returns a handle on a method or constructor.
        /// </summary>
        /// <param name="executable"></param>
        /// <returns></returns>
        static java.lang.invoke.MethodHandle Unreflect(java.lang.reflect.Executable executable)
        {
            var lookup = java.lang.invoke.MethodHandles.publicLookup();

            try
            {
                return Unreflect(lookup, executable);
            }
            catch (java.lang.IllegalAccessException)
            {
                return Unreflect(lookup, Accessible(executable));
            }
        }

        /// <summary>
        /// Returns a handle on a method or constructor, through the given lookup.
        /// </summary>
        /// <param name="lookup"></param>
        /// <param name="executable"></param>
        /// <returns></returns>
        static java.lang.invoke.MethodHandle Unreflect(java.lang.invoke.MethodHandles.Lookup lookup, java.lang.reflect.Executable executable)
        {
            return executable is java.lang.reflect.Constructor constructor
                ? lookup.unreflectConstructor(constructor)
                : lookup.unreflect((java.lang.reflect.Method)executable);
        }

        /// <summary>
        /// Returns a copy of the given member, marked accessible.
        /// </summary>
        /// <param name="executable"></param>
        /// <returns></returns>
        static java.lang.reflect.Executable Accessible(java.lang.reflect.Executable executable)
        {
            var declaring = executable.getDeclaringClass();
            var parameters = executable.getParameterTypes();

            java.lang.reflect.Executable copy = executable is java.lang.reflect.Constructor
                ? declaring.getDeclaredConstructor(parameters)
                : declaring.getDeclaredMethod(executable.getName(), parameters);

            copy.setAccessible(true);
            return copy;
        }

        /// <summary>
        /// Returns a delegate that calls the given method or constructor, taking the receiver first where it
        /// has one.
        /// </summary>
        /// <param name="executable"></param>
        /// <returns></returns>
        /// <remarks>
        /// The signature is the method's own, each class resolved to the CLR type IKVM compiled it as, and
        /// the receiver first where the method has one. Not erased to objects: a caller converts each argument
        /// to its parameter, and the conversion it makes depends on what that parameter is — a primitive is
        /// boxed the Java way, a list is wrapped, a reference is cast. Against <see cref="object"/> parameters
        /// none of that happens and the values arrive in whatever shape they were already in.
        ///
        /// <para>The type built here is the canonical one, so the delegate returned is IKVM's own rather than
        /// a second delegate wrapping it. A ghost interface in the signature is the one shape that cannot be
        /// built this way, and it fails in <see cref="FromMethodHandle"/> rather than binding something else.</para>
        /// </remarks>
        public static Delegate FromMethod(java.lang.reflect.Executable executable)
        {
            ArgumentNullException.ThrowIfNull(executable);

            return FromExecutable(executable);
        }

        /// <summary>
        /// Returns a delegate that calls the method a linq4j call names, which is the method it records except
        /// where Calcite records one it is not calling.
        /// </summary>
        /// <param name="executable"></param>
        /// <param name="argumentCount">Arguments the call carries, the receiver included.</param>
        /// <returns></returns>
        /// <remarks>
        /// The recorded method is the method. Measured over every plan the tests build, one call in Calcite's
        /// own code is not: <c>EnumerableWindow</c> names <c>BINARY_SEARCH5_UPPER</c>, whose method takes five
        /// parameters, and passes it six arguments. That compiles only because Janino writes
        /// <c>BinarySearch.upperBound(a, b, c, d, e, f)</c> as text and javac binds the six-parameter overload;
        /// <c>MethodCallExpression.evaluate</c>, which calls <c>method.invoke</c>, would throw on the same
        /// expression.
        ///
        /// <para>Nothing derived from the recorded method can find what was meant there — a handle over it
        /// takes five arguments — so the overload is looked for by name and arity, and only when the count
        /// disagrees. An ambiguous answer is refused rather than picked from.</para>
        /// </remarks>
        public static Delegate FromMethod(java.lang.reflect.Executable executable, int argumentCount)
        {
            ArgumentNullException.ThrowIfNull(executable);

            var receiver = IsInstance(executable) ? 1 : 0;
            if (executable.getParameterTypes().Length + receiver == argumentCount)
                return FromExecutable(executable);

            return FromExecutable(ByArity(executable, argumentCount - receiver));
        }

        /// <summary>
        /// Returns the method of the same name taking the given number of parameters.
        /// </summary>
        /// <param name="executable"></param>
        /// <param name="parameterCount"></param>
        /// <returns></returns>
        static java.lang.reflect.Executable ByArity(java.lang.reflect.Executable executable, int parameterCount)
        {
            java.lang.reflect.Method? found = null;

            foreach (var candidate in executable.getDeclaringClass().getMethods())
            {
                if (candidate.getName() != executable.getName() || candidate.getParameterTypes().Length != parameterCount)
                    continue;
                if (found != null)
                    throw new NotSupportedException($"'{executable}' is recorded against {executable.getParameterTypes().Length} parameters and called with {parameterCount}, and more than one overload takes that many.");

                found = candidate;
            }

            return found
                ?? throw new NotSupportedException($"'{executable}' is recorded against {executable.getParameterTypes().Length} parameters and called with {parameterCount}, and no overload takes that many.");
        }

        /// <summary>
        /// Returns whether a member is called on a receiver.
        /// </summary>
        /// <param name="executable"></param>
        /// <returns></returns>
        static bool IsInstance(java.lang.reflect.Executable executable)
        {
            return executable is java.lang.reflect.Method && (executable.getModifiers() & java.lang.reflect.Modifier.STATIC) == 0;
        }

        /// <summary>
        /// Returns a delegate that calls the given method or constructor.
        /// </summary>
        /// <param name="executable"></param>
        /// <returns></returns>
        static Delegate FromExecutable(java.lang.reflect.Executable executable)
        {
            try
            {
                return FromMethod(SignatureOf(executable, false), executable);
            }
            catch (java.lang.invoke.WrongMethodTypeException)
            {
                // a ghost interface's CLR type is not the type IKVM signs with, and there is no public way to
                // ask which type that is. Erasing the references leaves a signature it certainly signs with,
                // and the handle converts into it -- which for a ghost is the only thing that can
                return FromMethod(SignatureOf(executable, true), executable);
            }
        }

        /// <summary>
        /// Returns the delegate type a call to the given member is made through.
        /// </summary>
        /// <param name="executable"></param>
        /// <param name="erased">Whether to take every reference as <see cref="object"/>.</param>
        /// <returns></returns>
        static Type SignatureOf(java.lang.reflect.Executable executable, bool erased)
        {
            var parameterClasses = executable.getParameterTypes();
            var receiver = IsInstance(executable) ? 1 : 0;

            var types = new Type[parameterClasses.Length + receiver];
            if (receiver == 1)
                types[0] = Resolve(executable.getDeclaringClass(), erased);
            for (int i = 0; i < parameterClasses.Length; i++)
                types[i + receiver] = Resolve(parameterClasses[i], erased);

            var returning = executable is java.lang.reflect.Method method ? method.getReturnType() : executable.getDeclaringClass();
            return CreateDelegateType(types, Resolve(returning, erased));
        }

        /// <summary>
        /// Returns the CLR type a class takes in a delegate's signature.
        /// </summary>
        /// <param name="clazz"></param>
        /// <param name="erased"></param>
        /// <returns></returns>
        static Type Resolve(java.lang.Class clazz, bool erased)
        {
            return erased && clazz.isPrimitive() == false ? typeof(object) : Linq4j.Tree.ClrTypes.FromClass(clazz);
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
            var methodType = (java.lang.invoke.MethodType)Call(LoadMethodType.MakeGenericMethod(delegateType), null)!;
            methodHandle = methodHandle.asType(methodType).asFixedArity();

            // the adapted handle materializes as its canonical delegate, which by construction has exactly the
            // signature asked for; where that is the type asked for there is nothing further to do
            var canonical = CreateDelegateType(Array.ConvertAll(invoke.GetParameters(), p => p.ParameterType), invoke.ReturnType);
            var inner = (Delegate)Call(GetDelegateForInvokeExact.MakeGenericMethod(canonical), [methodHandle])!;
            if (canonical == delegateType)
                return inner;

            return Delegate.CreateDelegate(delegateType, inner, Invoke(canonical), false)
                ?? throw new ArgumentException($"Cannot create a '{delegateType}' for a method handle of type {methodType}.", nameof(delegateType));
        }

        /// <summary>
        /// Calls one of <c>ByteCodeHelper</c>'s generic methods, whose type argument is only known here.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <remarks>
        /// Reflection wraps whatever the target threw, and what it throws is the answer: a
        /// <c>WrongMethodTypeException</c> from <c>GetDelegateForInvokeExact</c> says the signature asked for
        /// is not one IKVM signs with, and <see cref="FromExecutable"/> reads it. Wrapped, nothing can.
        /// </remarks>
        static object? Call(MethodInfo method, object?[]? arguments)
        {
            try
            {
                return method.Invoke(null, arguments);
            }
            catch (TargetInvocationException e) when (e.InnerException != null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                throw;
            }
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
