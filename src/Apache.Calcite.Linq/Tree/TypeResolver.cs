using System;

using JavaType = java.lang.reflect.Type;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Resolves the <see cref="JavaType"/> that a linq4j tree carries to the CLR type the translated
    /// <see cref="System.Linq.Expressions.Expression"/> is built against.
    /// </summary>
    /// <remarks>
    /// A linq4j tree is typed in Java's reflection model because that is what Calcite hands to Janino.
    /// Under IKVM every one of those types is a real CLR type, so this is a lookup rather than a mapping:
    /// nothing here decides what a value is, it only says which CLR type already holds it.
    /// </remarks>
    static class TypeResolver
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

    }

}
