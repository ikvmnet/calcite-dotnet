using System;

using Apache.Calcite.Extensions.Interop;

using IKVM.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Calls <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>, which Calcite declares package private.
    /// </summary>
    /// <remarks>
    /// The public <c>PhysTypeImpl.of</c> derives the row class again from the relational row type. Accumulators
    /// whose classes have no public fields, such as <c>UnionOperation</c> and <c>CollectOperation</c>, all map
    /// to the same interned empty struct type, so that route can return another accumulator's row class. This
    /// overload keeps the row class it is given, as <c>EnumerableAggregate</c> does.
    /// </remarks>
    static class PhysTypeImplWorkaround
    {

        /// <summary>
        /// A delegate over the package private <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>.
        /// </summary>
        /// <remarks>
        /// The method is found by its Java name and signature, made accessible, and wrapped by
        /// <c>ikvm.runtime.Util.getDelegateFromMethod</c>. If Calcite renames or removes it, the class
        /// initializer throws.
        /// </remarks>
        static readonly MH<object, object, object> OfJavaRowClass = CreateOfJavaRowClass();

        /// <summary>
        /// Resolves <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c> and returns a delegate that calls it.
        /// </summary>
        /// <returns>A delegate taking the type factory and the row class and returning the physical type.</returns>
        static MH<object, object, object> CreateOfJavaRowClass()
        {
            var method = ((java.lang.Class)typeof(PhysTypeImpl)).getDeclaredMethod("of", (java.lang.Class)typeof(JavaTypeFactory), (java.lang.Class)typeof(java.lang.reflect.Type));
            method.setAccessible(true);
            return (MH<object, object, object>)JavaDelegates.FromMethod(method);
        }

        /// <summary>
        /// Returns the physical type of a row described by a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory">The type factory the physical type uses.</param>
        /// <param name="javaRowClass">The row class, such as a synthetic record over accumulator state types.</param>
        /// <returns>A physical type whose row class is <paramref name="javaRowClass"/>.</returns>
        public static PhysType Of(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(javaRowClass);

            return (PhysType)OfJavaRowClass(typeFactory, javaRowClass);
        }

    }

}
