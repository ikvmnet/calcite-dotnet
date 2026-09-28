using System;
using System.Reflection;

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
        /// The package private <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>. IKVM compiles a package private
        /// member to an internal one, which reflection can reach.
        /// </summary>
        static readonly MethodInfo OfJavaRowClass =
            typeof(PhysTypeImpl).GetMethod("of", BindingFlags.NonPublic | BindingFlags.Static, [typeof(JavaTypeFactory), typeof(java.lang.reflect.Type)])
            ?? throw new NotSupportedException("Calcite's PhysTypeImpl has no package private of(JavaTypeFactory, Type).");

        /// <summary>
        /// Returns the physical type of a row described by a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory">The type factory the physical type uses.</param>
        /// <param name="javaRowClass">The row class, such as a synthetic record over accumulator state types.</param>
        /// <returns>A physical type whose row class is <paramref name="javaRowClass"/>.</returns>
        /// <remarks>
        /// An exception Calcite throws is rethrown unwrapped, with its original stack trace.
        /// </remarks>
        public static PhysType Of(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(javaRowClass);

            try
            {
                return (PhysType)OfJavaRowClass.Invoke(null, [typeFactory, javaRowClass])!;
            }
            catch (TargetInvocationException e) when (e.InnerException is not null)
            {
                System.Runtime.ExceptionServices.ExceptionDispatchInfo.Capture(e.InnerException).Throw();
                throw;
            }
        }

    }

}
