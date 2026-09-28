using System;

using Apache.Calcite.Extensions.Interop;

using IKVM.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// The members of <c>PhysTypeImpl</c> that Calcite declares package private, reached because they cannot
    /// be called.
    /// </summary>
    /// <remarks>
    /// A workaround, and named so that it reads as one. Nothing here is a port in the sense the rest of this
    /// convention is: the code it reaches is Calcite's, it is callable from Calcite's own package, and every
    /// line of this would be deleted the day the member became public.
    ///
    /// <para>What is needed is a <c>PhysType</c> for a row whose type is a synthetic record rather than a
    /// relational type — an accumulator's, which is <c>createSyntheticType(List&lt;Type&gt;)</c> over the state
    /// types each aggregate implementor asked for. <c>EnumerableAggregate</c> gets one in a line:
    /// <c>PhysTypeImpl.of(typeFactory, typeFactory.createSyntheticType(aggStateTypes))</c>.</para>
    ///
    /// <para>There is no public route to it, and <b>it is not reproducible from the public members</b>, which
    /// is why this calls Calcite's rather than writing it again. It builds the row type a field at a time and
    /// then passes the record it was given straight to the package private constructor. The public
    /// <c>of</c> takes the row type alone and derives the row class again from it, and that is not the same
    /// record: a field whose type is a class with no public fields — an accumulator such as
    /// <c>UnionOperation</c> or <c>CollectOperation</c> — becomes the empty struct type, whose digest is
    /// <c>RecordType()</c> whatever the class. Relational types are interned process wide on their digest, so
    /// two such accumulators share one row type across every connection, and the row class derived from it is
    /// whichever class reached the interner first. Measured: <c>ST_COLLECT</c> and then <c>ST_UNION</c>, on
    /// separate connections, gave the union a <c>CollectOperation</c> slot to assign a <c>UnionOperation</c>
    /// to.</para>
    /// </remarks>
    static class PhysTypeImplWorkaround
    {

        /// <summary>
        /// <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>, which is package private.
        /// </summary>
        /// <remarks>
        /// Resolved by Java reflection and called through <c>ikvm.runtime.Util.getDelegateFromMethod</c> over
        /// the method marked accessible, so the member is found by its Java name and signature rather than by
        /// what IKVM compiled it to, and a call is a delegate call rather than a <c>MethodInfo.Invoke</c>. A
        /// snapshot that renames it fails here, in the class initializer, rather than at the first aggregate.
        /// </remarks>
        static readonly MH<object, object, object> OfJavaRowClass = CreateOfJavaRowClass();

        static MH<object, object, object> CreateOfJavaRowClass()
        {
            var method = ((java.lang.Class)typeof(PhysTypeImpl)).getDeclaredMethod("of", (java.lang.Class)typeof(JavaTypeFactory), (java.lang.Class)typeof(java.lang.reflect.Type));
            method.setAccessible(true);
            return (MH<object, object, object>)JavaDelegates.FromMethod(method);
        }

        /// <summary>
        /// Returns the physical type of a row whose type is a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="javaRowClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c> itself, so the physical type carries the record it was
        /// given as its row class, exactly as <c>EnumerableAggregate</c>'s does.
        /// </remarks>
        public static PhysType Of(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(javaRowClass);

            return (PhysType)OfJavaRowClass(typeFactory, javaRowClass);
        }

    }

}
