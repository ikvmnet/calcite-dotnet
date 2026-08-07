using System;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The members of <c>PhysTypeImpl</c> that Calcite declares package private, written again because they
    /// cannot be called.
    /// </summary>
    /// <remarks>
    /// A workaround, and named so that it reads as one. Nothing here is a port in the sense the rest of this
    /// convention is: the code it replaces is Calcite's, it is reachable from Calcite's own package, and every
    /// line of it would be deleted the day the member became public.
    ///
    /// <para>What is needed is a <c>PhysType</c> for a row whose type is a synthetic record rather than a
    /// relational type — an accumulator's, which is <c>createSyntheticType(List&lt;Type&gt;)</c> over the state
    /// types each aggregate implementor asked for. <c>EnumerableAggregate</c> gets one in a line:
    /// <c>PhysTypeImpl.of(typeFactory, typeFactory.createSyntheticType(aggStateTypes))</c>.</para>
    ///
    /// <para>There is no public route to it. The public <c>of</c> takes a <see cref="RelDataType"/>, and the
    /// obvious way to get one — <c>typeFactory.createType(syntheticRecordType)</c> — returns the record's
    /// <c>relType</c> and requires it non-null, while <c>createSyntheticType(List&lt;Type&gt;)</c> builds the
    /// record with <c>relType</c> null. Only the <c>RelRecordType</c> overload sets it. So the row type has to
    /// be rebuilt a field at a time, which is what the package private <c>of</c> does.</para>
    /// </remarks>
    static class PhysTypeImplWorkaround
    {

        /// <summary>
        /// Returns the physical type of a row whose type is a Java row class rather than a relational type.
        /// </summary>
        /// <param name="typeFactory"></param>
        /// <param name="javaRowClass"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>PhysTypeImpl.of(JavaTypeFactory, Type)</c>. The row type is rebuilt from the fields of the record
        /// and the format is left unoptimised, exactly as it does, because an accumulator of one field is still
        /// a record.
        ///
        /// <para>One line of it cannot be reproduced. Calcite ends
        /// <c>new PhysTypeImpl(typeFactory, rowType, javaRowClass, CUSTOM)</c>, passing the record it was
        /// given straight through; that constructor is package private too, so this goes back out through the
        /// public <c>of</c>, which derives the row class again from the row type just built. The two agree
        /// because <c>JavaTypeFactoryImpl.register</c> interns a synthetic type on its field types and their
        /// nullability, so the round trip returns the same object — but that is an agreement rather than an
        /// identity, and it is the one place this differs from what it replaces.</para>
        /// </remarks>
        public static PhysType Of(JavaTypeFactory typeFactory, java.lang.reflect.Type javaRowClass)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(javaRowClass);

            var builder = typeFactory.builder();

            if (javaRowClass is J.Types.RecordType recordType)
            {
                var fields = recordType.getRecordFields();
                for (int i = 0; i < fields.size(); i++)
                {
                    var field = (J.Types.RecordField)fields.get(i);
                    builder.add(field.getName(), typeFactory.createType(field.getType()));
                }
            }

            return PhysTypeImpl.of(typeFactory, builder.build(), JavaRowFormat.CUSTOM, false);
        }

    }

}
