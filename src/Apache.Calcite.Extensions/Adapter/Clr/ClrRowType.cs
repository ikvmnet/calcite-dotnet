using System;

using org.apache.calcite.jdbc;

namespace Apache.Calcite.Extensions.Adapter.Clr
{

    /// <summary>
    /// The row type of a table whose rows are instances of a CLR class.
    /// </summary>
    /// <remarks>
    /// A <see cref="JavaRecordType"/>, which is the whole of how the class survives: <c>getJavaClass</c> tests
    /// for one and answers with its <c>clazz</c>, before it can reach <c>createSyntheticType</c> and lose it.
    /// That is the same mechanism <c>ReflectiveSchema</c> rides — <c>createStructType(Class)</c> ends in
    /// <c>canonize(new JavaRecordType(list, type))</c> — and the only difference here is that the fields are
    /// found by <see cref="ClrReflect"/> rather than by <c>getFields()</c>, which for a .NET class is empty.
    ///
    /// <para>It carries a <see cref="ClrRecordType"/> as well because a row of it has two Java identities, for
    /// two different questions. Building one is <c>new_(clazz, ...)</c> and wants the class. Reading a member
    /// of one goes through <c>JavaRowFormat.CUSTOM.field</c>, which branches on the declared type of the row
    /// <em>expression</em> and would call <c>getFields()[i]</c> on a class — so what declares a row of this is
    /// the record type. Both resolve to the same CLR type, so nothing downstream sees a disagreement.</para>
    ///
    /// <para><b>Two things this does not get that Calcite's own does.</b> <c>canonize</c> is protected, so
    /// this is not interned; the type is built once per table and <c>RelOptTableImpl</c> caches
    /// <c>getRowType</c>, so a single instance circulates and structural equality still holds, but it is an
    /// agreement rather than an identity. And <c>createTypeWithNullability</c> asked for a nullability this
    /// does not have goes through <c>copyRecordType</c> to <c>createStructType</c>, which drops the class;
    /// what saves it is that a row type is NOT NULL and asking for NOT NULL returns the same object, so the
    /// loss only reaches a reflective row used as a nullable struct <em>column</em>. Calcite degrades
    /// identically there, and this reproduces it rather than working around it.</para>
    /// </remarks>
    public sealed class ClrRowType : JavaRecordType
    {

        readonly java.lang.Class clazz;
        readonly ClrRecordType recordType;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="fields">One <c>RelDataTypeField</c> per member, in <see cref="ClrReflect"/>'s order.</param>
        /// <param name="clazz">The CLR class a row is an instance of.</param>
        /// <param name="recordType">The same class, presented so its members can be reached by ordinal.</param>
        public ClrRowType(java.util.List fields, java.lang.Class clazz, ClrRecordType recordType) :
            base(fields, clazz)
        {
            this.clazz = clazz ?? throw new ArgumentNullException(nameof(clazz));
            this.recordType = recordType ?? throw new ArgumentNullException(nameof(recordType));
        }

        /// <summary>
        /// The class a row is an instance of.
        /// </summary>
        /// <remarks>
        /// Kept rather than read back off <see cref="JavaRecordType"/>, whose <c>clazz</c> is package private
        /// in Java and so <c>internal</c> in what IKVM compiled — nameable from here and not readable.
        /// </remarks>
        public java.lang.Class Clazz => clazz;

        /// <summary>
        /// The class of a row, presented so that a member can be reached by ordinal.
        /// </summary>
        public ClrRecordType RecordType => recordType;

    }

}
