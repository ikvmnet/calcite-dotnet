using System;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// The comparisons of <c>Utilities</c> that take a <c>java.lang.Comparable</c>, for values that have been
    /// through IKVM.
    /// </summary>
    /// <remarks>
    /// <c>PhysTypeImpl.generateComparator</c> casts a field to <c>Comparable</c> before comparing it, and the
    /// cast is not a conversion: it is there so javac picks the <c>(Comparable, Comparable)</c> overload out of
    /// the source Janino writes, rather than one of the eight primitive ones. Calcite never checks anything by
    /// it, because in Java every value it reaches already is a <c>Comparable</c>.
    ///
    /// <para>Here the cast would run, and <c>java.lang.Comparable</c> is one of the interfaces IKVM implements
    /// as a <em>ghost</em>: a <see cref="string"/> satisfies it without the CLR type system saying so, and a
    /// <c>castclass</c> to it throws. That holds for C# we write as much as for a tree we build — the ghost
    /// conversion is emitted by IKVM's own compiler for the Java it compiles, and nowhere else.</para>
    ///
    /// <para>So the comparison is the value's own, reached the way the CLR states it. IKVM maps
    /// <c>java.lang.Comparable</c> onto <see cref="IComparable"/>, so a value that is comparable in Java is
    /// comparable here, by an interface the type system does see. The null ordering is Calcite's, copied:
    /// nulls first sorts them below, nulls last above, and a merge join refuses two of them outright, because
    /// treating them as equal would join a null to a null.</para>
    /// </remarks>
    static class JavaComparisons
    {

        /// <summary>
        /// <c>Utilities.compare(Comparable, Comparable)</c>.
        /// </summary>
        public static int Compare(object v0, object v1) => ((IComparable)v0).CompareTo(v1);

        /// <summary>
        /// <c>Utilities.compare(Comparable, Comparable, Comparator)</c>.
        /// </summary>
        public static int Compare(object v0, object v1, java.util.Comparator comparator) => comparator.compare(v0, v1);

        /// <summary>
        /// <c>Utilities.compareNullsFirst(Comparable, Comparable)</c>.
        /// </summary>
        public static int CompareNullsFirst(object? v0, object? v1)
        {
            return ReferenceEquals(v0, v1) ? 0
                : v0 == null ? -1
                : v1 == null ? 1
                : ((IComparable)v0).CompareTo(v1);
        }

        /// <summary>
        /// <c>Utilities.compareNullsFirst(Comparable, Comparable, Comparator)</c>.
        /// </summary>
        public static int CompareNullsFirst(object? v0, object? v1, java.util.Comparator comparator)
        {
            return ReferenceEquals(v0, v1) ? 0
                : v0 == null ? -1
                : v1 == null ? 1
                : comparator.compare(v0, v1);
        }

        /// <summary>
        /// <c>Utilities.compareNullsLast(Comparable, Comparable)</c>.
        /// </summary>
        public static int CompareNullsLast(object? v0, object? v1)
        {
            return ReferenceEquals(v0, v1) ? 0
                : v0 == null ? 1
                : v1 == null ? -1
                : ((IComparable)v0).CompareTo(v1);
        }

        /// <summary>
        /// <c>Utilities.compareNullsLast(Comparable, Comparable, Comparator)</c>.
        /// </summary>
        public static int CompareNullsLast(object? v0, object? v1, java.util.Comparator comparator)
        {
            return ReferenceEquals(v0, v1) ? 0
                : v0 == null ? 1
                : v1 == null ? -1
                : comparator.compare(v0, v1);
        }

        /// <summary>
        /// <c>Utilities.compareNullsLastForMergeJoin(Comparable, Comparable)</c>.
        /// </summary>
        public static int CompareNullsLastForMergeJoin(object? v0, object? v1) => CompareNullsLastForMergeJoin(v0, v1, null);

        /// <summary>
        /// <c>Utilities.compareNullsLastForMergeJoin(Comparable, Comparable, Comparator)</c>.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableDefaults.compareNullsLastForMergeJoin</c>, which is what <c>Utilities</c> forwards to.
        /// Two nulls are not equal here, and saying so is the exception the algorithm reads: a merge join that
        /// called them equal would join every null of one input to every null of the other.
        /// </remarks>
        public static int CompareNullsLastForMergeJoin(object? v0, object? v1, java.util.Comparator? comparator)
        {
            if (v0 == null && v1 == null)
                throw BothValuesAreNull();

            return ReferenceEquals(v0, v1) ? 0
                : v0 == null ? 1
                : v1 == null ? -1
                : comparator == null ? ((IComparable)v0).CompareTo(v1) : comparator.compare(v0, v1);
        }

        /// <summary>
        /// Returns the exception a merge join reads as "these two keys are both null".
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableDefaults.BothValuesAreNullException</c>, which linq4j declares package private, so it
        /// can be neither named nor constructed from here. <c>ClrEnumerableDefaults.MergeJoin</c> catches it by
        /// the name on the type for the same reason; this is the other half of that.
        /// </remarks>
        static java.lang.RuntimeException BothValuesAreNull()
        {
            return (java.lang.RuntimeException)Activator.CreateInstance(BothValuesAreNullType, nonPublic: true)!;
        }

        /// <inheritdoc cref="BothValuesAreNull" />
        static readonly Type BothValuesAreNullType =
            typeof(org.apache.calcite.linq4j.EnumerableDefaults).GetNestedType("BothValuesAreNullException", System.Reflection.BindingFlags.NonPublic | System.Reflection.BindingFlags.Public)
            ?? throw new NotSupportedException("linq4j has no EnumerableDefaults.BothValuesAreNullException.");

    }

}
