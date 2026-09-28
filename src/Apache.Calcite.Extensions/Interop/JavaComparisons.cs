using System;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// The <c>Utilities</c> comparisons that take a <c>java.lang.Comparable</c>, taking <see cref="object"/>
    /// and comparing through <see cref="IComparable"/>.
    /// </summary>
    /// <remarks>
    /// <c>PhysTypeImpl.generateComparator</c> casts each field to <c>Comparable</c> only so that the Java
    /// compiler picks the <c>(Comparable, Comparable)</c> overload. <c>java.lang.Comparable</c> is an IKVM
    /// ghost interface: a <see cref="string"/> satisfies it in Java but not in the CLR type system, so a CLR
    /// cast to it throws, in an expression tree and in C# alike. IKVM maps it onto <see cref="IComparable"/>,
    /// which every Java-comparable value implements, so these methods compare through that instead.
    ///
    /// <para>The null handling is Calcite's: nulls-first orders a null below any value, nulls-last above,
    /// and the merge-join comparison throws when both values are null.</para>
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
        /// Mirrors <c>EnumerableDefaults.compareNullsLastForMergeJoin</c>, to which <c>Utilities</c> forwards.
        /// Two nulls throw linq4j's <c>BothValuesAreNullException</c>, which the merge join catches; treating
        /// them as equal would join nulls to nulls.
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
        /// Creates linq4j's <c>EnumerableDefaults.BothValuesAreNullException</c>.
        /// </summary>
        /// <remarks>
        /// The type is package private, so it is created by reflection.
        /// </remarks>
        /// <returns>A new instance of the exception, for the caller to throw.</returns>
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
