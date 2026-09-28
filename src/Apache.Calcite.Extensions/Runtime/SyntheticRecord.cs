using System;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Base of the record classes emitted for <c>JavaRowFormat.CUSTOM</c> rows.
    /// </summary>
    /// <remarks>
    /// Like Calcite's generated record, an emitted record defines equality, hashing, ordering and
    /// <c>toString</c> itself; this base makes each reachable under both its Java and its CLR name. IKVM
    /// maps a Java <c>equals</c> or <c>hashCode</c> call onto <see cref="Equals(object)"/> and
    /// <see cref="GetHashCode"/>. <c>java.lang.Comparable</c> is an IKVM ghost interface over
    /// <see cref="IComparable"/>, so a Java <c>compareTo</c> call arrives at
    /// <see cref="IComparable.CompareTo"/>, which forwards to <see cref="compareTo"/>.
    /// </remarks>
    public abstract class SyntheticRecord : java.lang.Comparable
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        protected SyntheticRecord()
        {

        }

        /// <inheritdoc cref="Equals(object)" />
        public bool equals(object? other) => Equals(other);

        /// <inheritdoc />
        public abstract override bool Equals(object? other);

        /// <inheritdoc cref="GetHashCode" />
        public int hashCode() => GetHashCode();

        /// <inheritdoc />
        public abstract override int GetHashCode();

        /// <inheritdoc />
        public abstract override string ToString();

        /// <summary>
        /// Orders this record against another, field by field.
        /// </summary>
        /// <param name="other">The record to compare with.</param>
        /// <returns>A negative number, zero or a positive number as this record orders before, equal to or
        /// after <paramref name="other"/>.</returns>
        public abstract int compareTo(object? other);

        /// <inheritdoc />
        public int CompareTo(object? other) => compareTo(other);

    }

}
