using System;

namespace Apache.Calcite.Extensions.Runtime
{

    /// <summary>
    /// Base of the record classes emitted for <c>JavaRowFormat.CUSTOM</c>.
    /// </summary>
    /// <remarks>
    /// Calcite's generated record declares <c>equals</c>, <c>hashCode</c>, <c>compareTo</c> and
    /// <c>toString</c> itself, and implements <c>Serializable</c> and nothing else. So does the type
    /// <c>SyntheticRecordEmitter.ClassDecl</c> emits: the four are on the record, not here.
    ///
    /// <para>What is here is the base a CLR type needs so that those four are reachable by the names the two
    /// runtimes call them by. <c>equals</c> and <c>hashCode</c> are <see cref="Equals(object)"/> and
    /// <see cref="GetHashCode"/>, which are what IKVM maps a Java call onto, and the record overrides those.
    /// <c>compareTo</c> is <see cref="IComparable.CompareTo"/>, because <c>java.lang.Comparable</c> is a ghost
    /// deriving from it and declares no method of its own — a value reaches it through
    /// <c>Comparable.__Helper.compareTo</c>, which calls <see cref="IComparable.CompareTo"/>.</para>
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
        /// <param name="other"></param>
        /// <returns></returns>
        public abstract int compareTo(object? other);

        /// <inheritdoc />
        public int CompareTo(object? other) => compareTo(other);

    }

}
