using System;
using System.Runtime.CompilerServices;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// What a prepared plan is stored under.
    /// </summary>
    /// <remarks>
    /// Everything the prepare pipeline is given that changes its answer, and nothing it derives: the SQL
    /// text, the convention asked for, the element type, the row limit, and the scope and its generation.
    /// The session-invariant inputs — schema instance, type factory, configuration, default schema path —
    /// are the scope's identity and are not repeated here.
    ///
    /// <para>The scope and element type compare by reference. A scope has no value to compare, and an
    /// element type is a <c>java.lang.Class</c>, one instance per class.</para>
    /// </remarks>
    public readonly struct PlanCacheKey : IEquatable<PlanCacheKey>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        internal PlanCacheKey(PlanCacheScope scope, long generation, string sql, bool async, java.lang.reflect.Type elementType, long maxRowCount)
        {
            Scope = scope ?? throw new ArgumentNullException(nameof(scope));
            Generation = generation;
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Async = async;
            ElementType = elementType ?? throw new ArgumentNullException(nameof(elementType));
            MaxRowCount = maxRowCount;
        }

        /// <summary>
        /// Gets the session partition the entry belongs to.
        /// </summary>
        public PlanCacheScope Scope { get; }

        /// <summary>
        /// Gets the scope's schema generation the plan was built under.
        /// </summary>
        public long Generation { get; }

        /// <summary>
        /// Gets the statement's text.
        /// </summary>
        public string Sql { get; }

        /// <summary>
        /// Gets whether the plan was prepared into the asynchronous convention.
        /// </summary>
        public bool Async { get; }

        /// <summary>
        /// Gets what a caller asked a row to be.
        /// </summary>
        public java.lang.reflect.Type ElementType { get; }

        /// <summary>
        /// Gets the row limit, or a negative number for none.
        /// </summary>
        public long MaxRowCount { get; }

        /// <inheritdoc />
        public bool Equals(PlanCacheKey other)
        {
            return ReferenceEquals(Scope, other.Scope)
                && Generation == other.Generation
                && string.Equals(Sql, other.Sql, StringComparison.Ordinal)
                && Async == other.Async
                && ReferenceEquals(ElementType, other.ElementType)
                && MaxRowCount == other.MaxRowCount;
        }

        /// <inheritdoc />
        public override bool Equals(object? obj) => obj is PlanCacheKey other && Equals(other);

        /// <inheritdoc />
        public override int GetHashCode()
        {
            return HashCode.Combine(
                RuntimeHelpers.GetHashCode(Scope),
                Generation,
                Sql is null ? 0 : StringComparer.Ordinal.GetHashCode(Sql),
                Async,
                RuntimeHelpers.GetHashCode(ElementType),
                MaxRowCount);
        }

    }

}
