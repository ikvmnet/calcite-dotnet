using System.Threading;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// One session's partition of a plan cache.
    /// </summary>
    /// <remarks>
    /// A prepared plan is meaningful only to the session that planned it: its row types are interned by
    /// that session's type factory, and a scannable table it reads may be held in the compiled tree as the
    /// instance the schema answered at plan time. Entries are therefore partitioned by scope, and a cache
    /// instance shared between sessions shares capacity and eviction — never plans.
    ///
    /// <para>The generation is the scope's schema version, and it lives here rather than on a plan because
    /// it is relative: a plan does not know what version it was built under, a key does. Executing DDL
    /// bumps it, which strands every key built under the old generation — the version-in-key pattern,
    /// which needs no invalidation callback and so cannot miss one. Stranded entries leave the cache
    /// through its own eviction.</para>
    /// </remarks>
    public sealed class PlanCacheScope
    {

        long generation;

        /// <summary>
        /// Gets the scope's current schema generation.
        /// </summary>
        internal long Generation => Interlocked.Read(ref generation);

        /// <summary>
        /// Moves the scope to a new generation, stranding every key built under the old one.
        /// </summary>
        internal void BumpGeneration() => Interlocked.Increment(ref generation);

    }

}
