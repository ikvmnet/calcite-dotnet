namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A store for prepared plans.
    /// </summary>
    /// <remarks>
    /// An implementation owns retention and eviction and nothing else. What may be cached, what the key
    /// is, whether a hit is still valid against the live schema, and when a generation moves are all
    /// decided by the prepare pipeline before an implementation is consulted — so an implementation can
    /// cost hit rate, and cannot cost correctness. Every method may be called concurrently when the
    /// instance is shared between connections.
    ///
    /// <para>A plan is an in-process compiled delegate. It cannot be serialized, so a cache cannot be
    /// distributed, which is why the contract is synchronous: there is no I/O a legitimate implementation
    /// could be doing.</para>
    /// </remarks>
    public interface IPlanCache
    {

        /// <summary>
        /// Returns the plan stored under <paramref name="key"/>, or <see langword="null"/> where there is
        /// none.
        /// </summary>
        PreparedPlan? Get(in PlanCacheKey key);

        /// <summary>
        /// Offers a plan for <paramref name="key"/>. An implementation may decline to retain it.
        /// </summary>
        void Add(in PlanCacheKey key, PreparedPlan plan);

        /// <summary>
        /// Removes the plan stored under <paramref name="key"/>, which the pipeline has found stale.
        /// </summary>
        void Remove(in PlanCacheKey key);

        /// <summary>
        /// Drops every entry belonging to <paramref name="scope"/>. Called when the owning session is
        /// disposed, so a shared cache does not hold a dead session's plans until eviction finds them.
        /// </summary>
        void Clear(PlanCacheScope scope);

    }

}
