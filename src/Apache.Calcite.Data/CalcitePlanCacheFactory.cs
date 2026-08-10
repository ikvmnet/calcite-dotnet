using System;

using Apache.Calcite.Extensions.Prepare;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Creates the plan cache for a connection's session.
    /// </summary>
    /// <remarks>
    /// Consulted once, when the session is created on the connection's first open. The factory decides
    /// scope: return a new cache per call for a connection-private cache, or the same instance from every
    /// call to share one capacity budget across connections. Entries are never shared between sessions
    /// either way — a plan is meaningful only to the session that made it — so the choice is about memory,
    /// not correctness. The connection passed in is still opening: its connection string is readable, its
    /// session-backed members are not.
    ///
    /// <para>The session never disposes a cache, because it cannot know the factory did not share it; it
    /// clears its own entries and no more. A factory that hands out per-connection caches owning anything
    /// disposable is the one party positioned to tie their lifetimes together.</para>
    /// </remarks>
    public abstract class CalcitePlanCacheFactory
    {

        /// <summary>
        /// Returns a factory that answers every connection with <paramref name="cache"/>, sharing one
        /// capacity budget across the connections it is set on.
        /// </summary>
        /// <param name="cache">The cache every connection is answered with.</param>
        /// <exception cref="ArgumentNullException">Thrown when <paramref name="cache"/> is <see langword="null"/>.</exception>
        public static CalcitePlanCacheFactory From(IPlanCache cache)
        {
            ArgumentNullException.ThrowIfNull(cache);

            return new InstanceFactory(cache);
        }

        /// <summary>
        /// Creates or returns the plan cache for a session being created.
        /// </summary>
        /// <param name="connection">The connection whose session is being created.</param>
        /// <returns>The cache the session will consult for every statement it plans.</returns>
        public abstract IPlanCache CreatePlanCache(CalciteConnection connection);

        /// <summary>
        /// The factory <see cref="From"/> answers with.
        /// </summary>
        sealed class InstanceFactory : CalcitePlanCacheFactory
        {

            readonly IPlanCache cache;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            public InstanceFactory(IPlanCache cache)
            {
                this.cache = cache;
            }

            /// <inheritdoc />
            public override IPlanCache CreatePlanCache(CalciteConnection connection) => cache;

        }

    }

}
