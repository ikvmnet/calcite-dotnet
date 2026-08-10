using System;
using System.Collections.Generic;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A bounded, least-recently-used <see cref="IPlanCache"/>.
    /// </summary>
    /// <remarks>
    /// Bounded by entry count rather than by weight: a plan is a compiled delegate whose size nothing can
    /// measure honestly. One lock over a map and a recency list — a plan cache is consulted once per
    /// statement, not per row, so the lock is not on a hot path even when the instance is shared between
    /// connections.
    /// </remarks>
    public sealed class LruPlanCache : IPlanCache
    {

        readonly object gate = new();
        readonly int capacity;
        readonly Dictionary<PlanCacheKey, LinkedListNode<KeyValuePair<PlanCacheKey, PreparedPlan>>> map = new();
        readonly LinkedList<KeyValuePair<PlanCacheKey, PreparedPlan>> order = new();

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="capacity">The most entries the cache will hold.</param>
        /// <exception cref="ArgumentOutOfRangeException">Thrown when <paramref name="capacity"/> is not
        /// positive.</exception>
        public LruPlanCache(int capacity)
        {
            if (capacity <= 0)
                throw new ArgumentOutOfRangeException(nameof(capacity), capacity, "A plan cache needs room for at least one plan.");

            this.capacity = capacity;
        }

        /// <summary>
        /// Gets the most entries the cache will hold.
        /// </summary>
        public int Capacity => capacity;

        /// <summary>
        /// Gets the number of entries currently held.
        /// </summary>
        public int Count
        {
            get { lock (gate) return map.Count; }
        }

        /// <inheritdoc />
        public PreparedPlan? Get(in PlanCacheKey key)
        {
            lock (gate)
            {
                if (map.TryGetValue(key, out var node) == false)
                    return null;

                order.Remove(node);
                order.AddFirst(node);
                return node.Value.Value;
            }
        }

        /// <inheritdoc />
        public void Add(in PlanCacheKey key, PreparedPlan plan)
        {
            ArgumentNullException.ThrowIfNull(plan);

            lock (gate)
            {
                if (map.TryGetValue(key, out var node))
                {
                    order.Remove(node);
                    map.Remove(key);
                }

                map.Add(key, order.AddFirst(new KeyValuePair<PlanCacheKey, PreparedPlan>(key, plan)));

                while (map.Count > capacity && order.Last is { } last)
                {
                    order.RemoveLast();
                    map.Remove(last.Value.Key);
                }
            }
        }

        /// <inheritdoc />
        public void Remove(in PlanCacheKey key)
        {
            lock (gate)
            {
                if (map.TryGetValue(key, out var node))
                {
                    order.Remove(node);
                    map.Remove(key);
                }
            }
        }

        /// <inheritdoc />
        public void Clear(PlanCacheScope scope)
        {
            ArgumentNullException.ThrowIfNull(scope);

            lock (gate)
            {
                var node = order.First;
                while (node is not null)
                {
                    var next = node.Next;
                    if (ReferenceEquals(node.Value.Key.Scope, scope))
                    {
                        order.Remove(node);
                        map.Remove(node.Value.Key);
                    }

                    node = next;
                }
            }
        }

    }

}
