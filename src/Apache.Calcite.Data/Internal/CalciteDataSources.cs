using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Provides lookup for a data source based on a connection string.
    /// </summary>
    /// <remarks>
    /// The <see cref="CalciteDataSource"/> a bare <c>new CalciteConnection(connectionString)</c> draws on.
    /// A connection string that has not been seen before makes one, and every connection opened with an
    /// equivalent string afterwards shares it, so a model is read once for the process rather than once per
    /// connection. Data sources a caller builds for itself are referenced directly by the caller and are not
    /// held here.
    ///
    /// <para>An entry is held strongly, because the point of it is to be there for the next connection when
    /// nothing else references it; what bounds the set is time rather than reachability. Each entry has a
    /// timer that fires every <see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/> and
    /// prunes the entry once it has gone <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>
    /// with no connection open on it — removed here and disposed, so a schema holding a client gets to close
    /// it. <see cref="Clear"/> and <see cref="ClearAll"/> do the same on demand, and an entry still held at
    /// process exit is disposed then. Disposal retires the root rather than tearing it down: a connection
    /// still open keeps it until that connection is disposed.</para>
    /// </remarks>
    internal static class CalciteDataSources
    {

        /// <summary>
        /// A data source the provider keeps, with the timer that prunes it.
        /// </summary>
        sealed class Entry
        {

            public Entry(CalciteDataSource dataSource, string key)
            {
                DataSource = dataSource;
                Timer = new Timer(_ => Prune(key, this), null, dataSource.PruningInterval, dataSource.PruningInterval);
            }

            public CalciteDataSource DataSource { get; }

            public Timer Timer { get; }

        }

        static readonly ConcurrentDictionary<string, Entry> registered = new();

        static CalciteDataSources()
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) => ClearAll();
            AppDomain.CurrentDomain.DomainUnload += (_, _) => ClearAll();
        }

        /// <summary>
        /// Gets the data source for a connection string, making it if it is new.
        /// </summary>
        /// <param name="options">The connection string.</param>
        /// <returns>The data source.</returns>
        /// <remarks>
        /// An empty connection string names nothing to key on, and two connections written that way have no
        /// reason to meet, so it gets a data source of its own rather than the one every other empty string
        /// would share — one that is not kept here and builds its root for the one connection.
        ///
        /// <para>The data source answered may be pruned between this call and its use, so a caller that
        /// finds it disposed looks it up again; <see cref="CalciteConnection.Open"/> does.</para>
        /// </remarks>
        public static CalciteDataSource Resolve(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (options.Count == 0)
                return new CalciteDataSource(options, [], pooled: false);

            var key = options.DataSourceKey;
            if (registered.TryGetValue(key, out var existing))
                return existing.DataSource;

            // Really unseen, need to create a new data source. If someone beats us to it use what they put.
            var created = new Entry(new CalciteDataSource(new CalciteConnectionStringBuilder(key), []), key);
            var winner = registered.GetOrAdd(key, created);
            if (winner != created)
                Remove(created);

            return winner.DataSource;
        }

        /// <summary>
        /// Removes the data source for a connection string and disposes it, so that the next connection
        /// opened with it builds a new one.
        /// </summary>
        /// <param name="options">The connection string.</param>
        public static void Clear(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (options.Count > 0 && registered.TryRemove(options.DataSourceKey, out var entry))
                Remove(entry);
        }

        /// <summary>
        /// Removes every data source and disposes it, so that the next connection opened with any
        /// connection string builds a new one.
        /// </summary>
        public static void ClearAll()
        {
            foreach (var key in new List<string>(registered.Keys))
                if (registered.TryRemove(key, out var entry))
                    Remove(entry);
        }

        /// <summary>
        /// The timer's tick: disposes and removes the entry where nothing has used it for its idle lifetime.
        /// </summary>
        static void Prune(string key, Entry entry)
        {
            try
            {
                if (entry.DataSource.TryPrune(Environment.TickCount64) == false)
                    return;

                if (registered.TryRemove(new KeyValuePair<string, Entry>(key, entry)))
                    entry.Timer.Dispose();
            }
            catch
            {
                // a timer callback has nowhere to throw to; the next tick tries again
            }
        }

        static void Remove(Entry entry)
        {
            entry.Timer.Dispose();
            entry.DataSource.Dispose();
        }

    }

}
