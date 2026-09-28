using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Threading;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The process-wide set of data sources the provider keeps, one per connection string.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A connection created from a connection string alone draws on the data source kept here for that
    /// string, keyed by <see cref="CalciteConnectionStringBuilder.DataSourceKey"/>, so equivalent connection
    /// strings share one root schema and a model is read once per process rather than once per connection.
    /// Data sources built with <see cref="CalciteDataSourceBuilder"/> are not held here.
    /// </para>
    /// <para>
    /// Entries are held strongly, so an entry survives while no connection references it; time bounds the
    /// set instead. Each entry has a timer that fires every
    /// <see cref="CalciteConnectionStringBuilder.ConnectionPruningInterval"/> and removes and disposes the
    /// entry once it has had no open connection for
    /// <see cref="CalciteConnectionStringBuilder.ConnectionIdleLifetime"/>. <see cref="Clear"/> and
    /// <see cref="ClearAll"/> do the same on demand, and entries still held at process exit or domain unload
    /// are disposed then. Disposing a data source retires its root, so a connection still open keeps working
    /// until it is disposed.
    /// </para>
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
        /// Gets the data source for a connection string, creating and keeping it where there is none.
        /// </summary>
        /// <param name="options">The connection string.</param>
        /// <returns>The data source.</returns>
        /// <remarks>
        /// <para>
        /// An empty connection string gets a new, unpooled data source that is not kept, so each such
        /// connection builds its own root.
        /// </para>
        /// <para>
        /// The data source returned may be pruned before it is used, so a caller that finds it disposed looks
        /// it up again, as <see cref="CalciteConnection.Open"/> does.
        /// </para>
        /// </remarks>
        public static CalciteDataSource Resolve(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (options.Count == 0)
                return new CalciteDataSource(options, [], pooled: false);

            var key = options.DataSourceKey;
            if (registered.TryGetValue(key, out var existing))
                return existing.DataSource;

            // not seen yet; where another thread adds one first, that one is used and this one discarded
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
        /// The timer's tick: removes the entry where it has had no open connection for its idle lifetime.
        /// </summary>
        /// <remarks>
        /// Where <c>TryPrune</c> answers <see langword="true"/> it has already marked the data source disposed
        /// and retired its root.
        /// </remarks>
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

        /// <summary>
        /// Stops an entry's timer and disposes its data source.
        /// </summary>
        static void Remove(Entry entry)
        {
            entry.Timer.Dispose();
            entry.DataSource.Dispose();
        }

    }

}
