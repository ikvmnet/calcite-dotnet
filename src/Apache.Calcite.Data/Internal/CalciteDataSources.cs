using System;
using System.Collections.Concurrent;

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
    /// <para>An entry lives until <see cref="Clear"/> or <see cref="ClearAll"/> removes it, or the process
    /// ends. Removal does not dispose it: a connection already opened on it keeps using it, and it goes when
    /// the last such connection lets go of it. Every entry still held at process exit is disposed then, so
    /// that a schema holding a client gets to close it.</para>
    /// </remarks>
    internal static class CalciteDataSources
    {

        static readonly ConcurrentDictionary<string, CalciteDataSource> registered = new();

        static CalciteDataSources()
        {
            AppDomain.CurrentDomain.ProcessExit += (_, _) => DisposeAll();
            AppDomain.CurrentDomain.DomainUnload += (_, _) => DisposeAll();
        }

        /// <summary>
        /// Gets the data source for a connection string, making it if it is new.
        /// </summary>
        /// <param name="options">The connection string.</param>
        /// <returns>The data source.</returns>
        /// <remarks>
        /// An empty connection string names nothing to key on, and two connections written that way have no
        /// reason to meet, so it gets a data source of its own rather than the one every other empty string
        /// would share.
        /// </remarks>
        public static CalciteDataSource Resolve(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (options.Count == 0)
                return new CalciteDataSource(options);

            var key = options.DataSourceKey;
            if (registered.TryGetValue(key, out var existing))
                return existing;

            // Really unseen, need to create a new data source. If someone beats us to it use what they put.
            var created = new CalciteDataSource(new CalciteConnectionStringBuilder(key));
            var winner = registered.GetOrAdd(key, created);
            if (winner != created)
                created.Dispose();

            return winner;
        }

        /// <summary>
        /// Removes the data source for a connection string, so that the next connection opened with it
        /// builds a new one.
        /// </summary>
        /// <param name="options">The connection string.</param>
        public static void Clear(CalciteConnectionStringBuilder options)
        {
            ArgumentNullException.ThrowIfNull(options);

            if (options.Count > 0)
                registered.TryRemove(options.DataSourceKey, out _);
        }

        /// <summary>
        /// Removes every data source, so that the next connection opened with any connection string builds a
        /// new one.
        /// </summary>
        public static void ClearAll()
        {
            registered.Clear();
        }

        static void DisposeAll()
        {
            foreach (var dataSource in registered.Values)
                dataSource.Dispose();
        }

    }

}
