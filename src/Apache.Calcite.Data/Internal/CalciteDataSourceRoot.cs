using System;
using System.Collections.Generic;
using System.Threading;

using com.google.common.collect;

using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The root schemas of one data source: what a connection string and a <see cref="CalciteDataSourceBuilder"/>
    /// describe, and the roots assembled from it for the data source's connections.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's JDBC connection builds its root and <c>DUAL</c> in <c>CalciteConnectionImpl</c>'s constructor
    /// and then applies the model in the driver's <c>onConnectionInit</c>, once per connection.
    /// <see cref="Assemble"/> runs the same steps in the same order, so a model can replace <c>DUAL</c>, and
    /// then the builder's steps. The model is read once, by <see cref="Build"/>, and what its factories make
    /// is made once (see <see cref="CalciteModelAssembler"/>), so the schema objects an adapter holds are
    /// shared by every root; everything else a root holds is its own.
    /// </para>
    /// <para>
    /// How sessions use roots depends on <see cref="Sharing"/>. An <see cref="CalciteRootSharing.Isolated"/>
    /// data source gives each session a root no other session holds: a session takes one with
    /// <see cref="Rent"/> and gives it back with <see cref="Return"/>. One that DDL altered, or that still
    /// holds a table at its root it was not assembled with, is dropped; a clean one is kept for the next
    /// session until it has been idle for the idle lifetime. The other modes hand every session the one root.
    /// </para>
    /// <para>
    /// A schema that implements <see cref="IDisposable"/> is disposed when nothing can reach it again. One the
    /// data source owns, that is one the model's factories made or that was on the first root assembled, is
    /// disposed when the data source's root is released; any other is disposed with the root that holds it.
    /// </para>
    /// </remarks>
    internal sealed class CalciteDataSourceRoot
    {

        /// <summary>
        /// A root schema handed to a session, with the root-level tables it was assembled with.
        /// </summary>
        public sealed class Lease
        {

            internal Lease(CalciteSchema schema)
            {
                Schema = schema;
                Tables = RootTables(schema);
            }

            /// <summary>
            /// Gets the root schema.
            /// </summary>
            public CalciteSchema Schema { get; }

            /// <summary>
            /// Gets the tables at the root, by name, as they were when the root was assembled.
            /// </summary>
            internal IReadOnlyDictionary<string, object?> Tables { get; }

            /// <summary>
            /// Returns whether the root holds exactly the tables it was assembled with, by name and by table.
            /// </summary>
            /// <returns><see langword="false"/> where a table was added at the root, removed or replaced.</returns>
            /// <remarks>
            /// A recursive query registers its working table at the root and removes it when the query is
            /// closed, so a root whose statement was abandoned still holds it.
            /// </remarks>
            internal bool Unchanged()
            {
                var now = RootTables(Schema);
                if (now.Count != Tables.Count)
                    return false;

                foreach (var (name, table) in now)
                    if (Tables.TryGetValue(name, out var before) == false || ReferenceEquals(before, table) == false)
                        return false;

                return true;
            }

            static Dictionary<string, object?> RootTables(CalciteSchema schema)
            {
                // the root's own schema defines no tables, so these are the ones added to it
                var tables = new Dictionary<string, object?>(StringComparer.Ordinal);
                for (var it = schema.getTableNames().iterator(); it.hasNext();)
                {
                    var name = (string)it.next();
                    tables[name] = schema.getTable(name, true)?.getTable();
                }

                return tables;
            }

        }

        readonly bool _dual;
        readonly CalciteModelAssembler? _assembler;
        readonly IReadOnlyList<Action<SchemaPlus>> _configure;
        readonly string? _defaultSchemaName;
        readonly CalciteRootSharing _sharing;
        readonly long _idleLifetimeMilliseconds;
        readonly Lease _first;
        readonly HashSet<object> _owned = new(ReferenceEqualityComparer.Instance);
        readonly List<(Lease Lease, long ReturnedAt)> _idle = [];
        readonly object _sync = new();
        int _users;
        bool _retired;
        bool _disposed;
        long _idleSince = Environment.TickCount64;

        CalciteDataSourceRoot(bool dual, CalciteModel? model, IReadOnlyList<Action<SchemaPlus>> configure, CalciteSchema? rootSchema, CalciteRootSharing sharing, TimeSpan idleLifetime)
        {
            _dual = dual;
            _assembler = model is null ? null : new CalciteModelAssembler(model);
            _configure = configure;
            _defaultSchemaName = model?.DefaultSchemaName;
            _sharing = sharing;
            _idleLifetimeMilliseconds = (long)idleLifetime.TotalMilliseconds;

            _first = Assemble(rootSchema);

            // the data source owns what its first root was assembled with: the model's schemas, and whatever the
            // builder's steps added, which may be one instance they add to every root
            for (var it = _first.Schema.getSubSchemaMap().values().iterator(); it.hasNext();)
                _owned.Add(((CalciteSchema)it.next()).schema);
            if (_assembler is not null)
                foreach (var schema in _assembler.Schemas)
                    _owned.Add(schema);

            if (_sharing == CalciteRootSharing.Isolated)
                _idle.Add((_first, Environment.TickCount64));
        }

        /// <summary>
        /// Reads the model and assembles the first root.
        /// </summary>
        /// <param name="options">The connection string options.</param>
        /// <param name="configure">Steps to run over each root after the model, in order.</param>
        /// <param name="rootSchema">Root schema to assemble the first root on, or <see langword="null"/> to
        /// create one. <c>DUAL</c> and the model are added to it either way, as Calcite does with a root handed
        /// to its connection.</param>
        /// <param name="sharing">How sessions use roots.</param>
        /// <param name="idleLifetime">How long an <see cref="CalciteRootSharing.Isolated"/> data source keeps a
        /// root no session is using.</param>
        /// <returns>The data source's roots.</returns>
        /// <exception cref="CalciteException">Thrown when the model could not be read or applied, or the root
        /// could not be built.</exception>
        public static CalciteDataSourceRoot Build(CalciteConnectionStringBuilder options, IReadOnlyList<Action<SchemaPlus>> configure, CalciteSchema? rootSchema = null, CalciteRootSharing sharing = CalciteRootSharing.Owned, TimeSpan idleLifetime = default)
        {
            ArgumentNullException.ThrowIfNull(options);
            ArgumentNullException.ThrowIfNull(configure);

            try
            {
                var cfg = new CalciteConnectionConfigImpl(CalciteEngineProperties.Build(options));
                var model = CalciteModel.Resolve(options, cfg);
                return new CalciteDataSourceRoot(cfg.conformance().isSupportedDualTable(), model, configure, rootSchema, sharing, idleLifetime);
            }
            catch (Exception e) when (e is not CalciteException)
            {
                throw new CalciteException("Failed to initialize Calcite.", e);
            }
        }

        /// <summary>
        /// Assembles a root: Calcite's root, <c>DUAL</c> where the conformance has one, the model, and the
        /// builder's steps.
        /// </summary>
        /// <param name="root">The root to assemble on, or <see langword="null"/> to create one.</param>
        /// <returns>The root.</returns>
        /// <exception cref="CalciteException">The model could not be applied.</exception>
        Lease Assemble(CalciteSchema? root = null)
        {
            root ??= CalciteSchema.createRootSchema(true);

            if (_dual)
            {
                SchemaPlus schemaPlus = root.plus();
                // Dual table contains one row with a value X
                schemaPlus.add(
                    "DUAL", ViewTable.viewMacro(schemaPlus, "VALUES ('X')",
                    ImmutableList.of(), null, java.lang.Boolean.valueOf(false)));
            }

            // the model after DUAL, as Calcite's driver applies it after the connection's constructor
            if (_assembler is not null)
            {
                try
                {
                    _assembler.Apply(root.plus());
                }
                catch (Exception e) when (e is not CalciteException)
                {
                    throw new CalciteException("Failed to load Calcite model.", e);
                }
            }

            foreach (var step in _configure)
                step(root.plus());

            return new Lease(root);
        }

        /// <summary>
        /// Gets how sessions use roots.
        /// </summary>
        public CalciteRootSharing Sharing => _sharing;

        /// <summary>
        /// Gets the default schema the model named, or <see langword="null"/> where it named none.
        /// </summary>
        public string? DefaultSchemaName => _defaultSchemaName;

        /// <summary>
        /// Gets the lock a statement plans under and DDL alters a <see cref="CalciteRootSharing.Shared"/> root
        /// under.
        /// </summary>
        /// <remarks>
        /// Calcite does not guard a root against concurrent use: a <c>CalciteSchema</c> keeps its tables and
        /// sub-schemas in <c>NameMap</c>s over <c>TreeMap</c>s, and DDL writes into them. Planning (snapshot,
        /// validation, optimization, implementation) takes the read lock, so many statements plan at once;
        /// DDL takes the write lock. The lock is thread-affine, so it covers planning, which runs on one
        /// thread, and not execution, which may resume on others across awaits. A running plan resolves
        /// tables again from its snapshot, and the snapshot shares its table map with the live root, so a
        /// table lookup during execution is not protected against concurrent DDL, and a recursive query's
        /// working table, which execution registers at the root by name, is not protected at all. Only a
        /// shared root is locked; the other modes give each session a root no other session uses.
        /// </remarks>
        public ReaderWriterLockSlim Lock { get; } = new(LockRecursionPolicy.SupportsRecursion);

        /// <summary>
        /// Gets the number of sessions holding this data source's roots.
        /// </summary>
        public int Users
        {
            get { lock (_sync) return _users; }
        }

        /// <summary>
        /// Gets the <see cref="Environment.TickCount64"/> at which no session last held a root: construction,
        /// or the last <see cref="Release"/> that left no session holding one.
        /// </summary>
        public long IdleSince
        {
            get { lock (_sync) return _idleSince; }
        }

        /// <summary>
        /// Returns the root a session plans against: for an <see cref="CalciteRootSharing.Isolated"/> data
        /// source, one no other session holds, otherwise the one root.
        /// </summary>
        /// <returns>The root.</returns>
        /// <exception cref="CalciteException">A new root could not be assembled.</exception>
        /// <remarks>
        /// An isolated root is the most recently returned one that has not been idle for the idle lifetime, or a
        /// new one, assembled outside the lock.
        /// </remarks>
        public Lease Rent()
        {
            if (_sharing != CalciteRootSharing.Isolated)
                return _first;

            List<Lease> expired;
            Lease? lease = null;
            lock (_sync)
            {
                expired = Trim(Environment.TickCount64);
                if (_idle.Count > 0)
                {
                    lease = _idle[^1].Lease;
                    _idle.RemoveAt(_idle.Count - 1);
                }
            }

            foreach (var e in expired)
                Drop(e);

            return lease ?? Assemble();
        }

        /// <summary>
        /// Takes back a root <see cref="Rent"/> handed out, keeping it for the next session where it is as it
        /// was assembled.
        /// </summary>
        /// <param name="lease">The root.</param>
        /// <param name="altered">Whether DDL was run against it.</param>
        /// <remarks>
        /// A root that DDL altered, or that holds a table at its root it was not assembled with, is dropped,
        /// as is one returned after the data source's root was retired.
        /// </remarks>
        public void Return(Lease lease, bool altered)
        {
            ArgumentNullException.ThrowIfNull(lease);

            if (_sharing != CalciteRootSharing.Isolated)
                return;

            if (altered == false && lease.Unchanged())
            {
                lock (_sync)
                {
                    if (_retired == false && _disposed == false)
                    {
                        _idle.Add((lease, Environment.TickCount64));
                        return;
                    }
                }
            }

            Drop(lease);
        }

        /// <summary>
        /// Removes the roots that have been idle for the idle lifetime, and returns them to be dropped outside
        /// the lock. Called under <c>_sync</c>.
        /// </summary>
        /// <param name="now">The current <see cref="Environment.TickCount64"/>.</param>
        /// <returns>The roots removed, oldest first.</returns>
        List<Lease> Trim(long now)
        {
            var expired = new List<Lease>();
            while (expired.Count < _idle.Count && now - _idle[expired.Count].ReturnedAt >= _idleLifetimeMilliseconds)
                expired.Add(_idle[expired.Count].Lease);

            _idle.RemoveRange(0, expired.Count);
            return expired;
        }

        /// <summary>
        /// Disposes the schemas a root holds that the data source does not own: those DDL created on it, and
        /// those the builder's steps made for it alone.
        /// </summary>
        /// <param name="lease">The root no session will be given again.</param>
        /// <remarks>
        /// Only the root's own sub-schemas are looked at, and an owned one is not descended into, since what it
        /// holds is the data source's. A schema DDL created inside an owned schema stays until the data source's
        /// root is released.
        /// </remarks>
        void Drop(Lease lease)
        {
            var done = new HashSet<object>(ReferenceEqualityComparer.Instance);
            for (var it = lease.Schema.getSubSchemaMap().values().iterator(); it.hasNext();)
            {
                var sub = (CalciteSchema)it.next();
                if (_owned.Contains(sub.schema) == false)
                    DisposeSchemas(sub, done);
            }
        }

        /// <summary>
        /// Counts a session onto this data source's roots.
        /// </summary>
        public void Acquire()
        {
            lock (_sync)
                _users++;
        }

        /// <summary>
        /// Counts a session off this data source's roots, disposing them where they have been retired and this
        /// was the last.
        /// </summary>
        public void Release()
        {
            bool dispose;
            lock (_sync)
            {
                _users--;
                if (_users == 0)
                    _idleSince = Environment.TickCount64;

                dispose = _users == 0 && _retired;
            }

            if (dispose)
                Dispose();
        }

        /// <summary>
        /// Marks these roots as no longer wanted, disposing them now where no session holds one and otherwise
        /// when the last session releases them.
        /// </summary>
        /// <remarks>
        /// A data source retires its roots on <see cref="CalciteDataSource.Clear"/>, on disposal, and when the
        /// provider evicts or prunes it; a session retires a root built for it alone. Connections still open
        /// on a root keep working until they are disposed.
        /// </remarks>
        public void Retire()
        {
            bool dispose;
            lock (_sync)
            {
                _retired = true;
                dispose = _users == 0;
            }

            if (dispose)
                Dispose();
        }

        /// <summary>
        /// Disposes every schema the roots hold that can be disposed, sub-schemas first.
        /// </summary>
        /// <remarks>
        /// Calcite has no disposal hook for a schema. A schema that implements <see cref="IDisposable"/> is
        /// disposed here, when the roots have been <see cref="Retire">retired</see> and no session holds one, so
        /// an adapter holding a client can release it. A shared or owned root is disposed whole; an isolated
        /// data source drops each root it still keeps, then disposes what it owns.
        /// </remarks>
        void Dispose()
        {
            List<(Lease Lease, long ReturnedAt)> idle;
            lock (_sync)
            {
                if (_disposed)
                    return;

                _disposed = true;
                idle = [.. _idle];
                _idle.Clear();
            }

            var done = new HashSet<object>(ReferenceEqualityComparer.Instance);
            if (_sharing == CalciteRootSharing.Isolated)
            {
                foreach (var (lease, _) in idle)
                    Drop(lease);

                for (var it = _first.Schema.getSubSchemaMap().values().iterator(); it.hasNext();)
                {
                    var sub = (CalciteSchema)it.next();
                    if (_owned.Contains(sub.schema))
                        DisposeSchemas(sub, done);
                }
            }
            else
            {
                DisposeSchemas(_first.Schema, done);
            }

            // a schema a factory made that a step then replaced is on no root
            if (_assembler is not null)
                foreach (var schema in _assembler.Schemas)
                    if (done.Add(schema) && schema is IDisposable disposable)
                        disposable.Dispose();

            Lock.Dispose();
        }

        static void DisposeSchemas(CalciteSchema schema, HashSet<object> done)
        {
            for (var it = schema.getSubSchemaMap().values().iterator(); it.hasNext();)
                DisposeSchemas((CalciteSchema)it.next(), done);

            if (done.Add(schema.schema) && schema.schema is IDisposable disposable)
                disposable.Dispose();
        }

    }

}
