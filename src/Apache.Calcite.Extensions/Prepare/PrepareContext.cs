using System.Collections.Generic;

using java.lang;
using java.util;
using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.tools;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// The <see cref="CalcitePrepare.Context"/> a statement is prepared in.
    /// </summary>
    internal sealed class PrepareContext : CalcitePrepare.Context
    {

        readonly JavaTypeFactory _typeFactory;
        readonly CalciteSchema _mutableRootSchema;
        readonly CalciteSchema _rootSchema;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;
        readonly System.Threading.ReaderWriterLockSlim? _rootLock;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="rootSchema">The mutable root schema. The constructor takes a snapshot of it, so a
        /// caller that shares the root between connections holds <paramref name="rootLock"/>'s read lock
        /// across the call.</param>
        /// <param name="config">The connection configuration.</param>
        /// <param name="defaultSchemaPath">The default schema path.</param>
        /// <param name="rootLock">The lock under which a shared root is read and altered, or
        /// <see langword="null"/> if one connection owns the root.</param>
        public PrepareContext(
            JavaTypeFactory typeFactory,
            CalciteSchema rootSchema,
            CalciteConnectionConfig config,
            IReadOnlyList<string> defaultSchemaPath,
            System.Threading.ReaderWriterLockSlim? rootLock = null)
        {
            _typeFactory = typeFactory;
            _mutableRootSchema = rootSchema;
            _rootSchema = rootSchema.createSnapshot(new org.apache.calcite.schema.impl.LongSchemaVersion(java.lang.System.currentTimeMillis()));
            _config = config;
            _defaultSchemaPath = defaultSchemaPath;
            _rootLock = rootLock;
        }

        /// <summary>
        /// Gets the lock under which the root is read and altered, or <see langword="null"/> if there is none.
        /// </summary>
        public System.Threading.ReaderWriterLockSlim? RootLock => _rootLock;

        public JavaTypeFactory getTypeFactory()
        {
            return _typeFactory;
        }

        public CalciteSchema getRootSchema()
        {
            return _rootSchema;
        }

        public CalciteSchema getMutableRootSchema()
        {
            return _mutableRootSchema;
        }

        public List getDefaultSchemaPath()
        {
            var list = new ArrayList(_defaultSchemaPath.Count);
            foreach (var s in _defaultSchemaPath)
                list.add(s);

            return list;
        }

        public List? getObjectPath()
        {
            return null;
        }

        public CalciteConnectionConfig config()
        {
            return _config;
        }

        public CalcitePrepare.SparkHandler spark()
        {
            return CalcitePrepare.Dummy.getSparkHandler(false);
        }

        /// <summary>
        /// Returns a data context over the snapshot root for use during planning.
        /// </summary>
        /// <remarks>
        /// The context carries no cancellation, timeout or parameter values; the context a statement executes
        /// against is created separately, when it is bound.
        /// </remarks>
        public DataContext getDataContext()
        {
            return new StatementDataContext(_rootSchema, _typeFactory, _config, _defaultSchemaPath, System.Threading.CancellationToken.None, 0, [], null);
        }

        /// <summary>
        /// Throws: running a plan through a <see cref="RelRunner"/> is not supported, so neither is
        /// <c>CREATE MATERIALIZED VIEW</c> nor <c>CREATE TABLE ... AS SELECT</c>.
        /// </summary>
        public RelRunner getRelRunner()
        {
            throw new UnsupportedOperationException(
                "CREATE MATERIALIZED VIEW and CREATE TABLE ... AS SELECT are not supported: "
                + "ServerDdlExecutor.populate loads their rows through a java.sql.PreparedStatement, "
                + "which this provider does not implement. The table has already been created.");
        }
    }

}
