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
    /// A native implementation of <see cref="CalcitePrepare.Context"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors the responsibilities of <c>CalciteConnectionImpl.ContextImpl</c> without inheriting
    /// from any JDBC class. This is the boundary the Calcite planner uses to discover schemas,
    /// configuration, and the type factory.
    /// <para>
    /// As there, the constructor stamps the moment of the prepare and takes
    /// <c>createSnapshot(new LongSchemaVersion(now))</c> of the root schema: <c>getRootSchema()</c>
    /// answers the snapshot, and the live root stays reachable only through
    /// <c>getMutableRootSchema()</c>, which is where DDL executes. The snapshot is Calcite's, holes
    /// included — <c>createSnapshot</c> shares the original's table and lattice maps so that
    /// materializations can create tables on the fly, and a <c>Schema</c> is only as frozen as its
    /// own <c>snapshot(version)</c> chooses to be.
    /// </para>
    /// <para>
    /// <c>getDataContext()</c> returns a minimal throwaway <see cref="DataContext"/> containing only
    /// the standard timestamp variables — exactly what <c>CalciteConnectionImpl.ContextImpl</c> does
    /// via <c>createDataContext(ImmutableMap.of(), rootSchema)</c>. It is used solely to back
    /// <c>RexExecutorImpl</c> for constant-folding during optimisation; query parameters and stashed
    /// values are not needed here.
    /// </para>
    /// </remarks>
    internal sealed class PrepareContext : CalcitePrepare.Context
    {

        readonly JavaTypeFactory _typeFactory;
        readonly CalciteSchema _mutableRootSchema;
        readonly CalciteSchema _rootSchema;
        readonly CalciteConnectionConfig _config;
        readonly IReadOnlyList<string> _defaultSchemaPath;

        public PrepareContext(
            JavaTypeFactory typeFactory,
            CalciteSchema rootSchema,
            CalciteConnectionConfig config,
            IReadOnlyList<string> defaultSchemaPath)
        {
            _typeFactory = typeFactory;
            _mutableRootSchema = rootSchema;
            _rootSchema = rootSchema.createSnapshot(new org.apache.calcite.schema.impl.LongSchemaVersion(java.lang.System.currentTimeMillis()));
            _config = config;
            _defaultSchemaPath = defaultSchemaPath;
        }

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

        /// <remarks>
        /// <c>@Nullable</c>, and null is what a context that is not preparing a view returns.
        /// </remarks>
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

        public DataContext getDataContext()
        {
            return new StatementDataContext(_rootSchema.plus(), _typeFactory, new AtomicBoolean(false), 0, [], null);
        }

        /// <summary>
        /// Refuses to run a plan Calcite built itself.
        /// </summary>
        /// <remarks>
        /// <c>CalciteConnectionImpl.ContextImpl.getRelRunner</c> unwraps the connection, the connection
        /// being the runner. There is no connection here, and <c>RelRunner.prepareStatement</c> is declared
        /// to return a <c>java.sql.PreparedStatement</c> — so implementing it means a hundred-odd members
        /// of a JDBC interface this project exists to not have, for the two members
        /// <c>ServerDdlExecutor.populate</c> calls.
        ///
        /// <para><c>populate</c> is the one caller, behind <c>CREATE MATERIALIZED VIEW</c> and
        /// <c>CREATE TABLE ... AS SELECT</c>. Both are refused here, and both are refused
        /// <i>after</i> <c>ServerDdlExecutor</c> has already added the table to the schema — that ordering
        /// is upstream's and this cannot change it. The planning half is not the obstacle:
        /// <c>ClrPrepareImpl.PrepareRel</c> is the <c>prepare2_</c> branch Calcite's own runner uses, and
        /// is ready for a runner that wants it.</para>
        /// </remarks>
        public RelRunner getRelRunner()
        {
            throw new UnsupportedOperationException(
                "CREATE MATERIALIZED VIEW and CREATE TABLE ... AS SELECT are not supported: "
                + "ServerDdlExecutor.populate loads their rows through a java.sql.PreparedStatement, "
                + "which this provider does not implement. The table has already been created.");
        }
    }

}
