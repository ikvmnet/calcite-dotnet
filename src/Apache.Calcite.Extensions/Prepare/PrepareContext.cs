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
            _rootSchema = rootSchema;
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
            return _rootSchema;
        }

        public List getDefaultSchemaPath()
        {
            var list = new ArrayList(_defaultSchemaPath.Count);
            foreach (var s in _defaultSchemaPath)
                list.add(s);

            return list;
        }

        public List getObjectPath()
        {
            return null!;
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
        /// Returns the runner Calcite uses to execute a plan it built itself.
        /// </summary>
        /// <remarks>
        /// <c>CalciteConnectionImpl.ContextImpl.getRelRunner</c>, which unwraps the connection — the
        /// connection <i>is</i> the runner there. There is no connection here, so this is
        /// <see cref="ClrRelRunner"/>, which plans through <c>ClrPrepareImpl.PrepareRel</c>: the same
        /// <c>prepare2_</c> branch Calcite's runner uses.
        ///
        /// <para>The one caller is <c>ServerDdlExecutor.populate</c>, behind
        /// <c>CREATE MATERIALIZED VIEW</c> and <c>CREATE TABLE ... AS SELECT</c>.</para>
        /// </remarks>
        public RelRunner getRelRunner()
        {
            return new ClrRelRunner(this);
        }
    }

}
