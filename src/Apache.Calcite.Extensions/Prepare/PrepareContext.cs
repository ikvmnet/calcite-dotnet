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

        public RelRunner getRelRunner()
        {
            throw new UnsupportedOperationException();
        }
    }

}
