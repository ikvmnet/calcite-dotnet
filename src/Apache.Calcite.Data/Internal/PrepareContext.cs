using System.Collections.Generic;

using java.lang;
using java.util;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.tools;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// A native implementation of <see cref="CalcitePrepare.Context"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors the responsibilities of <c>CalciteConnectionImpl.ContextImpl</c> without inheriting
    /// from any JDBC class. This is the boundary the Calcite planner uses to discover schemas,
    /// configuration, and the type factory.
    /// </remarks>
    internal sealed class PrepareContext : CalcitePrepare.Context
    {

        readonly JavaTypeFactory _typeFactory;
        readonly CalciteSchema _rootSchema;
        readonly CalciteConnectionConfig _config;
        readonly DataContext _dataContext;
        readonly IReadOnlyList<string> _defaultSchemaPath;

        public PrepareContext(
            JavaTypeFactory typeFactory,
            CalciteSchema rootSchema,
            CalciteConnectionConfig config,
            DataContext dataContext,
            IReadOnlyList<string> defaultSchemaPath)
        {
            _typeFactory = typeFactory;
            _rootSchema = rootSchema;
            _config = config;
            _dataContext = dataContext;
            _defaultSchemaPath = defaultSchemaPath;
        }

        public JavaTypeFactory getTypeFactory() => _typeFactory;

        public CalciteSchema getRootSchema() => _rootSchema;

        public CalciteSchema getMutableRootSchema() => _rootSchema;

        public List getDefaultSchemaPath()
        {
            var list = new ArrayList(_defaultSchemaPath.Count);
            foreach (var s in _defaultSchemaPath)
                list.add(s);
            return list;
        }

        public List getObjectPath() => null!;

        public CalciteConnectionConfig config() => _config;

        public CalcitePrepare.SparkHandler spark() =>
            CalcitePrepare.Dummy.getSparkHandler(false);

        public DataContext getDataContext() => _dataContext;

        public RelRunner getRelRunner() => throw new UnsupportedOperationException();

    }

}
