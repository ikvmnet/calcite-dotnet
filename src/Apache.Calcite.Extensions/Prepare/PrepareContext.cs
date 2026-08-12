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
            return new StatementDataContext(_rootSchema, _typeFactory, _config, _defaultSchemaPath, new AtomicBoolean(false), 0, [], null);
        }

        /// <summary>
        /// Refuses to run a plan Calcite built itself.
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
