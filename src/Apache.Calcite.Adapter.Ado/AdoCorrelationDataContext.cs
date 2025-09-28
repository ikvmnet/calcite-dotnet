using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// A special <see cref="DataContext"/> which handles correlation variable for batch nested loop joins.
    /// </summary>
    class AdoCorrelationDataContext : DataContext
    {

        public const int OFFSET = int.MaxValue - 10000;

        readonly DataContext _delegate;
        readonly object[] _parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="delegate"></param>
        /// <param name="parameters"></param>
        public AdoCorrelationDataContext(DataContext @delegate, object[] parameters)
        {
            _delegate = @delegate;
            _parameters = parameters;
        }

        public SchemaPlus getRootSchema()
        {
            return _delegate.getRootSchema();
        }

        public JavaTypeFactory getTypeFactory()
        {
            return _delegate.getTypeFactory();
        }

        public QueryProvider getQueryProvider()
        {
            return _delegate.getQueryProvider();
        }

        public object? get(string name)
        {
            return _delegate.get(name);
        }

    }

}
