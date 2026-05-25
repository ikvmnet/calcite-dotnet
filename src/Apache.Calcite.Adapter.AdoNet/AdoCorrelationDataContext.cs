using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A <see cref="DataContext"/> that injects correlation variables into a nested-loop join execution.
    /// </summary>
    /// <remarks>
    /// During batch nested-loop join execution the planner inserts correlation variables into the inner
    /// side of the join. This context intercepts variable lookups whose index is at or above
    /// <see cref="Offset"/> and resolves them from the parameter array supplied at construction time,
    /// delegating all other lookups to the wrapped <see cref="DataContext"/>.
    /// </remarks>
    public class AdoCorrelationDataContext : DataContext
    {

        /// <summary>
        /// Index threshold above which a variable lookup is treated as a correlation variable.
        /// Variable indices in the range <c>[<see cref="Offset"/>, int.MaxValue]</c> are resolved
        /// from the parameter array passed to the constructor rather than the delegate context.
        /// </summary>
        public const int Offset = int.MaxValue - 10000;

        readonly DataContext _delegate;
        readonly object[] _parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="delegate">The underlying <see cref="DataContext"/> to which non-correlation variable lookups are forwarded.</param>
        /// <param name="parameters">The correlation variable values, indexed relative to <see cref="Offset"/>.</param>
        public AdoCorrelationDataContext(DataContext @delegate, object[] parameters)
        {
            _delegate = @delegate;
            _parameters = parameters;
        }

        /// <inheritdoc />
        public SchemaPlus getRootSchema()
        {
            return _delegate.getRootSchema();
        }

        /// <inheritdoc />
        public JavaTypeFactory getTypeFactory()
        {
            return _delegate.getTypeFactory();
        }

        /// <inheritdoc />
        public QueryProvider getQueryProvider()
        {
            return _delegate.getQueryProvider();
        }

        /// <inheritdoc />
        public object? get(string name)
        {
            return _delegate.get(name);
        }

    }

}
