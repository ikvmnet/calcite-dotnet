using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

using System;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// A <see cref="DataContext"/> that supplies the outer row's values to a pushed-down statement that reads a
    /// correlation variable, and passes every other lookup to the context it wraps.
    /// </summary>
    /// <remarks>
    /// The correlation variable's fields are written into the statement as dynamic parameters numbered from
    /// <see cref="Offset"/>, far above any index a query's own parameters use. <see cref="get"/> answers
    /// <c>?</c> followed by such an index from the array given to the constructor.
    /// </remarks>
    public class AdoCorrelationDataContext : DataContext
    {

        /// <summary>
        /// The index of the first correlation parameter. The value of the parameter at <c>Offset + i</c> is
        /// element <c>i</c> of the array given to the constructor.
        /// </summary>
        public const int Offset = int.MaxValue - 10000;

        readonly DataContext _delegate;
        readonly object[] _parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="delegate">The context every other lookup goes to.</param>
        /// <param name="parameters">The outer row's values, in the order their indexes were assigned from
        /// <see cref="Offset"/>.</param>
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

        /// <summary>
        /// Returns the outer row's value for <c>?</c> followed by an index in the range this context holds, and
        /// otherwise the wrapped context's value for <paramref name="name"/>.
        /// </summary>
        /// <param name="name">The variable's name.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public object? get(string name)
        {
            if (name.StartsWith('?') && int.TryParse(name.AsSpan(1), out var index))
                if (index >= Offset && index < Offset + _parameters.Length)
                    return _parameters[index - Offset];

            return _delegate.get(name);
        }

    }

}
