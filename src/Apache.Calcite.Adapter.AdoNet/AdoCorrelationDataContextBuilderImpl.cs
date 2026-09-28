using System;

using com.google.common.collect;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The <see cref="IAdoCorrelationDataContextBuilder"/> for a plan in Calcite's <c>EnumerableConvention</c>. It
    /// generates linq4j code that constructs an <see cref="AdoCorrelationDataContext"/> when the plan runs.
    /// </summary>
    /// <remarks>
    /// Each registered field is read through the implementor's getter for its correlation variable, which declares
    /// the read into the builder's block. Indexes start at <see cref="AdoCorrelationDataContext.Offset"/>.
    /// </remarks>
    public class AdoCorrelationDataContextBuilderImpl : IAdoCorrelationDataContextBuilder
    {

        // (Class), never (java.lang.reflect.Type): IKVM defines a conversion from System.Type to java.lang.Class,
        // but a cast to the Type interface is a plain runtime cast, which a System.Type fails
        static readonly java.lang.reflect.Constructor NEW = Types.lookupConstructor((java.lang.Class)typeof(AdoCorrelationDataContext), typeof(DataContext), typeof(object[]));

        readonly ImmutableList.Builder _parameters = ImmutableList.builder();
        readonly EnumerableRelImplementor _implementor;
        readonly BlockBuilder _builder;
        readonly Expression _dataContext;

        int offset = AdoCorrelationDataContext.Offset;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="implementor">The implementor the correlation variables are registered on.</param>
        /// <param name="builder">The block the field reads are declared into.</param>
        /// <param name="dataContext">The expression of the context the plan runs with, which the new context wraps.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        public AdoCorrelationDataContextBuilderImpl(EnumerableRelImplementor implementor, BlockBuilder builder, Expression dataContext)
        {
            _implementor = implementor ?? throw new ArgumentNullException(nameof(implementor));
            _builder = builder ?? throw new ArgumentNullException(nameof(builder));
            _dataContext = dataContext ?? throw new ArgumentNullException(nameof(dataContext));
        }

        /// <inheritdoc />
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type)
        {
            _parameters.add(_implementor.getCorrelVariableGetter(id.getName()).field(_builder, ordinal, type));
            return offset++;
        }

        /// <summary>
        /// Returns the linq4j expression that constructs the context from the registered fields.
        /// </summary>
        /// <returns>The expression.</returns>
        public Expression Build()
        {
            return Expressions.new_(NEW, _dataContext, Expressions.newArrayInit((java.lang.Class)typeof(object), 1, _parameters.build()));
        }

    }

}
