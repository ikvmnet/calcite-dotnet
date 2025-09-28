using System;

using com.google.common.collect;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An implementation of <see cref="IAdoCorrelationDataContextBuilder"/>.
    /// </summary>
    public class AdoCorrelationDataContextBuilderImpl : IAdoCorrelationDataContextBuilder
    {

        static readonly java.lang.reflect.Constructor NEW = Types.lookupConstructor((java.lang.reflect.Type)typeof(AdoCorrelationDataContext), typeof(DataContext), typeof(object[]));

        readonly ImmutableList.Builder _parameters = ImmutableList.builder();
        readonly EnumerableRelImplementor _implementor;
        readonly BlockBuilder _builder;
        readonly Expression _dataContext;

        int offset = AdoCorrelationDataContext.OFFSET;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="builder"></param>
        /// <param name="dataContext"></param>
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

        /// <inheritdoc />
        public Expression Build()
        {
            return Expressions.new_(NEW, _dataContext, Expressions.newArrayInit((java.lang.reflect.Type)typeof(object), 1, _parameters.build()));
        }

    }

}
