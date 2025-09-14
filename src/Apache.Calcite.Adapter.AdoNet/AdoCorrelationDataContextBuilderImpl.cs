using com.google.common.collect;

using java.lang.reflect;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    public class AdoCorrelationDataContextBuilderImpl : IAdoCorrelationDataContextBuilder
    {

        static readonly Constructor NEW = Types.lookupConstructor((java.lang.reflect.Type)typeof(AdoCorrelationDataContext), typeof(DataContext), typeof(object[]));

        readonly ImmutableList.Builder parameters = ImmutableList.builder();
        readonly EnumerableRelImplementor implementor;
        readonly BlockBuilder builder;
        readonly Expression dataContext;

        int offset = AdoCorrelationDataContext.OFFSET;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="builder"></param>
        /// <param name="dataContext"></param>
        public AdoCorrelationDataContextBuilderImpl(EnumerableRelImplementor implementor, BlockBuilder builder, Expression dataContext)
        {
            this.implementor = implementor;
            this.builder = builder;
            this.dataContext = dataContext;
        }

        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type)
        {
            parameters.add(implementor.getCorrelVariableGetter(id.getName()).field(builder, ordinal, type));
            return offset++;
        }

        public Expression Build()
        {
            return Expressions.new_(NEW, dataContext, Expressions.newArrayInit((java.lang.reflect.Type)typeof(object), 1, parameters.build()));
        }

    }

}
