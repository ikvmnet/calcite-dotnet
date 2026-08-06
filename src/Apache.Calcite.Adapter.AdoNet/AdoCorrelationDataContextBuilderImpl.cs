using System;

using com.google.common.collect;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Default implementation of <see cref="IAdoCorrelationDataContextBuilder"/> that
    /// generates code to construct an <see cref="AdoCorrelationDataContext"/> at plan execution time.
    /// </summary>
    public class AdoCorrelationDataContextBuilderImpl : IAdoCorrelationDataContextBuilder
    {

        // (Class) and not (java.lang.reflect.Type): IKVM converts a System.Type to a java.lang.Class, but a
        // cast to the interface Class implements is a plain runtime cast, and a System.RuntimeType does not
        // implement it. This threw for as long as it existed, unnoticed because C# defers a static field
        // until it is first read and only Build below reads this one.
        static readonly java.lang.reflect.Constructor NEW = Types.lookupConstructor((java.lang.Class)typeof(AdoCorrelationDataContext), typeof(DataContext), typeof(object[]));

        readonly ImmutableList.Builder _parameters = ImmutableList.builder();
        readonly EnumerableRelImplementor _implementor;
        readonly BlockBuilder _builder;
        readonly Expression _dataContext;

        int offset = AdoCorrelationDataContext.Offset;

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
            return Expressions.new_(NEW, _dataContext, Expressions.newArrayInit((java.lang.Class)typeof(object), 1, _parameters.build()));
        }

    }

}
