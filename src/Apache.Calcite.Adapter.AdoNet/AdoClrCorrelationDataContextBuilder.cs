using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq;

using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Collects the correlation variables needed to construct an <see cref="AdoCorrelationDataContext"/> for a
    /// correlated sub-query, for a plan of the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// What <see cref="AdoCorrelationDataContextBuilderImpl"/> does for a plan of Calcite's convention. Two
    /// things differ, and the first is why this exists at all: a correlation variable is registered on the
    /// implementor that is implementing the plan and is unknown to every other, so the variable has to be
    /// read from this convention's implementor rather than Calcite's. Handing over the wrong one does not
    /// fail while planning — it fails looking the variable up.
    ///
    /// <para>The second is that nothing here is linq4j. The other builds a linq4j tree and declares its field
    /// reads into a block; this reads each field as an expression and builds the context directly, so no
    /// block is needed and nothing is left to translate.</para>
    /// </remarks>
    public class AdoClrCorrelationDataContextBuilder : IAdoCorrelationDataContextBuilder
    {

        static readonly System.Reflection.ConstructorInfo Constructor = typeof(AdoCorrelationDataContext).GetConstructor([typeof(org.apache.calcite.DataContext), typeof(object[])])
            ?? throw new InvalidOperationException($"{nameof(AdoCorrelationDataContext)} has no (DataContext, object[]) constructor.");

        readonly List<Expression> _parameters = [];
        readonly ClrEnumerableRelImplementor _implementor;
        readonly Expression _dataContext;

        int offset = AdoCorrelationDataContext.Offset;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="implementor">The implementor of the plan the correlation variables are registered on.</param>
        /// <param name="dataContext">The context the outer query was bound with.</param>
        /// <exception cref="ArgumentNullException"></exception>
        public AdoClrCorrelationDataContextBuilder(ClrEnumerableRelImplementor implementor, Expression dataContext)
        {
            _implementor = implementor ?? throw new ArgumentNullException(nameof(implementor));
            _dataContext = dataContext ?? throw new ArgumentNullException(nameof(dataContext));
        }

        /// <inheritdoc />
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type)
        {
            ArgumentNullException.ThrowIfNull(id);

            // the getter reads the field as linq4j and declares into the block it was created with, not one
            // passed to it, so there is nothing for a block of this builder's to receive
            var field = _implementor.GetCorrelVariableGetter(id.getName()).field(null, ordinal, type);

            _parameters.Add(_implementor.Translator.Translate(field));
            return offset++;
        }

        /// <summary>
        /// Returns the expression constructing the context the sub-query reads its correlated values from.
        /// </summary>
        /// <returns></returns>
        public Expression Build()
        {
            return Expression.New(Constructor, _dataContext, Expression.NewArrayInit(typeof(object), _parameters));
        }

    }

}
