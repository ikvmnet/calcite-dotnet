using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.core;
using Apache.Calcite.Extensions.Adapter.Cursor;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The <see cref="IAdoCorrelationDataContextBuilder"/> for a plan in <c>ClrCursorConvention</c>. It builds a
    /// <c>System.Linq.Expressions</c> expression that constructs an <see cref="AdoCorrelationDataContext"/> when
    /// the plan runs.
    /// </summary>
    /// <remarks>
    /// Correlation variables are registered on the implementor implementing the plan, so the fields are read
    /// through that implementor's getters; the getters of another implementor do not know the variables. Each
    /// field read is translated from linq4j where the getter produces it. Indexes start at
    /// <see cref="AdoCorrelationDataContext.Offset"/>. One instance serves both the synchronous and the awaiting
    /// body of a node.
    /// </remarks>
    public class AdoClrCorrelationDataContextBuilder : IAdoCorrelationDataContextBuilder
    {

        static readonly System.Reflection.ConstructorInfo Constructor = typeof(AdoCorrelationDataContext).GetConstructor([typeof(org.apache.calcite.DataContext), typeof(object[])])
            ?? throw new InvalidOperationException($"{nameof(AdoCorrelationDataContext)} has no (DataContext, object[]) constructor.");

        readonly List<Expression> _parameters = [];
        readonly Func<string, RexToLixTranslator.InputGetter> _correlVariableGetter;
        readonly LixToClrTranslator _translator;
        readonly Expression _dataContext;

        int offset = AdoCorrelationDataContext.Offset;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="implementor">The implementor of the plan the correlation variables are registered on.</param>
        /// <param name="dataContext">The expression of the context the outer query runs with, which the new context
        /// wraps.</param>
        /// <exception cref="ArgumentNullException"><paramref name="implementor"/> or <paramref name="dataContext"/> is
        /// <see langword="null"/>.</exception>
        public AdoClrCorrelationDataContextBuilder(Apache.Calcite.Extensions.Adapter.Cursor.ClrCursorRelImplementor implementor, Expression dataContext) :
            this(
                (implementor ?? throw new ArgumentNullException(nameof(implementor))).GetCorrelVariableGetter,
                implementor.Translator,
                dataContext)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="correlVariableGetter">Returns the getter a correlation variable was registered with.</param>
        /// <param name="translator">Translates the getter's linq4j field read into an expression.</param>
        /// <param name="dataContext">The expression of the context the outer query runs with.</param>
        /// <exception cref="ArgumentNullException">An argument is <see langword="null"/>.</exception>
        AdoClrCorrelationDataContextBuilder(Func<string, RexToLixTranslator.InputGetter> correlVariableGetter, LixToClrTranslator translator, Expression dataContext)
        {
            _correlVariableGetter = correlVariableGetter ?? throw new ArgumentNullException(nameof(correlVariableGetter));
            _translator = translator ?? throw new ArgumentNullException(nameof(translator));
            _dataContext = dataContext ?? throw new ArgumentNullException(nameof(dataContext));
        }

        /// <inheritdoc />
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type)
        {
            ArgumentNullException.ThrowIfNull(id);

            // this getter declares into the block it was created with and ignores the block argument
            var field = _correlVariableGetter(id.getName()).field(null, ordinal, type);

            _parameters.Add(_translator.Translate(field));
            return offset++;
        }

        /// <summary>
        /// Returns the expression that constructs the context from the registered fields.
        /// </summary>
        /// <returns>The expression.</returns>
        public Expression Build()
        {
            return Expression.New(Constructor, _dataContext, Expression.NewArrayInit(typeof(object), _parameters));
        }

    }

}
