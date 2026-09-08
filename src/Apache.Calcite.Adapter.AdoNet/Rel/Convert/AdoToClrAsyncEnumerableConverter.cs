using System;
using System.Linq.Expressions;
using System.Threading;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Relational operator that converts a tree of <see cref="AdoConvention"/> nodes into a
    /// <see cref="ClrAsyncEnumerableConvention"/> result by executing the generated SQL against the
    /// underlying ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="AdoToClrEnumerableConverter"/>, and it sends the same statement: the
    /// SQL, the parameter enrichment and the row builder are that converter's own, shared rather than
    /// written again. What differs is two calls — <see cref="AdoSequences.ReadAsync{TRow}"/> where the other
    /// names <see cref="AdoSequences.Read{TRow}"/> — and what those give back, which is an
    /// <see cref="System.Collections.Generic.IAsyncEnumerable{T}"/> the operators above can await.
    ///
    /// <para><b>Why the asynchronous convention needs one of these at all.</b> Without it a pushed-down
    /// subtree reaches an asynchronous plan through
    /// <c>EnumerableToClrAsyncEnumerableConverter</c> over <see cref="AdoToEnumerableConverter"/> — two
    /// converters and a linq4j enumerator between the data reader and the plan. Both of those crossings are
    /// honest about costing no thread, and they are right: nothing under them suspends, because the reader
    /// underneath is synchronous. That is the whole problem. The ADO leaf is the one place in a plan where
    /// there is network I/O to suspend on, so a plan that is asynchronous everywhere except there is
    /// asynchronous nowhere that matters.</para>
    ///
    /// <para>Nothing here awaits. <c>Implement</c> runs while the statement is being prepared and builds an
    /// expression tree, as every node of this convention does; the <c>await</c>s are all inside
    /// <see cref="AdoSequences.ReadAsync{TRow}"/>, which is what the tree calls.</para>
    /// </remarks>
    public class AdoToClrAsyncEnumerableConverter : ConverterImpl, ClrAsyncEnumerableRel
    {

        static readonly System.Reflection.MethodInfo ReadAsyncMethod = typeof(AdoSequences).GetMethod(nameof(AdoSequences.ReadAsync))
            ?? throw new InvalidOperationException($"'{nameof(AdoSequences.ReadAsync)}' is missing from {nameof(AdoSequences)}.");

        static readonly System.Reflection.MethodInfo CreateEnricherMethod = typeof(AdoEnumerable).GetMethod(nameof(AdoEnumerable.CreateEnricher), [typeof(AdoDataSource), typeof(java.util.List), typeof(java.util.List), typeof(org.apache.calcite.DataContext)])
            ?? throw new InvalidOperationException($"'{nameof(AdoEnumerable.CreateEnricher)}' is missing from {nameof(AdoEnumerable)}.");

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public AdoToClrAsyncEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new AdoToClrAsyncEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            if (getInput() is not AdoRel self)
                throw new AdoCalciteException("Unsupported input type.");

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var rowType = physType.RowType;

            if (self.getConvention() is not AdoConvention convention)
                throw new AdoCalciteException($"getConvention() is null for {self}.");

            var dataContextBuilder = new AdoClrCorrelationDataContextBuilder(implementor, implementor.Root);

            var writer = AdoToClrEnumerableConverter.GenerateSql(convention, dataContextBuilder, self, (JavaTypeFactory)getCluster().getTypeFactory(), out var sqlImplementor);
            var parameters = writer.Indexes;
            var parameterTypeNames = AdoToEnumerableConverter.GetParameterTypeNames(sqlImplementor, parameters);

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            // the schema SPI defines a convention's expression as linq4j, so this is the one thing here that
            // arrives as a linq4j tree, and it is translated where it is produced rather than composed into
            // anything first. Everything else this node builds is an expression tree from the start.
            var dataSource = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(AdoDataSource)));

            // a correlated sub-query leaves a parameter per correlation variable in the SQL, and the values
            // live on the context the builder closed over the outer row. Without the enricher the command is
            // handed to the provider unfilled.
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            return implementor.Result(physType,
                Expression.Call(null,
                    ReadAsyncMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    AdoToClrEnumerableConverter.RowBuilder(physType, rowType, getRowType().getFieldCount()),
                    enricher,
                    // every operator of this convention ends in a token and is called with default here: the
                    // token the caller passes to GetAsyncEnumerator is what the [EnumeratorCancellation]
                    // parameter receives, and it arrives at enumeration rather than at build time
                    Expression.Default(typeof(CancellationToken))));
        }

    }

}
