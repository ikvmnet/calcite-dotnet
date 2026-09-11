using System;
using System.Linq.Expressions;

using Apache.Calcite.Data.Types;
using Apache.Calcite.Extensions.Adapter.AsyncEnumerable;
using Apache.Calcite.Extensions.Adapter.Enumerable;

using org.apache.calcite;
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
    /// The counterpart of <see cref="AdoToClrEnumerableConverter"/>, and the same node in every way but the
    /// sequence: the SQL, the parameter enrichment and the row builder are that node's own code, called from
    /// here, and what differs is that the rows come from <see cref="AdoSequences.ReadAsync{TRow}"/> rather
    /// than <see cref="AdoSequences.Read{TRow}"/>.
    ///
    /// <para><b>Why the adapter has this and Calcite's JDBC adapter has no counterpart.</b> JDBC has no
    /// asynchronous API; ADO.NET does. Without this node an asynchronous plan reaches the adapter through a
    /// converter over a synchronous leaf, which is honest for every other sub-plan — nothing under one
    /// suspends — and wrong for this one. The ADO leaf is the single place in a plan with real network I/O
    /// to wait on, so a plan whose whole purpose is not to hold a thread would have held one for every
    /// row.</para>
    ///
    /// <para><b>What is asynchronous here is the row loop, and only that.</b> The statement is still sent
    /// at <c>GetAsyncEnumerator</c>, synchronously, because that is where this convention acquires and
    /// <c>GetAsyncEnumerator</c> cannot await — <see cref="AdoSequences.ReadAsync{TRow}"/> says what that
    /// buys and what it costs. The row loop is where a query spends its time, and it is the part that no
    /// longer parks a thread.</para>
    ///
    /// <para>Cancellation is the convention's: the token the consumer hands <c>GetAsyncEnumerator</c> is
    /// what reaches <c>ReadAsync</c>, and the tree passes <c>default</c>, as it does for every other
    /// operator. Calcite's own channel, <c>DataContext.Variable.CANCEL_FLAG</c>, is read by nothing in this
    /// adapter and is not read here.</para>
    /// </remarks>
    public class AdoToClrAsyncEnumerableConverter : ConverterImpl, ClrAsyncEnumerableRel
    {

        static readonly System.Reflection.MethodInfo ReadAsyncMethod = typeof(AdoSequences).GetMethod(nameof(AdoSequences.ReadAsync))
            ?? throw new InvalidOperationException($"'{nameof(AdoSequences.ReadAsync)}' is missing from {nameof(AdoSequences)}.");

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

            var writer = AdoToClrEnumerableConverter.GenerateSql(convention, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder, self, out var sqlImplementor);
            var parameters = writer.Indexes;
            var parameterTypeNames = AdoToEnumerableConverter.GetParameterTypeNames(sqlImplementor, parameters);

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            // the schema SPI defines a convention's expression as linq4j, so this is the one thing here that
            // arrives as a linq4j tree, and it is translated where it is produced rather than composed into
            // anything first. Everything else this node builds is an expression tree from the start.
            var dataSource = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(AdoDataSource)));

            // the schema's mapping, fetched off the schema at run time the way the data source is. The row
            // builder is shared with the synchronous converter and neither of them decides this: which
            // .NET type a provider value becomes inside a plan is the schema's answer, and a route out of
            // the convention that did not ask would read the same column differently from the other two
            var typeRegistry = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(ClrTypeRegistry)));

            // a correlated sub-query leaves a parameter per correlation variable in the SQL, and the values
            // live on the context the builder closed over the outer row. Without the enricher the command is
            // handed to the provider unfilled.
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, AdoToClrEnumerableConverter.CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            // through ClrAsyncBuiltInMethod.Call rather than Expression.Call, because the operator ends in a
            // CancellationToken like every other one of this convention, and that is what appends the
            // default the [EnumeratorCancellation] attribute reads
            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(
                    ReadAsyncMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    AdoToClrEnumerableConverter.RowBuilder(typeRegistry, physType, rowType),
                    enricher));
        }

    }

}
