using System;
using System.Data.Common;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.DataCursor;
using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

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
    /// <see cref="ClrDataCursorConvention"/> result by executing the generated SQL against the underlying
    /// ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// The counterpart of <see cref="AdoToClrEnumerableConverter"/>, generating the same SQL by the same
    /// route and building the same row, and the leaf the cursor convention exists for. The rows are read
    /// through <see cref="AdoCursors"/>, which hands the <see cref="DbDataReader"/> back as the cursor: an
    /// advance of the plan is an advance of the reader, and the token a caller gives
    /// <c>ReadAsync</c> is the token the provider's <c>ReadAsync</c> is given. Under the sequence
    /// convention that token could only enter once, at the enumerator, and a per-read token reached the
    /// provider by cancelling the statement's; here it reaches it directly.
    ///
    /// <para><b>Two bodies, and one line between them.</b> <see cref="Implement"/> opens with
    /// <see cref="AdoCursors.Open{TRow}"/>, which opens the connection and sends the statement there, and
    /// <see cref="ImplementAsync"/> with <see cref="AdoCursors.OpenAsync{TRow}"/>, which does both with
    /// await under the open's token. Everything else — the SQL, the parameters, the enricher, the row
    /// builder — is the same code building the same tree, because none of it is about the cursor.</para>
    /// </remarks>
    public class AdoToClrDataCursorConverter : ConverterImpl, ClrDataCursorRel
    {

        static readonly System.Reflection.MethodInfo OpenMethod = typeof(AdoCursors).GetMethod(nameof(AdoCursors.Open))
            ?? throw new InvalidOperationException($"'{nameof(AdoCursors.Open)}' is missing from {nameof(AdoCursors)}.");

        static readonly System.Reflection.MethodInfo OpenAsyncMethod = typeof(AdoCursors).GetMethod(nameof(AdoCursors.OpenAsync))
            ?? throw new InvalidOperationException($"'{nameof(AdoCursors.OpenAsync)}' is missing from {nameof(AdoCursors)}.");

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public AdoToClrDataCursorConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new AdoToClrDataCursorConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public ClrDataCursorResult Implement(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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

            // a correlated sub-query leaves a parameter per correlation variable in the SQL, and the values
            // live on the context the builder closed over the outer row. Without the enricher the command is
            // handed to the provider unfilled.
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, AdoToClrEnumerableConverter.CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            return implementor.Result(physType,
                Expression.Call(null,
                    OpenMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    AdoToClrEnumerableConverter.RowBuilder(physType, rowType),
                    enricher));
        }

        /// <inheritdoc />
        public ClrDataCursorAsyncResult ImplementAsync(ClrDataCursorRelImplementor implementor, ClrEnumerablePrefer pref)
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

            // a correlated sub-query leaves a parameter per correlation variable in the SQL, and the values
            // live on the context the builder closed over the outer row. Without the enricher the command is
            // handed to the provider unfilled.
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, AdoToClrEnumerableConverter.CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            // through ClrDataCursorBuiltInMethod.CallAsync rather than Expression.Call, because the open ends
            // in a CancellationToken like every other awaiting one, and that is what appends the
            // implementor's token parameter
            return implementor.ResultAsync(physType,
                ClrDataCursorBuiltInMethod.CallAsync(implementor,
                    OpenAsyncMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    AdoToClrEnumerableConverter.RowBuilder(physType, rowType),
                    enricher));
        }

    }

}
