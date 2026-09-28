using System;
using System.Data.Common;
using System.Linq.Expressions;

using Apache.Calcite.Extensions;
using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.sql.type;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Runs a subtree of an <see cref="AdoConvention"/> as one SQL statement and hands its rows to
    /// <see cref="ClrCursorConvention"/>. The counterpart of <see cref="AdoToEnumerableConverter"/>, generating the
    /// same SQL.
    /// </summary>
    /// <remarks>
    /// <para>
    /// The rows are read through <see cref="AdoCursors"/>, which returns the provider's
    /// <see cref="DbDataReader"/> as the plan's cursor: each read of the plan is one read of the provider's
    /// reader, and the token given to <c>ReadAsync</c> is passed to the provider's <c>ReadAsync</c>.
    /// </para>
    /// <para>
    /// <see cref="Implement"/> opens with <see cref="AdoCursors.Open{TRow}"/> and <see cref="ImplementAsync"/> with
    /// <see cref="AdoCursors.OpenAsync{TRow}"/>, which opens the connection and executes asynchronously under the
    /// open's token. The SQL, parameters and row builder are the same in both.
    /// </para>
    /// </remarks>
    public class AdoToClrCursorConverter : ConverterImpl, ClrCursorRel
    {

        static readonly System.Reflection.MethodInfo OpenMethod = typeof(AdoCursors).GetMethod(nameof(AdoCursors.Open))
            ?? throw new InvalidOperationException($"'{nameof(AdoCursors.Open)}' is missing from {nameof(AdoCursors)}.");

        static readonly System.Reflection.MethodInfo OpenAsyncMethod = typeof(AdoCursors).GetMethod(nameof(AdoCursors.OpenAsync))
            ?? throw new InvalidOperationException($"'{nameof(AdoCursors.OpenAsync)}' is missing from {nameof(AdoCursors)}.");

        internal static readonly System.Reflection.MethodInfo GetDbReaderValueMethod = typeof(AdoReaderUtil).GetMethod(nameof(AdoReaderUtil.GetDbReaderValue), [typeof(DbDataReader), typeof(int), typeof(SqlTypeName)])
            ?? throw new InvalidOperationException($"'{nameof(AdoReaderUtil.GetDbReaderValue)}' is missing from {nameof(AdoReaderUtil)}.");

        internal static readonly System.Reflection.MethodInfo CreateEnricherMethod = typeof(AdoEnumerable).GetMethod(nameof(AdoEnumerable.CreateEnricher), [typeof(AdoDataSource), typeof(java.util.List), typeof(java.util.List), typeof(DataContext)])
            ?? throw new InvalidOperationException($"'{nameof(AdoEnumerable.CreateEnricher)}' is missing from {nameof(AdoEnumerable)}.");


        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The traits, whose convention is <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The subtree of the <see cref="AdoConvention"/>.</param>
        public AdoToClrCursorConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new AdoToClrCursorConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <summary>
        /// Returns the base cost multiplied by 0.1. Mirrors <c>JdbcToEnumerableConverter.computeSelfCost</c>.
        /// </summary>
        /// <param name="planner">The planner.</param>
        /// <param name="mq">The metadata query.</param>
        /// <returns>The cost, or <see langword="null"/>.</returns>
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);
            if (cost == null)
                return null;

            return cost.multiplyBy(.1);
        }

        /// <inheritdoc />
        /// <exception cref="AdoCalciteException">The input is not an <see cref="AdoRel"/> of an <see cref="AdoConvention"/>.</exception>
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (getInput() is not AdoRel self)
                throw new AdoCalciteException("Unsupported input type.");

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var rowType = physType.RowType;

            if (self.getConvention() is not AdoConvention convention)
                throw new AdoCalciteException($"getConvention() is null for {self}.");

            var dataContextBuilder = new AdoClrCorrelationDataContextBuilder(implementor, implementor.Root);

            var writer = GenerateSql(convention, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder, self, out var sqlImplementor);
            var parameters = writer.Indexes;
            var parameterTypeNames = AdoToEnumerableConverter.GetParameterTypeNames(sqlImplementor, parameters);

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            // the schema SPI gives the convention's expression as linq4j; it is translated here, where it is
            // produced
            var dataSource = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(AdoDataSource)));

            // each parameter in the SQL, whether a dynamic parameter or a correlation variable, is filled from
            // the context when the command is created
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            return implementor.Result(physType,
                Expression.Call(null,
                    OpenMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    RowBuilder(physType, rowType),
                    enricher));
        }

        /// <inheritdoc />
        /// <exception cref="AdoCalciteException">The input is not an <see cref="AdoRel"/> of an <see cref="AdoConvention"/>.</exception>
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            if (getInput() is not AdoRel self)
                throw new AdoCalciteException("Unsupported input type.");

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.PreferArray());
            var rowType = physType.RowType;

            if (self.getConvention() is not AdoConvention convention)
                throw new AdoCalciteException($"getConvention() is null for {self}.");

            var dataContextBuilder = new AdoClrCorrelationDataContextBuilder(implementor, implementor.Root);

            var writer = GenerateSql(convention, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder, self, out var sqlImplementor);
            var parameters = writer.Indexes;
            var parameterTypeNames = AdoToEnumerableConverter.GetParameterTypeNames(sqlImplementor, parameters);

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            // the schema SPI gives the convention's expression as linq4j; it is translated here, where it is
            // produced
            var dataSource = implementor.Translator.Translate(Schemas.unwrap(convention.Expression, typeof(AdoDataSource)));

            // each parameter in the SQL, whether a dynamic parameter or a correlation variable, is filled from
            // the context when the command is created
            var enricher = parameters.isEmpty()
                ? (Expression)Expression.Constant(null, typeof(DbCommandEnricher))
                : Expression.Call(null, CreateEnricherMethod, dataSource, Expression.Constant(parameters), Expression.Constant(parameterTypeNames), dataContextBuilder.Build());

            // CallAsync appends the implementor's cancellation token as the last argument
            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor,
                    OpenAsyncMethod.MakeGenericMethod(rowType),
                    dataSource,
                    Expression.Constant(sql),
                    RowBuilder(physType, rowType),
                    enricher));
        }


        /// <summary>
        /// Builds the lambda that reads the reader's current row as a <paramref name="rowType"/>.
        /// </summary>
        /// <param name="physType">The row's physical type.</param>
        /// <param name="rowType">The CLR type of a row.</param>
        /// <returns>A <c>Func&lt;DbDataReader, TRow&gt;</c> lambda.</returns>
        /// <remarks>
        /// As in <see cref="AdoToEnumerableConverter"/>, and matching <c>JavaRowFormat.optimize</c>: a row of no
        /// fields is <see langword="null"/>, a row of one field is that field's value, and a wider row is an
        /// <c>object[]</c>.
        /// </remarks>
        internal static Expression RowBuilder(ClrPhysType physType, Type rowType)
        {
            var reader = Expression.Parameter(typeof(DbDataReader), "reader");
            var fieldCount = physType.RelRowType.getFieldCount();

            Expression body;
            if (fieldCount == 0)
                body = Expression.Constant(null, typeof(object));
            else if (fieldCount == 1)
                body = ReadField(reader, physType, 0);
            else
            {
                var values = new Expression[fieldCount];
                for (int i = 0; i < fieldCount; i++)
                    values[i] = ReadField(reader, physType, i);

                body = Expression.NewArrayInit(typeof(object), values);
            }

            // a one-column row is cast from object to its row type; an object[] row needs nothing
            if (body.Type != rowType)
                body = Expression.Convert(body, rowType);

            return Expression.Lambda(typeof(Func<,>).MakeGenericType(typeof(DbDataReader), rowType), body, reader);
        }

        /// <summary>
        /// Returns a call to <see cref="AdoReaderUtil.GetDbReaderValue(DbDataReader, int, SqlTypeName)"/> that reads
        /// one field as its declared SQL type, whatever CLR type the provider returns.
        /// </summary>
        /// <param name="reader">The reader parameter.</param>
        /// <param name="physType">The row's physical type.</param>
        /// <param name="index">The field's ordinal.</param>
        /// <returns>The call.</returns>
        static Expression ReadField(ParameterExpression reader, ClrPhysType physType, int index)
        {
            var fieldType = ((RelDataTypeField)physType.RelRowType.getFieldList().get(index)).getType();

            return Expression.Call(null,
                GetDbReaderValueMethod,
                reader,
                Expression.Constant(index),
                Expression.Constant(fieldType.getSqlTypeName()));
        }

        /// <summary>
        /// Translates the subtree to SQL, applies the syntax's rewrite, and writes the statement with each
        /// parameter named as the driver binds it.
        /// </summary>
        /// <param name="convention">The convention, whose dialect and syntax the statement is written in.</param>
        /// <param name="typeFactory">The type factory.</param>
        /// <param name="dataContextBuilder">Registers each correlation variable the statement reads.</param>
        /// <param name="input">The root of the subtree.</param>
        /// <param name="implementor">The implementor used, which records each correlation variable's SQL type.</param>
        /// <returns>The writer, holding the SQL and the variable index behind each parameter.</returns>
        internal static AdoSqlWriter GenerateSql(AdoConvention convention, JavaTypeFactory typeFactory, IAdoCorrelationDataContextBuilder dataContextBuilder, AdoRel input, out AdoImplementor implementor)
        {
            implementor = new AdoImplementor(convention.Dialect, typeFactory, dataContextBuilder);
            var result = implementor.visitRoot(input);

            var writer = new AdoSqlWriter(convention.Dialect, convention.Syntax);
            convention.Syntax.Rewrite(result.asStatement(), convention.Dialect, typeFactory).unparse(writer, 0, 0);
            return writer;
        }

    }

}
