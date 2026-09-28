using System.Data.Common;

using java.lang;
using java.lang.reflect;
using java.util;
using java.util.function;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.type;
using org.apache.calcite.sql.util;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Runs a subtree of an <see cref="AdoConvention"/> as one SQL statement and hands its rows to Calcite's
    /// <see cref="EnumerableConvention"/>. Mirrors <c>JdbcToEnumerableConverter</c>.
    /// </summary>
    /// <remarks>
    /// The generated code calls <see cref="AdoEnumerable.CreateReader(AdoDataSource, string, Function1, DbCommandEnricher)"/>,
    /// so the statement runs when the enumerable is enumerated.
    /// </remarks>
    public class AdoToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        static readonly Method GetDbReaderValueMethod = ((Class)typeof(AdoReaderUtil)).getDeclaredMethod(nameof(AdoReaderUtil.GetDbReaderValue), [typeof(DbDataReader), typeof(int), typeof(SqlTypeName)]);
        static readonly Method CreateReaderMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string), typeof(Function1)]);
        static readonly Method CreateReaderWithEnricherMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string), typeof(Function1), typeof(DbCommandEnricher)]);
        static readonly Method CreateEnricherMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateEnricher), [typeof(AdoDataSource), typeof(java.util.List), typeof(java.util.List), typeof(DataContext)]);

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traits">The traits, whose convention is <see cref="EnumerableConvention"/>.</param>
        /// <param name="input">The subtree of the <see cref="AdoConvention"/>.</param>
        public AdoToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, List inputs)
        {
            return new AdoToEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
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

        /// <summary>
        /// Generates the linq4j block that runs the statement and builds each row. Mirrors
        /// <c>JdbcToEnumerableConverter.implement</c>.
        /// </summary>
        /// <param name="implementor">Calcite's implementor.</param>
        /// <param name="pref">The preferred row format.</param>
        /// <returns>The result.</returns>
        /// <exception cref="AdoCalciteException">The input is not an <see cref="AdoRel"/>.</exception>
        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var list = new BlockBuilder();
            var self = getInput() as AdoRel;
            if (self is null)
                throw new AdoCalciteException("Unsupported input type.");

            var physType =
                PhysTypeImpl.of(
                    implementor.getTypeFactory(),
                    getRowType(),
                    pref.prefer(JavaRowFormat.ARRAY));

            var convention = (AdoConvention)Objects.requireNonNull(
                self.getConvention(),
                new DelegateSupplier<string>(() => $"scan.getConvention() is null for {self}"));

            var dataContextBuilder =
                new AdoCorrelationDataContextBuilderImpl(implementor, list, DataContext.ROOT);

            // each parameter is written as the name the provider binds, not JDBC's positional ?
            var writer = GenerateSql(convention, dataContextBuilder, self, out var sqlImplementor);
            var dataSource = Schemas.unwrap(convention.Expression, typeof(AdoDataSource));

            var parameters = writer.Indexes;
            var hasParameters = parameters.isEmpty() == false;
            var parameterTypeNames = GetParameterTypeNames(sqlImplementor, parameters);

            var sql = writer.toSqlString().getSql();
            Hook.QUERY_PLAN.run(sql);

            var sql_ = list
                .append("sql",
                    Expressions.constant(sql));

            var fields_ = list
                .append("fields",
                    Expressions.constant(getRowType().getFieldList()));

            var rowBuilder = new BlockBuilder();

            var reader_ = Expressions.parameter(
                Modifier.FINAL,
                (Class)typeof(DbDataReader),
                rowBuilder.newName("reader"));

            // as in JdbcToEnumerableConverter, and matching JavaRowFormat.optimize: no fields is a null row,
            // one field is the value itself, and more is an Object[]. Avatica's accessors expect exactly this
            var fieldCount = getRowType().getFieldCount();
            if (fieldCount == 0)
            {
                rowBuilder.add(
                    Expressions.return_(null, Expressions.constant(null, (Class)typeof(object))));
            }
            else if (fieldCount == 1)
            {
                rowBuilder.add(
                    Expressions.return_(null, ReadField(rowBuilder, reader_, physType, 0)));
            }
            else
            {
                var values_ = rowBuilder
                    .append("values",
                        Expressions.newArrayBounds(
                            (Class)typeof(object),
                            1,
                            Expressions.constant(fieldCount)));

                for (int i = 0; i < fieldCount; i++)
                {
                    rowBuilder.add(
                        Expressions.statement(
                            Expressions.assign(
                                Expressions.arrayIndex(values_, Expressions.constant(i)),
                                ReadField(rowBuilder, reader_, physType, i))));
                }

                rowBuilder.add(
                    Expressions.return_(null, values_));
            }

            // reader => () => row
            var rowBuilderFactory_ = list
                .append("rowBuilderFactory",
                    Expressions.lambda(
                        Expressions.block(
                            Expressions.return_(
                                null,
                                Expressions.lambda(rowBuilder.toBlock()))),
                        reader_));

            // each parameter in the SQL, whether a dynamic parameter or a correlation variable, is filled from
            // the context when the command is created, as JdbcToEnumerableConverter binds its parameters
            var enumerable_ = hasParameters
                ? list.append("enumerable",
                    Expressions.call(
                        null,
                        CreateReaderWithEnricherMethod,
                        dataSource,
                        sql_,
                        rowBuilderFactory_,
                        list.append("enricher",
                            Expressions.call(
                                null,
                                CreateEnricherMethod,
                                dataSource,
                                // known while planning, so passed as constants
                                Expressions.constant(parameters),
                                Expressions.constant(parameterTypeNames),
                                dataContextBuilder.Build()))))
                : list.append("enumerable",
                    Expressions.call(
                        null,
                        CreateReaderMethod,
                        dataSource,
                        sql_,
                        rowBuilderFactory_));

            list.add(Expressions.return_(null, enumerable_));

            return implementor.result(physType, list.toBlock());
        }

        /// <summary>
        /// Appends a call to <see cref="AdoReaderUtil.GetDbReaderValue(DbDataReader, int, SqlTypeName)"/> that reads
        /// one field as its declared SQL type, whatever CLR type the provider returns.
        /// </summary>
        /// <param name="rowBuilder">The block to append to.</param>
        /// <param name="reader_">The reader parameter.</param>
        /// <param name="physType">The row's physical type.</param>
        /// <param name="index">The field's ordinal.</param>
        /// <returns>The variable holding the value.</returns>
        static Expression ReadField(BlockBuilder rowBuilder, ParameterExpression reader_, PhysType physType, int index)
        {
            var fieldType = ((RelDataTypeField)physType.getRowType().getFieldList().get(index)).getType();

            return rowBuilder.append("value",
                Expressions.call(
                    null,
                    GetDbReaderValueMethod,
                    reader_,
                    Expressions.constant(index),
                    Expressions.constant(fieldType.getSqlTypeName())));
        }

        /// <summary>
        /// Translates the subtree to SQL, applies the syntax's rewrite, and writes the statement with each
        /// parameter named as the driver binds it.
        /// </summary>
        /// <param name="convention">The convention, whose dialect and syntax the statement is written in.</param>
        /// <param name="dataContextBuilder">Registers each correlation variable the statement reads.</param>
        /// <param name="input">The root of the subtree.</param>
        /// <param name="implementor">The implementor used, which records each correlation variable's SQL type.</param>
        /// <returns>The writer, holding the SQL and the variable index behind each parameter.</returns>
        AdoSqlWriter GenerateSql(AdoConvention convention, IAdoCorrelationDataContextBuilder dataContextBuilder, AdoRel input, out AdoImplementor implementor)
        {
            var typeFactory = (JavaTypeFactory)getCluster().getTypeFactory();
            implementor = new AdoImplementor(convention.Dialect, typeFactory, dataContextBuilder);
            var result = implementor.visitRoot(input);

            var writer = new AdoSqlWriter(convention.Dialect, convention.Syntax);
            convention.Syntax.Rewrite(result.asStatement(), convention.Dialect, typeFactory).unparse(writer, 0, 0);
            return writer;
        }

        /// <summary>
        /// Returns the SQL type name of each parameter, in the writer's parameter order.
        /// </summary>
        /// <param name="implementor">The implementor that recorded a type per correlation variable index.</param>
        /// <param name="indexes">The variable index behind each parameter, in parameter order.</param>
        /// <returns>A list of <see cref="SqlTypeName"/> names, <see langword="null"/> for a parameter that is not a
        /// correlation variable.</returns>
        internal static java.util.ArrayList GetParameterTypeNames(AdoImplementor implementor, java.util.List indexes)
        {
            var names = new java.util.ArrayList(indexes.size());
            for (int i = 0; i < indexes.size(); i++)
                names.add(implementor.GetDynamicParamType(((java.lang.Number)indexes.get(i)).intValue())?.name());

            return names;
        }

        #region EnumerableRel

        /// <inheritdoc />
        public Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return EnumerableRel.__DefaultMethods.deriveTraits(this, childTraits, childId);
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return EnumerableRel.__DefaultMethods.getDeriveMode(this);
        }

        /// <inheritdoc />
        public Pair? passThroughTraits(RelTraitSet required)
        {
            return EnumerableRel.__DefaultMethods.passThroughTraits(this, required);
        }

        #endregion

        #region PhysicalNode

        /// <inheritdoc />
        public RelNode? derive(RelTraitSet childTraits, int childId)
        {
            return PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);
        }

        /// <inheritdoc />
        public List derive(List inputTraits)
        {
            return PhysicalNode.__DefaultMethods.derive(this, inputTraits);
        }

        /// <inheritdoc />
        public RelNode? passThrough(RelTraitSet required)
        {
            return PhysicalNode.__DefaultMethods.passThrough(this, required);
        }

        #endregion

    }

}
