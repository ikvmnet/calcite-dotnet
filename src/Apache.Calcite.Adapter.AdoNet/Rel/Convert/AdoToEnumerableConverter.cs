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
    /// Relational operator that converts a tree of <see cref="AdoConvention"/> nodes into
    /// an <see cref="EnumerableConvention"/> result by executing the generated SQL against
    /// the underlying ADO.NET data source.
    /// </summary>
    public class AdoToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        static readonly Method GetDbReaderValueMethod = ((Class)typeof(AdoReaderUtil)).getDeclaredMethod(nameof(AdoReaderUtil.GetDbReaderValue), [typeof(DbDataReader), typeof(int), typeof(SqlTypeName)]);
        static readonly Method CreateReaderMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string), typeof(Function1)]);
        static readonly Method CreateReaderWithEnricherMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string), typeof(Function1), typeof(DbCommandEnricher)]);
        static readonly Method CreateEnricherMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateEnricher), [typeof(AdoDataSource), typeof(java.util.List), typeof(DataContext)]);

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public AdoToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, List inputs)
        {
            return new AdoToEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
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

            // generate the SQL for the query
            var sqlString = GenerateSql(convention.Dialect, dataContextBuilder, self);
            var sql = sqlString.getSql();
            Hook.QUERY_PLAN.run(sql);

            // declare SQL string as a variable
            var sql_ = list
                .append("sql",
                    Expressions.constant(sql));

            var fields_ = list
                .append("fields",
                    Expressions.constant(getRowType().getFieldList()));

            var rowBuilder = new BlockBuilder();

            // parameter to lambda for the DbDataReader
            var reader_ = Expressions.parameter(
                Modifier.FINAL,
                (Class)typeof(DbDataReader),
                rowBuilder.newName("reader"));

            // the shape of a row is decided by how many fields it has, exactly as
            // JdbcToEnumerableConverter decides it, because JavaRowFormat.optimize has already told the
            // physical type the same thing: no field is a null, one field is the value itself, and only
            // beyond that is a row an array. Returning an array for a single column leaves Avatica's
            // accessor casting an Object[] to the column's type.
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
                // declare values array
                var values_ = rowBuilder
                    .append("values",
                        Expressions.newArrayBounds(
                            (Class)typeof(object),
                            1,
                            Expressions.constant(fieldCount)));

                // generate a call to GetDbReaderValue for each field
                for (int i = 0; i < fieldCount; i++)
                {
                    // assign value to array at specified field index
                    rowBuilder.add(
                        Expressions.statement(
                            Expressions.assign(
                                Expressions.arrayIndex(values_, Expressions.constant(i)),
                                ReadField(rowBuilder, reader_, physType, i))));
                }

                // return values array
                rowBuilder.add(
                    Expressions.return_(null, values_));
            }

            // generate row builder factory lambda
            var rowBuilderFactory_ = list
                .append("rowBuilderFactory",
                    Expressions.lambda(
                        Expressions.block(
                            Expressions.return_(
                                null,
                                Expressions.lambda(rowBuilder.toBlock()))),
                        reader_));

            var dataSource_ = Schemas.unwrap(convention.Expression, typeof(AdoDataSource));

            // a correlated sub-query leaves a parameter marker per correlation variable in the SQL, and the
            // values live on the context the builder closed over the outer row. Without this the command is
            // handed to the provider with its parameters unfilled. Calcite does the same at
            // JdbcToEnumerableConverter:222.
            var parameters = sqlString.getDynamicParameters();
            var enumerable_ = parameters is not null && parameters.isEmpty() == false
                ? list.append("enumerable",
                    Expressions.call(
                        null,
                        CreateReaderWithEnricherMethod,
                        dataSource_,
                        sql_,
                        rowBuilderFactory_,
                        list.append("enricher",
                            Expressions.call(
                                null,
                                CreateEnricherMethod,
                                dataSource_,
                                // the indexes are settled while planning, so they travel as a constant
                                Expressions.constant(Indexes(parameters)),
                                dataContextBuilder.Build()))))
                : list.append("enumerable",
                    Expressions.call(
                        null,
                        CreateReaderMethod,
                        dataSource_,
                        sql_,
                        rowBuilderFactory_));

            // return enumerable
            list.add(Expressions.return_(null, enumerable_));

            // return block
            return implementor.result(physType, list.toBlock());
        }

        /// <summary>
        /// Returns the variable index behind each dynamic parameter, in parameter order.
        /// </summary>
        /// <param name="parameters"></param>
        /// <returns></returns>
        static java.util.List Indexes(java.util.List parameters)
        {
            var indexes = new java.util.ArrayList(parameters.size());
            for (int i = 0; i < parameters.size(); i++)
                indexes.add(java.lang.Integer.valueOf(((java.lang.Number)parameters.get(i)).intValue()));

            return indexes;
        }

        /// <summary>
        /// Appends the read of one field and returns the expression holding it.
        /// </summary>
        /// <param name="rowBuilder"></param>
        /// <param name="reader_"></param>
        /// <param name="physType"></param>
        /// <param name="index"></param>
        /// <returns></returns>
        /// <remarks>
        /// The declared SQL type decides how the value is read, not whatever the provider chose to surface
        /// it as, so the row holds what the plan was built against.
        /// </remarks>
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
        /// Generates the SQL string to implement the enumerable.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="dataContextBuilder"></param>
        /// <param name="input"></param>
        /// <returns></returns>
        SqlString GenerateSql(SqlDialect dialect, IAdoCorrelationDataContextBuilder dataContextBuilder, AdoRel input)
        {
            var implementor = new AdoImplementor(dialect, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder);
            var result = implementor.visitRoot(input);
            return result.asStatement().toSqlString(dialect);
        }

        #region EnumerableRel

        /// <inheritdoc />
        public Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            return EnumerableRel.__DefaultMethods.deriveTraits(this, childTraits, childId);
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return EnumerableRel.__DefaultMethods.getDeriveMode(this);
        }

        /// <inheritdoc />
        public Pair passThroughTraits(RelTraitSet required)
        {
            return EnumerableRel.__DefaultMethods.passThroughTraits(this, required);
        }

        #endregion

        #region PhysicalNode

        /// <inheritdoc />
        public RelNode derive(RelTraitSet childTraits, int childId)
        {
            return PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);
        }

        /// <inheritdoc />
        public List derive(List inputTraits)
        {
            return PhysicalNode.__DefaultMethods.derive(this, inputTraits);
        }

        /// <inheritdoc />
        public RelNode passThrough(RelTraitSet required)
        {
            return PhysicalNode.__DefaultMethods.passThrough(this, required);
        }

        #endregion

    }

}
