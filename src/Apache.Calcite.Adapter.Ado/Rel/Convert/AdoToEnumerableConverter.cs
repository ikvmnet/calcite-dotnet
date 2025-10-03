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

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    /// <summary>
    /// Relational expression represeting a scan of a table in an ADO data source.
    /// </summary>
    public class AdoToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        private static readonly Method GetDbReaderValueMethod = ((Class)typeof(AdoReaderUtil)).getDeclaredMethod(nameof(AdoReaderUtil.GetDbReaderValue), [typeof(DbDataReader), typeof(int), typeof(SqlTypeName)]);
        private static readonly Method CreateReaderMethod = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string), typeof(Function1)]);

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

            // declare values array
            var values_ = rowBuilder
                .append("values",
                    Expressions.newArrayBounds(
                        (Class)typeof(object),
                        1,
                        Expressions.constant(getRowType().getFieldCount())));

            // generate a call to GetDbReaderValue for each field
            for (int i = 0; i < getRowType().getFieldCount(); i++)
            {
                var primitive = Primitive.ofBoxOr(physType.fieldClass(i));
                var fieldType = ((RelDataTypeField)physType.getRowType().getFieldList().get(i)).getType();

                // extract value from DbReader
                var value_ = rowBuilder
                    .append("value",
                        Expressions.call(
                            null,
                            GetDbReaderValueMethod,
                            reader_,
                            Expressions.constant(i),
                            Expressions.constant(fieldType.getSqlTypeName())));

                // assign value to array at specified field index
                rowBuilder.add(
                    Expressions.statement(
                        Expressions.assign(
                            Expressions.arrayIndex(values_, Expressions.constant(i)),
                            value_)));
            }

            // return values array
            rowBuilder.add(
                Expressions.return_(null, values_));

            // generate row builder factory lambda
            var rowBuilderFactory_ = list
                .append("rowBuilderFactory",
                    Expressions.lambda(
                        Expressions.block(
                            Expressions.return_(
                                null,
                                Expressions.lambda(rowBuilder.toBlock()))),
                        reader_));

            // call AdoEnumerable.CreateReader
            var enumerable_ = list
                .append("enumerable",
                    Expressions.call(
                        null,
                        CreateReaderMethod,
                        Schemas.unwrap(convention.Expression, typeof(AdoDataSource)),
                        sql_,
                        rowBuilderFactory_));

            // return enumerable
            list.add(Expressions.return_(null, enumerable_));

            // return block
            return implementor.result(physType, list.toBlock());
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