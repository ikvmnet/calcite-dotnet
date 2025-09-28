using System;

using java.lang;
using java.lang.reflect;
using java.util;
using java.util.function;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.config;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.runtime;
using org.apache.calcite.schema;
using org.apache.calcite.sql;
using org.apache.calcite.sql.util;
using org.apache.calcite.util;

namespace Apache.Calcite.Adapter.Ado.Rel.Convert
{

    /// <summary>
    /// Relational expression representing a scan of a table in an ADO data source.
    /// </summary>
    class AdoToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        static readonly Method _enumerableCreateReader = ((Class)typeof(AdoEnumerable)).getDeclaredMethod(nameof(AdoEnumerable.CreateReader), [typeof(AdoDataSource), typeof(string)]);

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
            return new AdoToEnumerableConverter(getCluster(), getTraitSet(), (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            return base.computeSelfCost(planner, mq)?.multiplyBy(.1);
        }

        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var builder = new BlockBuilder(false);
            var child = (AdoRel)getInput();
            var physType = PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.prefer(JavaRowFormat.CUSTOM));
            var convention = (AdoConvention)Objects.requireNonNull(child.getConvention(), new DelegateSupplier<string>(() => $"child.getConvention() is null for {child}"));
            var dataContextBuilder = new AdoCorrelationDataContextBuilderImpl(implementor, builder, DataContext.ROOT);
            var sqlString = GenerateSql(convention.Dialect, dataContextBuilder);
            var sql = sqlString.getSql();

            // print out log if directed to
            if (((java.lang.Boolean)CalciteSystemProperty.DEBUG.value()).booleanValue())
                Console.WriteLine("[" + sql + "]");

            // run query plan
            Hook.QUERY_PLAN.run(sql);

            // we return an expression the calls CreateReader on AdoEnumerable.
            builder.add(
                Expressions.return_(
                    null,
                    Expressions.call(
                        _enumerableCreateReader,
                        Schemas.unwrap(convention.Expression, (Class)typeof(AdoDataSource)),
                        Expressions.constant(sql))));

            // final result
            return implementor.result(physType, builder.toBlock());
        }

        /// <summary>
        /// Generates the SQL to pass to ADO.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="dataContextBuilder"></param>
        /// <returns></returns>
        SqlString GenerateSql(SqlDialect dialect, IAdoCorrelationDataContextBuilder dataContextBuilder)
        {
            var implementor = new AdoImplementor(dialect, (JavaTypeFactory)getCluster().getTypeFactory(), dataContextBuilder);
            var result = implementor.visitRoot(getInput());
            return result.asStatement().toSqlString(dialect);
        }

        /// <inheritdoc />
        public Pair passThroughTraits(RelTraitSet required)
        {
            return EnumerableRel.__DefaultMethods.passThroughTraits(this, required);
        }

        /// <inheritdoc />
        public Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            return EnumerableRel.__DefaultMethods.deriveTraits(this, childTraits, childId);
        }

        /// <inheritdoc />
        public DeriveMode getDeriveMode()
        {
            return PhysicalNode.__DefaultMethods.getDeriveMode(this);
        }

        /// <inheritdoc />
        public RelNode passThrough(RelTraitSet required)
        {
            return PhysicalNode.__DefaultMethods.passThrough(this, required);
        }

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

    }

}
