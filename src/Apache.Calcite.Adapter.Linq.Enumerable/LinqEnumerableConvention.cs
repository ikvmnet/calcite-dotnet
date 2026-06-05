using java.lang;
using java.util.function;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

using static java.util.Objects;

namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    public class LinqEnumerableConvention : Convention.Impl
    {

        public static readonly LinqEnumerableConvention INSTANCE = new LinqEnumerableConvention("INSTANCE");

        public static readonly double COST_MULTIPLIER = 1.0d;

        public LinqEnumerableConvention(string name) :
            base("LinqEnumerable." + name, typeof(LinqEnumerableRel))
        {

        }

        public override Class getInterface()
        {
            return typeof(LinqEnumerableRel);
        }

        public override string getName()
        {
            return "LINQENUMERABLE";
        }

        public override RelNode enforce(RelNode input, RelTraitSet required)
        {
            var rel = input;
            if (input.getConvention() != INSTANCE)
            {
                rel = ConventionTraitDef.INSTANCE.convert(input.getCluster().getPlanner(), input, INSTANCE, true);
                requireNonNull(rel, new DelegateSupplier<string>(() => "Unable to convert input to " + INSTANCE + ", input = " + input));
            }

            var collation = required.getCollation();
            if (collation != null && collation != RelCollations.EMPTY)
                rel = EnumerableSort.create(rel, collation, null, null);

            return rel;
        }

        public override RelTraitDef getTraitDef()
        {
            return ConventionTraitDef.INSTANCE;
        }

        public override bool satisfies(RelTrait trait)
        {
            return this == trait;
        }

        public override void register(RelOptPlanner planner)
        {

        }

        public override bool canConvertConvention(Convention toConvention)
        {
            return false;
        }

        public override bool useAbstractConvertersForConversion(RelTraitSet fromTraits, RelTraitSet toTraits)
        {
            return false;
        }

        public override RelFactories.Struct getRelFactories()
        {
            return RelFactories.Struct.fromContext(Contexts.of(
                LinqEnumerableRelFactories.ENUMERABLE_TABLE_SCAN_FACTORY,
                LinqEnumerableRelFactories.ENUMERABLE_PROJECT_FACTORY,
                LinqEnumerableRelFactories.ENUMERABLE_FILTER_FACTORY,
                LinqEnumerableRelFactories.ENUMERABLE_SORT_FACTORY));
        }

    }

}
