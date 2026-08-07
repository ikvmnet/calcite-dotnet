using System;
using System.Collections;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;
using Apache.Calcite.Extensions.Interop;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrEnumerableConvention"/> sub-plan as an
    /// <c>EnumerableConvention</c> one.
    /// </summary>
    /// <remarks>
    /// The other direction of <see cref="EnumerableToClrEnumerableConverter"/>, and the harder one. Calcite
    /// compiles its side with Janino from generated source, which cannot mention an object, so the sub-plan is
    /// compiled here and the delegate is stashed on the <see cref="DataContext"/> for the generated code to
    /// call. The rows are not touched.
    /// </remarks>
    public class ClrEnumerableToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        /// <remarks>
        /// The generated code names a type of this assembly, so Java has to be able to see it.
        /// </remarks>
        static ClrEnumerableToEnumerableConverter()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(ClrEnumerableToEnumerableConverter).Assembly);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrEnumerableToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableToEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        /// <remarks>
        /// The same multiplier the other direction applies, for the same reason: what a converter costs is
        /// what the convention it produces costs against a typical one.
        /// </remarks>
        public override RelOptCost computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost == null ? null! : cost.multiplyBy(EnumerableConvention.COST_MULTIPLIER);
        }

        /// <inheritdoc />
        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            // the same map, so what this side stashes reaches the DataContext the plan is bound with
            var clr = new ClrEnumerableRelImplementor(implementor.getRexBuilder(), implementor.map);
            var result = clr.VisitChild(null, 0, (ClrEnumerableRel)getInput(), ClrEnumerablePrefers.FromCalcite(pref));

            var plan = Expression.Lambda<Func<DataContext, IEnumerable>>(
                Expression.Convert(result.Expression, typeof(IEnumerable)),
                clr.Root).Compile();

            var stashed = implementor.stash(plan, (java.lang.Class)typeof(Func<DataContext, IEnumerable>));

            return implementor.result(result.PhysType,
                J.Blocks.toBlock(J.Expressions.call(BindMethod, stashed, DataContext.ROOT)));
        }

        /// <summary>
        /// <see cref="JavaSequences.Bind"/>, which runs the compiled sub-plan and reads it as a linq4j sequence.
        /// </summary>
        static readonly java.lang.reflect.Method BindMethod = ((java.lang.Class)typeof(JavaSequences))
            .getDeclaredMethod(nameof(JavaSequences.Bind), [typeof(Func<DataContext, IEnumerable>), typeof(DataContext)]);

        /// <inheritdoc />
        public Pair deriveTraits(RelTraitSet childTraits, int childId) => EnumerableRel.__DefaultMethods.deriveTraits(this, childTraits, childId);

        /// <inheritdoc />
        public DeriveMode getDeriveMode() => EnumerableRel.__DefaultMethods.getDeriveMode(this);

        /// <inheritdoc />
        public Pair passThroughTraits(RelTraitSet required) => EnumerableRel.__DefaultMethods.passThroughTraits(this, required);

        /// <inheritdoc />
        public RelNode derive(RelTraitSet childTraits, int childId) => PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);

        /// <inheritdoc />
        public java.util.List derive(java.util.List inputTraits) => PhysicalNode.__DefaultMethods.derive(this, inputTraits);

        /// <inheritdoc />
        public RelNode passThrough(RelTraitSet required) => PhysicalNode.__DefaultMethods.passThrough(this, required);

    }

}
