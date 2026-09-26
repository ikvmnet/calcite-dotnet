using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrDataCursorConvention"/> sub-plan as an
    /// <c>EnumerableConvention</c> one.
    /// </summary>
    /// <remarks>
    /// <see cref="ClrEnumerableToEnumerableConverter"/> for this convention, and the same arrangement:
    /// Calcite compiles its side with Janino from generated source, which cannot mention an object, so the
    /// sub-plan's tree is stashed on the <see cref="DataContext"/> for the generated code to call back into.
    /// The sub-plan is the synchronous open, because a linq4j <c>Enumerator</c> is pulled and the generated
    /// source calling it cannot await. The rows are not touched.
    ///
    /// <para>A tree rather than a delegate, because compiling is not planning: it happens the first time
    /// the plan runs. See <see cref="ClrPlan{TRows}"/>.</para>
    /// </remarks>
    public class ClrDataCursorToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        /// <summary>
        /// Initializes the static instance.
        /// </summary>
        /// <remarks>
        /// The generated code names a type of this assembly, so Java has to be able to see it.
        /// </remarks>
        static ClrDataCursorToEnumerableConverter()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(ClrDataCursorToEnumerableConverter).Assembly);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traits"></param>
        /// <param name="input"></param>
        public ClrDataCursorToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrDataCursorToEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            // dearer than the crossing into the sequence convention, for the reason the multiplier gives
            return cost?.multiplyBy(ClrDataCursorConvention.JavaCrossingCostMultiplier);
        }

        /// <inheritdoc />
        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            // the same map, so what this side stashes reaches the DataContext the plan is bound with. The
            // token parameter is declared for the awaiting hierarchy and never reached: only the synchronous
            // open is built here
            var clr = new ClrDataCursorRelImplementor(implementor.getRexBuilder(), implementor.map);
            var result = clr.VisitChild(null, 0, (ClrDataCursorRel)getInput(), ClrEnumerablePrefers.FromCalcite(pref));

            // the tree, not a delegate. Compiling here would be JIT work done while the plan is still being
            // assembled, and once per converter besides; ClrPlan compiles itself the first time it is run.
            var plan = new ClrPlan<ClrDataCursor>(
                Expression.Lambda<Func<DataContext, ClrDataCursor>>(
                    Expression.Convert(result.Expression, typeof(ClrDataCursor)),
                    clr.Root));

            // stashed as an Object, because the generated source declares the variable by the type's name
            // and cannot name a generic instantiation -- see JavaPlans
            var stashed = implementor.stash(plan, (java.lang.Class)typeof(java.lang.Object));

            // their convention's row abstraction, built from the three values ours carries, because that
            // is what EnumerableRelImplementor.result takes -- and it casts to PhysTypeImpl besides
            var physType = PhysTypeImpl.of(clr.TypeFactory, result.PhysType.RelRowType, result.PhysType.Format, false);

            return implementor.result(physType,
                J.Blocks.toBlock(J.Expressions.call(BindMethod, stashed, DataContext.ROOT)));
        }

        /// <summary>
        /// <see cref="JavaPlans.BindCursor"/>, which opens the sub-plan and reads it as a linq4j sequence.
        /// </summary>
        static readonly java.lang.reflect.Method BindMethod = ((java.lang.Class)typeof(JavaPlans))
            .getDeclaredMethod(nameof(JavaPlans.BindCursor), [typeof(java.lang.Object), typeof(DataContext)]);

        /// <inheritdoc />
        public Pair? deriveTraits(RelTraitSet childTraits, int childId) => EnumerableRel.__DefaultMethods.deriveTraits(this, childTraits, childId);

        /// <inheritdoc />
        public DeriveMode getDeriveMode() => EnumerableRel.__DefaultMethods.getDeriveMode(this);

        /// <inheritdoc />
        public Pair? passThroughTraits(RelTraitSet required) => EnumerableRel.__DefaultMethods.passThroughTraits(this, required);

        /// <inheritdoc />
        public RelNode? derive(RelTraitSet childTraits, int childId) => PhysicalNode.__DefaultMethods.derive(this, childTraits, childId);

        /// <inheritdoc />
        public java.util.List derive(java.util.List inputTraits) => PhysicalNode.__DefaultMethods.derive(this, inputTraits);

        /// <inheritdoc />
        public RelNode? passThrough(RelTraitSet required) => PhysicalNode.__DefaultMethods.passThrough(this, required);

    }

}
