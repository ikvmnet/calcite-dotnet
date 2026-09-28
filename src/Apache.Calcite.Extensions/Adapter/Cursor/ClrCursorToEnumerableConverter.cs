using System;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Relational operator that converts the output of a <see cref="ClrCursorConvention"/> sub-plan to
    /// <c>EnumerableConvention</c>.
    /// </summary>
    /// <remarks>
    /// Calcite compiles <c>EnumerableConvention</c> code from generated Java source, which cannot hold an
    /// object, so the sub-plan is stashed in the <see cref="DataContext"/> and the generated code calls into
    /// it. The sub-plan is implemented with its synchronous open only, because generated Java reads rows
    /// through a linq4j <c>Enumerator</c> and cannot await. The expression tree is compiled the first time the
    /// plan runs, not while it is being implemented.
    ///
    /// <para>Correlation variables of an <c>EnumerableConvention</c> correlate above this node are passed to
    /// the sub-plan through the <see cref="DataContext"/>, because the sub-plan is compiled separately from
    /// the Java lambda that declares them.</para>
    /// </remarks>
    public class ClrCursorToEnumerableConverter : ConverterImpl, EnumerableRel
    {

        /// <summary>
        /// Adds this assembly to the IKVM boot class path, because the generated Java code names a type in it.
        /// </summary>
        static ClrCursorToEnumerableConverter()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(ClrCursorToEnumerableConverter).Assembly);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster">The cluster the node belongs to.</param>
        /// <param name="traits">The node's traits, in <c>EnumerableConvention</c>.</param>
        /// <param name="input">The sub-plan, in <see cref="ClrCursorConvention"/>.</param>
        public ClrCursorToEnumerableConverter(RelOptCluster cluster, RelTraitSet traits, RelNode input) :
            base(cluster, ConventionTraitDef.INSTANCE, traits, input)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorToEnumerableConverter(getCluster(), traitSet, (RelNode)sole(inputs));
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(EnumerableConvention.COST_MULTIPLIER);
        }

        /// <inheritdoc />
        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            // Calcite's map, so that anything stashed reaches the DataContext the plan is bound with; only
            // the synchronous open is built
            var clr = new ClrCursorRelImplementor(implementor.getRexBuilder(), implementor.map);

            // each correlation variable the sub-plan reads is a parameter of the Java lambda an enclosing
            // correlate generates, which the separately compiled sub-plan cannot see. Its fields are read
            // with Calcite's getter into an Object[], passed through the DataContext, and registered here
            // as an ARRAY-format outer row
            var variables = ClrCorrelationVariables.Used(getInput());
            var corrBlock = new J.BlockBuilder(false);
            var builder = new J.BlockBuilder();
            var names = new java.util.ArrayList();
            var rows = new java.util.ArrayList();
            var reads = new List<(ParameterExpression Variable, Expression Read)>();

            foreach (var (name, type) in variables)
            {
                var outer = PhysTypeImpl.of(clr.TypeFactory, type, JavaRowFormat.ARRAY, false);
                var pe = J.Expressions.parameter((java.lang.Class)typeof(object[]), name);
                var variable = Expression.Parameter(typeof(object[]), name);
                clr.Translator.Bind(pe, variable);
                clr.RegisterCorrelVariable(name, pe, corrBlock, outer);
                reads.Add((variable, Expression.Convert(Expression.Call(clr.Root, DataContextGet, Expression.Constant(name)), typeof(object[]))));

                var getter = implementor.getCorrelVariableGetter(name);
                var fields = new java.util.ArrayList();
                for (int i = 0; i < type.getFieldCount(); i++)
                {
                    // boxed explicitly for the Object[] initializer rather than left to the Java compiler
                    var field = getter.field(builder, i, null);
                    fields.add(J.Primitive.@is(field.getType()) ? J.Expressions.box(field) : field);
                }

                names.add(J.Expressions.constant(name));
                rows.add(J.Expressions.newArrayInit((java.lang.reflect.Type)(java.lang.Class)typeof(object), (java.lang.Iterable)fields));
            }

            var result = clr.VisitChild(null, 0, (ClrCursorRel)getInput(), ClrCursorPrefers.FromCalcite(pref));

            foreach (var (name, _) in variables)
                clr.ClearCorrelVariable(name);

            var expression = result.Expression;
            if (variables.Count > 0)
            {
                // read the outer rows from the context, then the field reads declared over them, then run
                // the sub-plan
                clr.Translator.TranslateStatements(corrBlock.toBlock(), out var declared, out var statements);
                var variablesDeclared = new List<ParameterExpression>();
                var body = new List<Expression>();
                foreach (var (variable, read) in reads)
                {
                    variablesDeclared.Add(variable);
                    body.Add(Expression.Assign(variable, read));
                }

                variablesDeclared.AddRange(declared);
                body.AddRange(statements);
                body.Add(expression);
                expression = Expression.Block(expression.Type, variablesDeclared, body);
            }

            // ClrPlan compiles the tree the first time it runs, not while the plan is being implemented
            var plan = new ClrPlan<IClrCursor>(
                Expression.Lambda<Func<DataContext, IClrCursor>>(
                    Expression.Convert(expression, typeof(IClrCursor)),
                    clr.Root));

            // stashed as Object, because generated Java source cannot name IKVM's class for a generic
            // instantiation; JavaPlans casts it back
            var stashed = implementor.stash(plan, (java.lang.Class)typeof(java.lang.Object));

            // EnumerableRelImplementor.result takes a Calcite physical type and casts it to PhysTypeImpl
            var physType = PhysTypeImpl.of(clr.TypeFactory, result.PhysType.RelRowType, result.PhysType.Format, false);

            var call = variables.Count == 0
                ? J.Expressions.call(BindMethod, stashed, DataContext.ROOT)
                : J.Expressions.call(BindCorrelatedMethod, stashed, DataContext.ROOT,
                    J.Expressions.newArrayInit((java.lang.reflect.Type)(java.lang.Class)typeof(string), (java.lang.Iterable)names),
                    J.Expressions.newArrayInit((java.lang.reflect.Type)(java.lang.Class)typeof(object), (java.lang.Iterable)rows));

            builder.add(J.Expressions.return_(null, call));

            return implementor.result(physType, builder.toBlock());
        }

        /// <summary>
        /// <see cref="JavaPlans.BindCursor"/>, which binds the stashed plan and returns its rows as a linq4j
        /// <c>Enumerable</c>.
        /// </summary>
        static readonly java.lang.reflect.Method BindMethod = ((java.lang.Class)typeof(JavaPlans))
            .getDeclaredMethod(nameof(JavaPlans.BindCursor), [typeof(java.lang.Object), typeof(DataContext)]);

        /// <summary>
        /// <see cref="JavaPlans.BindCursorCorrelated"/>: <see cref="JavaPlans.BindCursor"/> that also passes
        /// the named outer rows of the correlation variables through the <see cref="DataContext"/>.
        /// </summary>
        static readonly java.lang.reflect.Method BindCorrelatedMethod = ((java.lang.Class)typeof(JavaPlans))
            .getDeclaredMethod(nameof(JavaPlans.BindCursorCorrelated), [typeof(java.lang.Object), typeof(DataContext), typeof(string[]), typeof(object[])]);

        /// <summary>
        /// <c>DataContext.get</c>, through which the sub-plan reads a correlation variable's outer row.
        /// </summary>
        static readonly System.Reflection.MethodInfo DataContextGet = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.DATA_CONTEXT_GET.method);

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
