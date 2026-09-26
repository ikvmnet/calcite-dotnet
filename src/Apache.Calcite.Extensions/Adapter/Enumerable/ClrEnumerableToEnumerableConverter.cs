using System;
using System.Collections;
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

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Relational operator that reads the result of a <see cref="ClrEnumerableConvention"/> sub-plan as an
    /// <c>EnumerableConvention</c> one.
    /// </summary>
    /// <remarks>
    /// The other direction of <see cref="EnumerableToClrEnumerableConverter"/>, and the harder one. Calcite
    /// compiles its side with Janino from generated source, which cannot mention an object, so the sub-plan's
    /// tree is stashed on the <see cref="DataContext"/> for the generated code to call back into. The rows
    /// are not touched.
    ///
    /// <para>A tree rather than a delegate, because compiling is not planning: it happens the first time the
    /// plan runs. See <see cref="ClrPlan{TRows}"/>.</para>
    ///
    /// <para>What would be better is translating the tree into a linq4j one, so that the sub-plan became
    /// part of the block Calcite compiles and there was no callback at all. That is the inverse of
    /// <c>LixToClrTranslator</c> and a real piece of work; <c>TODO.md</c> has it.</para>
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
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, org.apache.calcite.rel.metadata.RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(EnumerableConvention.COST_MULTIPLIER);
        }

        /// <inheritdoc />
        public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            // the same map, so what this side stashes reaches the DataContext the plan is bound with
            var clr = new ClrEnumerableRelImplementor(implementor.getRexBuilder(), implementor.map);

            // a correlation variable of a correlate above this node is a parameter of the Java lambda that
            // correlate generates, and the sub-plan cannot see it: it is compiled apart from that lambda.
            // So each one the sub-plan reads is handed in through the DataContext, as a row of the ARRAY
            // format whose fields Calcite's own getter reads out of the parameter, and the sub-plan reads it
            // back as it would the outer row of a correlate of its own. The field reads that getter declares
            // land in the block the correlate's lambda holds, which is where this block is placed too.
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
                    // boxed, because a primitive cannot sit in an Object[] and Janino does not box for an
                    // array initializer where javac would
                    var field = getter.field(builder, i, null);
                    fields.add(J.Primitive.@is(field.getType()) ? J.Expressions.box(field) : field);
                }

                names.add(J.Expressions.constant(name));
                rows.add(J.Expressions.newArrayInit((java.lang.reflect.Type)(java.lang.Class)typeof(object), (java.lang.Iterable)fields));
            }

            var result = clr.VisitChild(null, 0, (ClrEnumerableRel)getInput(), ClrEnumerablePrefers.FromCalcite(pref));

            foreach (var (name, _) in variables)
                clr.ClearCorrelVariable(name);

            var expression = result.Expression;
            if (variables.Count > 0)
            {
                // the outer rows first, read off the context, then the field reads the getter declared
                // over them, then the sub-plan
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

            // the tree, not a delegate. Compiling here would be JIT work done while the plan is still being
            // assembled, and once per converter besides; ClrPlan compiles itself the first time it is run.
            var plan = new ClrPlan<IEnumerable>(
                Expression.Lambda<Func<DataContext, IEnumerable>>(
                    Expression.Convert(expression, typeof(IEnumerable)),
                    clr.Root));

            // stashed as an Object, because the generated source declares the variable by the type's name
            // and cannot name a generic instantiation -- see JavaPlans
            var stashed = implementor.stash(plan, (java.lang.Class)typeof(java.lang.Object));

            // their convention's row abstraction, built from the three values ours carries, because that
            // is what EnumerableRelImplementor.result takes -- and it casts to PhysTypeImpl besides
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
        /// <see cref="JavaPlans.Bind"/>, which runs the compiled sub-plan and reads it as a linq4j sequence.
        /// </summary>
        static readonly java.lang.reflect.Method BindMethod = ((java.lang.Class)typeof(JavaPlans))
            .getDeclaredMethod(nameof(JavaPlans.Bind), [typeof(java.lang.Object), typeof(DataContext)]);

        /// <summary>
        /// <see cref="JavaPlans.BindCorrelated"/>, which is <see cref="JavaPlans.Bind"/> with the outer rows
        /// of the correlation variables the sub-plan reads.
        /// </summary>
        static readonly java.lang.reflect.Method BindCorrelatedMethod = ((java.lang.Class)typeof(JavaPlans))
            .getDeclaredMethod(nameof(JavaPlans.BindCorrelated), [typeof(java.lang.Object), typeof(DataContext), typeof(string[]), typeof(object[])]);

        /// <summary>
        /// <c>DataContext.get</c>, which a correlation variable handed in through the context is read by.
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
