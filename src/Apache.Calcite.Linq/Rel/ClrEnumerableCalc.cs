using System;
using System.Linq.Expressions;

using java.util.function;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Calc"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Calcite fuses the filter and the projection into one anonymous <c>Enumerator</c>, because generated
    /// Java source is the only place it can put a custom enumerator. <see cref="ClrEnumerableDefaults.Calc"/> is that
    /// enumerator, written once as an ordinary iterator and called from the tree, so the plan is the same one
    /// pass over the input.
    /// </remarks>
    public class ClrEnumerableCalc : Calc, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableCalc"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="program"></param>
        /// <returns></returns>
        public static ClrEnumerableCalc Create(RelNode input, RexProgram program)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSet()
                .replace(ClrEnumerableConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.calc(mq, input, program)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.calc(mq, input, program)));

            return new ClrEnumerableCalc(cluster, traitSet, input, program);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="program"></param>
        public ClrEnumerableCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, program)
        {

        }

        /// <inheritdoc />
        public override Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program)
        {
            return new ClrEnumerableCalc(getCluster(), traitSet, child, program);
        }

        /// <summary>
        /// Returns the program's projections with their local references expanded, which is what a collation
        /// is decided against.
        /// </summary>
        /// <returns></returns>
        java.util.List Exps()
        {
            var program = getProgram();
            var exps = new java.util.ArrayList();
            for (int i = 0; i < program.getProjectList().size(); i++)
                exps.add(program.expandLocalRef((RexLocalRef)program.getProjectList().get(i)));

            return exps;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair passThroughTraits(RelTraitSet required)
        {
            return ClrEnumerableTraitsUtils.PassThroughTraitsForProject(
                required,
                Exps(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet())!;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrEnumerableTraitsUtils.DeriveTraitsForProject(
                childTraits,
                childId,
                Exps(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet())!;
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = PhysTypeImpl.of(typeFactory, getRowType(), pref.Prefer(result.Format));

            var inputJavaType = result.PhysType.getJavaRowType();
            var inputType = result.PhysType.RowType();
            var outputType = physType.RowType();

            var rexBuilder = getCluster().getRexBuilder();
            var mq = getCluster().getMetadataQuery();
            var predicates = mq.getPulledUpPredicates(child);
            var simplify = new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
            var program = base.program.normalize(rexBuilder, simplify);

            var predicateType = typeof(Func<,>).MakeGenericType(inputType, typeof(bool));
            Expression predicate = Expression.Constant(null, predicateType);

            if (program.getCondition() != null)
            {
                // one row parameter per lambda, where Calcite has one for the whole enumerator
                var row = J.Expressions.parameter(inputJavaType, "row");
                var parameter = Expression.Parameter(inputType, "row");
                implementor.Translator.Bind(row, parameter);

                var builder = new J.BlockBuilder();
                var condition = RexToLixTranslator.translateCondition(
                    program,
                    typeFactory,
                    builder,
                    new RexToLixTranslator.InputGetterImpl(row, result.PhysType),
                    implementor.AllCorrelateVariables,
                    implementor.Conformance);
                builder.add(J.Expressions.return_(null, condition));

                predicate = Expression.Lambda(
                    predicateType,
                    implementor.Translator.TranslateBody(builder.toBlock(), typeof(bool)),
                    parameter);
            }

            var projectRow = J.Expressions.parameter(inputJavaType, "row");
            var projectParameter = Expression.Parameter(inputType, "row");
            implementor.Translator.Bind(projectRow, projectParameter);

            var projectBuilder = new J.BlockBuilder();
            var expressions = RexToLixTranslator.translateProjects(
                program,
                typeFactory,
                implementor.Conformance,
                projectBuilder,
                null,
                physType,
                DataContext.ROOT,
                new RexToLixTranslator.InputGetterImpl(projectRow, result.PhysType),
                implementor.AllCorrelateVariables);
            projectBuilder.add(J.Expressions.return_(null, physType.record(expressions)));

            var selector = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(inputType, outputType),
                implementor.Translator.TranslateBody(projectBuilder.toBlock(), outputType),
                projectParameter);

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.Calc.MakeGenericMethod(inputType, outputType), result.Expression, predicate, selector));
        }

    }

}
