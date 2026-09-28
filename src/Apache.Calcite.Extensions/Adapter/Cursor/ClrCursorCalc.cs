using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using java.util.function;
using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.metadata;
using org.apache.calcite.rex;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Calc"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableCalc</c>, which generates one anonymous <c>Enumerator</c> that filters and projects
    /// in a single pass. <see cref="ClrCursorDefaults.Calc"/> is that enumerator as a cursor, given the
    /// translated condition and projection as delegates.
    /// </remarks>
    public class ClrCursorCalc : Calc, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorCalc"/>, deriving its collation and distribution from the program.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="program">The program: projections and an optional condition.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorCalc Create(RelNode input, RexProgram program)
        {
            var cluster = input.getCluster();
            var mq = cluster.getMetadataQuery();
            var traitSet = cluster.traitSet()
                .replace(ClrCursorConvention.Instance)
                .replaceIfs(RelCollationTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdCollation.calc(mq, input, program)))
                .replaceIf(RelDistributionTraitDef.INSTANCE, new DelegateSupplier<object>(() => RelMdDistribution.calc(mq, input, program)));

            return new ClrCursorCalc(cluster, traitSet, input, program);
        }

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which carries <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The input.</param>
        /// <param name="program">The program: projections and an optional condition.</param>
        public ClrCursorCalc(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, RexProgram program) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, program)
        {

        }

        /// <inheritdoc />
        public override Calc copy(RelTraitSet traitSet, RelNode child, RexProgram program)
        {
            return new ClrCursorCalc(getCluster(), traitSet, child, program);
        }

        /// <summary>
        /// Returns the program's projections with their local references expanded, for trait propagation.
        /// </summary>
        /// <returns>The expanded projections.</returns>
        java.util.List Exps()
        {
            var program = getProgram();
            var exps = new java.util.ArrayList();
            for (int i = 0; i < program.getProjectList().size(); i++)
                exps.add(program.expandLocalRef((RexLocalRef)program.getProjectList().get(i)));

            return exps;
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? passThroughTraits(RelTraitSet required)
        {
            return ClrCursorTraitsUtils.PassThroughTraitsForProject(
                required,
                Exps(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        public org.apache.calcite.util.Pair? deriveTraits(RelTraitSet childTraits, int childId)
        {
            return ClrCursorTraitsUtils.DeriveTraitsForProject(
                childTraits,
                childId,
                Exps(),
                getInput().getRowType(),
                getInput().getCluster().getTypeFactory(),
                getTraitSet());
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.Prefer(result.Format));

            // the condition and projections are translated by Calcite's Rex translator, which takes Calcite's
            // physical types: the input's to read fields, the output's for storage types
            var inputCalcite = PhysTypeImpl.of(typeFactory, result.PhysType.RelRowType, result.PhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var inputJavaType = inputCalcite.getJavaRowType();
            var inputType = result.PhysType.RowType;
            var outputType = physType.RowType;

            var rexBuilder = getCluster().getRexBuilder();
            var mq = getCluster().getMetadataQuery();
            var predicates = mq.getPulledUpPredicates(child);
            var simplify = new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
            var program = base.program.normalize(rexBuilder, simplify);

            var predicateType = typeof(Func<,>).MakeGenericType(inputType, typeof(bool));
            Expression predicate = Expression.Constant(null, predicateType);

            if (program.getCondition() != null)
            {
                // each lambda has its own row parameter, where Calcite's enumerator shares one
                var row = J.Expressions.parameter(inputJavaType, "row");
                var parameter = Expression.Parameter(inputType, "row");
                implementor.Translator.Bind(row, parameter);

                var builder = new J.BlockBuilder();
                var condition = RexToLixTranslator.translateCondition(
                    program,
                    typeFactory,
                    builder,
                    new RexToLixTranslator.InputGetterImpl(row, inputCalcite),
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
                outputCalcite,
                DataContext.ROOT,
                new RexToLixTranslator.InputGetterImpl(projectRow, inputCalcite),
                implementor.AllCorrelateVariables,
                implementor.RexImplementorTable);
            projectBuilder.add(J.Expressions.return_(null, outputCalcite.record(expressions)));

            var selector = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(inputType, outputType),
                implementor.Translator.TranslateBody(projectBuilder.toBlock(), outputType),
                projectParameter);

            return implementor.Result(physType,
                Expression.Call(null, ClrCursorBuiltInMethod.Calc.MakeGenericMethod(inputType, outputType), result.Expression, predicate, selector));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var typeFactory = implementor.TypeFactory;
            var child = (ClrCursorRel)getInput();
            var result = implementor.VisitChildAsync(this, 0, child, pref);
            var physType = ClrPhysTypeImpl.Of(typeFactory, getRowType(), pref.Prefer(result.Format));

            // the condition and projections are translated by Calcite's Rex translator, which takes Calcite's
            // physical types: the input's to read fields, the output's for storage types
            var inputCalcite = PhysTypeImpl.of(typeFactory, result.PhysType.RelRowType, result.PhysType.Format, false);
            var outputCalcite = PhysTypeImpl.of(typeFactory, physType.RelRowType, physType.Format, false);

            var inputJavaType = inputCalcite.getJavaRowType();
            var inputType = result.PhysType.RowType;
            var outputType = physType.RowType;

            var rexBuilder = getCluster().getRexBuilder();
            var mq = getCluster().getMetadataQuery();
            var predicates = mq.getPulledUpPredicates(child);
            var simplify = new RexSimplify(rexBuilder, predicates, RexUtil.EXECUTOR);
            var program = base.program.normalize(rexBuilder, simplify);

            var predicateType = typeof(Func<,>).MakeGenericType(inputType, typeof(bool));
            Expression predicate = Expression.Constant(null, predicateType);

            if (program.getCondition() != null)
            {
                // each lambda has its own row parameter, where Calcite's enumerator shares one
                var row = J.Expressions.parameter(inputJavaType, "row");
                var parameter = Expression.Parameter(inputType, "row");
                implementor.Translator.Bind(row, parameter);

                var builder = new J.BlockBuilder();
                var condition = RexToLixTranslator.translateCondition(
                    program,
                    typeFactory,
                    builder,
                    new RexToLixTranslator.InputGetterImpl(row, inputCalcite),
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
                outputCalcite,
                DataContext.ROOT,
                new RexToLixTranslator.InputGetterImpl(projectRow, inputCalcite),
                implementor.AllCorrelateVariables,
                implementor.RexImplementorTable);
            projectBuilder.add(J.Expressions.return_(null, outputCalcite.record(expressions)));

            var selector = Expression.Lambda(
                typeof(Func<,>).MakeGenericType(inputType, outputType),
                implementor.Translator.TranslateBody(projectBuilder.toBlock(), outputType),
                projectParameter);

            return implementor.ResultAsync(physType,
                ClrCursorBuiltInMethod.CallAsync(implementor, ClrCursorBuiltInMethod.CalcAsync.MakeGenericMethod(inputType, outputType), result.Expression, predicate, selector));
        }

    }

}
