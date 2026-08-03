using System;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Rel
{

    /// <summary>
    /// Implementation of <see cref="Window"/> in the <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Every line of what a window computes comes from a generator of Calcite's — <c>translateBound</c>,
    /// <c>declareAndResetState</c>, <c>getPartitionIterator</c>, <c>getRowCollationKey</c> and the window
    /// aggregate implementors — and all of them are private. Nothing of it composes sequences; it is one
    /// procedure over an array of partition rows, and this convention would contribute only the input and the
    /// output.
    ///
    /// <para>So it is not rewritten. <c>EnumerableWindow</c> builds the block, reading its input from a
    /// variable this node binds to the child, and the block is translated rather than handed to Janino. That is
    /// what the port is for: the same computation, compiled as an expression tree.</para>
    ///
    /// <para><b>This does not work yet and its rule is not registered.</b> The block it produces reaches parts
    /// of the translator nothing else does. A linq4j call records a Method, but Janino resolves both the
    /// overload and the receiver from the source text it writes, so the recorded method is advisory in both
    /// positions: the block calls <c>Linq4j.asEnumerable</c> naming the array overload with a list, and
    /// <c>size()</c> on a <c>SortedMultiMap</c> naming it on <c>Collection</c>. Overload rebinding by argument
    /// type is in; doing the same for the receiver is not. Nor is the question of where a lambda declared as a
    /// linq4j <c>Function1</c> should be wrapped, which has to be decided by what consumes it.</para>
    /// </remarks>
    public class ClrEnumerableWindow : Window, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="constants"></param>
        /// <param name="rowType"></param>
        /// <param name="groups"></param>
        public ClrEnumerableWindow(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, java.util.List constants, RelDataType rowType, java.util.List groups) :
            base(cluster, traitSet, com.google.common.collect.ImmutableList.of(), input, constants, rowType, groups)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableWindow(getCluster(), traitSet, (RelNode)sole(inputs), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public override Window copy(java.util.List constants)
        {
            return new ClrEnumerableWindow(getCluster(), getTraitSet(), getInput(), constants, getRowType(), groups);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
        {
            var child = (ClrEnumerableRel)getInput();
            var result = implementor.VisitChild(this, 0, child, pref);

            // the variable the generated block reads its input by, bound to what the child produced
            var source_ = J.Expressions.parameter((java.lang.Class)typeof(Enumerable), "clrWindowSource");
            var source = Expression.Variable(typeof(Enumerable), "clrWindowSource");
            implementor.Translator.Bind(source_, source);

            // EnumerableWindow keeps its constructor package private, and its rule is the public way to one.
            // The stub already satisfies the traits the rule converts to, so RelOptRule.convert returns it
            // rather than asking the planner for anything.
            var stub = new SourceRel(getCluster(), getInput().getTraitSet().replace(EnumerableConvention.INSTANCE), result.PhysType, source_);
            var forEnumerable = (Window)copy(getTraitSet(), java.util.Collections.singletonList(stub));
            var window = (EnumerableRel)((org.apache.calcite.rel.convert.ConverterRule)EnumerableRules.ENUMERABLE_WINDOW_RULE).convert(forEnumerable);

            var enumerable = new EnumerableRelImplementor(implementor.RexBuilder, implementor.Map);
            var generated = window.implement(enumerable, pref);

            var rowType = TypeResolver.Resolve(generated.physType.getJavaRowType());
            var sourceType = TypeResolver.Resolve(result.PhysType.getJavaRowType());

            return implementor.Result(generated.physType,
                Expression.Block(
                    [source],
                    Expression.Assign(source,
                        Expression.Call(null, ClrBuiltInMethod.ToJava.MakeGenericMethod(sourceType), result.Expression)),
                    Expression.Call(null,
                        ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType),
                        implementor.Translator.TranslateBody(generated.block, typeof(Enumerable)))));
        }

        /// <summary>
        /// Stands in for the input while <see cref="EnumerableWindow"/> builds its block.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <remarks>
        /// It yields the variable rather than a sub-plan, so the generated block reads whatever this node
        /// assigns to it and nothing of the child is generated twice.
        /// </remarks>
        sealed class SourceRel(RelOptCluster cluster, RelTraitSet traitSet, PhysType physType, J.ParameterExpression source) :
            AbstractRelNode(cluster, traitSet), EnumerableRel
        {

            /// <inheritdoc />
            protected override RelDataType deriveRowType() => physType.getRowType();

            /// <inheritdoc />
            public EnumerableRel.Result implement(EnumerableRelImplementor implementor, EnumerableRel.Prefer pref)
            {
                return new EnumerableRel.Result(J.Blocks.toBlock(source), physType, physType.getFormat());
            }

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

}
