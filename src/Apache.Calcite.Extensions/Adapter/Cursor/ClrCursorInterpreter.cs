using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.interpreter;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Reads the rows of a relational expression that only the interpreter can run, as one of the
    /// <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableInterpreter</c>. Some nodes, such as a scan of a <c>FilterableTable</c>,
    /// <c>ProjectableFilterableTable</c> or transient table, have no implementation in any compiling convention;
    /// Calcite's <c>Interpreter</c>, constructed with the <c>DataContext</c> and the input node, runs them row by
    /// row. Its rows are converted through <c>JavaCursors.FromJava</c>, which acquires the interpreter's
    /// enumerator when the node's cursor is opened.
    ///
    /// <para>Only <see cref="Implement"/> is written. A linq4j <c>Enumerator</c> cannot be awaited, and the
    /// node does not visit its input, so the default <see cref="ClrCursorRel.ImplementAsync"/>, which wraps the
    /// synchronous open, is correct.</para>
    /// </remarks>
    public class ClrCursorInterpreter : SingleRel, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorInterpreter"/>.
        /// </summary>
        /// <param name="input">The node to interpret, in <c>BindableConvention</c>.</param>
        /// <param name="factor">The multiplier applied to this node's cost.</param>
        /// <returns>The new node.</returns>
        public static ClrCursorInterpreter Create(RelNode input, double factor)
        {
            var traitSet = input.getTraitSet().replace(ClrCursorConvention.Instance);

            return new ClrCursorInterpreter(input.getCluster(), traitSet, input, factor);
        }

        readonly double factor;

        /// <summary>
        /// Initializes a new instance. <see cref="Create"/> is preferred, as it derives the trait set.
        /// </summary>
        /// <param name="cluster">The cluster.</param>
        /// <param name="traitSet">The trait set, which must carry <see cref="ClrCursorConvention"/>.</param>
        /// <param name="input">The node to interpret.</param>
        /// <param name="factor">The multiplier applied to this node's cost.</param>
        public ClrCursorInterpreter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, double factor) :
            base(cluster, traitSet, input)
        {
            if (getConvention() is not ClrCursorConvention)
                throw new java.lang.AssertionError();

            this.factor = factor;
        }

        /// <inheritdoc />
        public override RelOptCost? computeSelfCost(RelOptPlanner planner, RelMetadataQuery mq)
        {
            var cost = base.computeSelfCost(planner, mq);

            return cost?.multiplyBy(factor);
        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorInterpreter(getCluster(), traitSet, (RelNode)sole(inputs), factor);
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.ARRAY);

            // Calcite writes new Interpreter(root, stash(input)); Stash gives the equivalent expression here
            Expression source = Expression.New(
                InterpreterConstructor,
                implementor.Root,
                implementor.Stash(getInput(), (java.lang.Class)typeof(RelNode)));

            // a one-column row is the value itself: an ARRAY format of one column optimises to SCALAR. The rows
            // are sliced on the Java side, as Calcite does, and FromJava converts each value to the physical
            // type's row type
            if (getRowType().getFieldCount() == 1)
                source = Expression.Call(null, Slice0, source);

            var rowType = physType.RowType;

            return implementor.Result(physType,
                Expression.Call(null, ClrCursorBuiltInMethod.FromJava.MakeGenericMethod(rowType), source));
        }

        /// <summary>
        /// <c>Interpreter(DataContext, RelNode)</c>.
        /// </summary>
        static readonly System.Reflection.ConstructorInfo InterpreterConstructor =
            typeof(Interpreter).GetConstructor([typeof(org.apache.calcite.DataContext), typeof(RelNode)])
            ?? throw new System.InvalidOperationException("Interpreter has no (DataContext, RelNode) constructor.");

        /// <summary>
        /// <c>Linq4j.slice0</c>, which reads a sequence of one-column rows as a sequence of their values.
        /// </summary>
        static readonly System.Reflection.MethodInfo Slice0 = ClrTypes.Resolve(org.apache.calcite.util.BuiltInMethod.SLICE0.method);

    }

}
