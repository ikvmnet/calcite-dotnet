using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.interpreter;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.metadata;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Reads the rows of a relational expression that only the interpreter can run, as one of the
    /// <see cref="ClrEnumerableConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableInterpreter</c>, and the same node: a plan can hold something no
    /// convention implements — a scan of a <c>FilterableTable</c>, a <c>ProjectableFilterableTable</c> or a
    /// transient table — and Calcite's <c>Interpreter</c> walks it row by row. Nothing about the interpreting
    /// is ours; it is Calcite's class, constructed with the <c>DataContext</c> and the node.
    ///
    /// <para>So the rows arrive as Java <c>Object[]</c> and cross into this convention the way every other
    /// sequence of Calcite's does, through <c>JavaSequences.FromJava</c>. Without this node the same plan
    /// runs with <c>EnumerableInterpreter</c> under <c>EnumerableToClrEnumerableConverter</c> — one more
    /// convention boundary for the same rows.</para>
    /// </remarks>
    public class ClrEnumerableInterpreter : SingleRel, ClrEnumerableRel
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableInterpreter"/>.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="factor">The multiplier applied to this node's cost.</param>
        /// <returns></returns>
        public static ClrEnumerableInterpreter Create(RelNode input, double factor)
        {
            var traitSet = input.getTraitSet().replace(ClrEnumerableConvention.Instance);

            return new ClrEnumerableInterpreter(input.getCluster(), traitSet, input, factor);
        }

        readonly double factor;

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="input"></param>
        /// <param name="factor"></param>
        public ClrEnumerableInterpreter(RelOptCluster cluster, RelTraitSet traitSet, RelNode input, double factor) :
            base(cluster, traitSet, input)
        {
            if (getConvention() is not ClrEnumerableConvention)
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
            return new ClrEnumerableInterpreter(getCluster(), traitSet, (RelNode)sole(inputs), factor);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), JavaRowFormat.ARRAY);

            // Calcite writes new Interpreter(root, stash(input)) and reads the value back off the DataContext,
            // because Janino compiles source text and source text cannot mention an object. An expression tree
            // holds the node itself, which is what Stash gives.
            Expression source = Expression.New(
                InterpreterConstructor,
                implementor.Root,
                implementor.Stash(getInput(), (java.lang.Class)typeof(RelNode)));

            // a one column result is the value, not a one element row — and sliced on the Java side, where
            // Calcite slices it, so that the sequence crossing the boundary is already the shape the physical
            // type describes. FromJava is then the one conversion, and it converts each value to the type
            // that physical type gives it: an optimised ARRAY of one column is SCALAR, whose Java row type is
            // the column's, and the interpreter's rows hold java.lang.Integer where that says int.
            if (getRowType().getFieldCount() == 1)
                source = Expression.Call(null, Slice0, source);

            var rowType = physType.RowType;

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.FromJava.MakeGenericMethod(rowType), source));
        }

        /// <summary>
        /// <c>Interpreter(DataContext, RelNode)</c>, which is what runs the input.
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
