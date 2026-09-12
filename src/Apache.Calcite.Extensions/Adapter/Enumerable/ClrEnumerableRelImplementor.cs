using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Linq4j.Function;
using Apache.Calcite.Extensions.Linq4j.Tree;
using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Turns a tree of <see cref="ClrEnumerableRel"/> into the expression that runs it.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumerableRelImplementor</c>, and used the same way: one instance
    /// implements one plan. A node reaches its inputs through <see cref="VisitChild"/> and returns a
    /// <see cref="ClrEnumerableResult"/>; <see cref="ImplementRoot"/> does the whole plan at once.
    ///
    /// <para>This is how a plan of this convention is run: cast the planned root to
    /// <see cref="ClrEnumerableRel"/>, pass it to <see cref="ImplementRoot"/>, and compile the lambda that
    /// comes back into a <c>Func&lt;DataContext, IEnumerable&lt;object&gt;&gt;</c>, or a
    /// <c>Func&lt;DataContext, IAsyncEnumerable&lt;object&gt;&gt;</c> where <see cref="Async"/>.</para>
    ///
    /// <para><b>The implementor carries the mode, and it is the only thing that does.</b> One plan implements
    /// either way: <see cref="Methods"/> answers the operators over the sequence being built and
    /// <see cref="Call"/> writes the call, so a node's <see cref="ClrEnumerableRel.Implement"/> is the same
    /// code both times. Nothing about a <em>row</em> changes with the mode — the physical type, the Rex
    /// translation, the correlation variables and the stash are the same — which is why the mode is here and
    /// not in the convention, the rules or the plan.</para>
    /// </remarks>
    public class ClrEnumerableRelImplementor : IClrRelImplementor
    {

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, CorrelInputGetter> corrVars = [];
        readonly bool async;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters) :
            this(rexBuilder, internalParameters, false)
        {

        }

        /// <summary>
        /// Initializes a new instance building a plan of the given kind of sequence.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        /// <param name="async">Whether the plan yields an
        /// <see cref="IAsyncEnumerable{T}"/> rather than an <see cref="IEnumerable{T}"/>.</param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, bool async) :
            this(rexBuilder, internalParameters, Expression.Parameter(typeof(DataContext), "root"), async)
        {

        }

        /// <summary>
        /// Initializes a new instance implementing a sub-plan of a plan already being implemented.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        /// <param name="root">The parameter the <see cref="DataContext"/> arrives by, which must be the one
        /// the enclosing plan's lambda declares.</param>
        /// <remarks>
        /// A converter whose two sides are both <see cref="System.Linq.Expressions"/> splices the sub-plan
        /// into the tree it is building rather than compiling it separately, so the sub-plan has to read the
        /// <see cref="DataContext"/> by the parameter that tree already has. An implementor that made its own
        /// would produce an expression referring to a parameter the enclosing lambda does not declare, which
        /// fails at <c>Compile</c> rather than here.
        /// </remarks>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root) :
            this(rexBuilder, internalParameters, root, false)
        {

        }

        /// <summary>
        /// Initializes a new instance building a sub-plan of the given kind, of a plan already being
        /// implemented.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        /// <param name="root">The parameter the <see cref="DataContext"/> arrives by, which must be the one
        /// the enclosing plan's lambda declares.</param>
        /// <param name="async">Whether this sub-plan yields an <see cref="IAsyncEnumerable{T}"/> rather than
        /// an <see cref="IEnumerable{T}"/>.</param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root, bool async)
        {
            this.rexBuilder = rexBuilder ?? throw new ArgumentNullException(nameof(rexBuilder));
            this.map = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));
            this.async = async;

            Root = root ?? throw new ArgumentNullException(nameof(root));
            Translator = new LixToClrTranslator(map);
            Translator.Bind(DataContext.ROOT, Root);

            AllCorrelateVariables = new DelegateFunction1<string, RexToLixTranslator.InputGetter>(GetCorrelVariableGetter);
        }

        /// <summary>
        /// Gets whether this implementor builds a plan yielding an <see cref="IAsyncEnumerable{T}"/> rather
        /// than an <see cref="IEnumerable{T}"/>.
        /// </summary>
        /// <remarks>
        /// A node reads this only where the mode changes something other than which operator a call lands on
        /// — which table a leaf reads, or which way a converter carries rows. Everything else goes through
        /// <see cref="Call"/> and does not need to know.
        /// </remarks>
        public bool Async => async;

        /// <summary>
        /// Gets the operators a node of this plan builds its calls from.
        /// </summary>
        /// <remarks>
        /// <see cref="ClrBuiltInMethod.Enumerable"/> or <see cref="ClrBuiltInMethod.AsyncEnumerable"/>,
        /// which carry the same member names over the two operator sets.
        /// </remarks>
        public ClrBuiltInMethod Methods => async ? ClrBuiltInMethod.AsyncEnumerable : ClrBuiltInMethod.Enumerable;

        /// <summary>
        /// Builds a call to one of <see cref="Methods"/>.
        /// </summary>
        /// <param name="method">The operator, with its type arguments already applied.</param>
        /// <param name="arguments">The arguments, less the cancellation token an asynchronous operator
        /// ends in.</param>
        /// <returns></returns>
        /// <remarks>
        /// What a node writes where its Calcite original writes <c>Expressions.call</c>. The token an
        /// asynchronous operator takes is appended here rather than at the call site, so that the two modes
        /// are the same code; <see cref="ClrBuiltInMethod.Call"/> says why the value is <c>default</c>.
        /// </remarks>
        public MethodCallExpression Call(System.Reflection.MethodInfo method, params Expression[] arguments)
        {
            return Methods.Call(method, arguments);
        }

        /// <summary>
        /// Returns the type of a sequence of the given rows, of the kind this plan is built from.
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public Type SequenceType(Type rowType)
        {
            return Methods.SequenceType(rowType);
        }

        /// <summary>
        /// Gets the builder for row expressions.
        /// </summary>
        public RexBuilder RexBuilder => rexBuilder;

        /// <summary>
        /// Gets the type factory, which decides what every field value is.
        /// </summary>
        public JavaTypeFactory TypeFactory => (JavaTypeFactory)rexBuilder.getTypeFactory();

        /// <summary>
        /// Gets the parameter the <see cref="DataContext"/> arrives by.
        /// </summary>
        public ParameterExpression Root { get; }

        /// <summary>
        /// Gets the linq4j expression standing for the <see cref="DataContext"/>, to hand to a generator of
        /// Calcite's that needs one.
        /// </summary>
        /// <remarks>
        /// Bound to <see cref="Root"/>, so a translated tree reads the argument the plan was called with.
        /// </remarks>
        public J.ParameterExpression RootExpression => DataContext.ROOT;

        /// <summary>
        /// Gets the translator that turns a linq4j expression into a CLR one. One serves the whole plan, so a
        /// variable means the same thing wherever a node mentions it.
        /// </summary>
        internal LixToClrTranslator Translator { get; }


        /// <inheritdoc />
        /// <remarks>
        /// Explicit, because <see cref="Translator"/> is internal and an interface member has to be
        /// implemented publicly otherwise. The translator is not part of this class's surface — a node reads
        /// it, and nothing outside the assembly has any use for one.
        /// </remarks>
        LixToClrTranslator IClrRelImplementor.Translator => Translator;

        /// <summary>
        /// Gets the internal parameters, which reach the query through the <see cref="DataContext"/> it is
        /// bound with rather than through the plan.
        /// </summary>
        public java.util.Map Map => map;

        /// <summary>
        /// Gets the lookup from a correlation variable's name to its getter, to hand to a generator of
        /// Calcite's that translates row expressions.
        /// </summary>
        public Function1 AllCorrelateVariables { get; }

        /// <summary>
        /// Implements one input of a node.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's plan, physical type and row format.</returns>
        public ClrEnumerableResult VisitChild(ClrEnumerableRel? parent, int ordinal, ClrEnumerableRel child, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return Implement(child, prefer);
        }

        /// <summary>
        /// Implements one node, and reads what it hands up across where that is not the kind of sequence
        /// being built.
        /// </summary>
        /// <param name="node"></param>
        /// <param name="prefer"></param>
        /// <returns></returns>
        /// <remarks>
        /// The one place <see cref="ClrEnumerableRel.ImplementAsync"/> is called, and the one place a plan of
        /// one kind of sequence meets a node that builds the other. A node built through <see cref="Call"/>
        /// hands up the kind being built and nothing happens here; a node that can only do one kind is read
        /// across, which is a <em>node</em> boundary rather than a plan boundary, so a leaf that can only
        /// await sits under a synchronous plan with everything above it unchanged, and the reverse.
        ///
        /// <para>What that costs runs one way only. Reading a synchronous sequence as an asynchronous one
        /// costs a state machine and no thread, because the source is pulled and nothing suspends. Reading an
        /// asynchronous one synchronously <b>blocks a thread per row</b>, because an
        /// <see cref="IEnumerable{T}"/> has nowhere to suspend and no wrapper invents one. The rows
        /// themselves are not touched either way: both kinds of plan ask the same type factory what a field
        /// is, so the physical type comes back unchanged and the crossing is the one call.</para>
        /// </remarks>
        ClrEnumerableResult Implement(ClrEnumerableRel node, ClrEnumerablePrefer prefer)
        {
            var result = async ? node.ImplementAsync(this, prefer) : node.Implement(this, prefer);

            if (Methods.IsSequence(result.Expression))
                return result;

            return new ClrEnumerableResult(
                Call(Methods.Bridge.MakeGenericMethod(result.PhysType.RowType), result.Expression),
                result.PhysType,
                result.Format);
        }

        /// <summary>
        /// Implements a whole plan as a function of the <see cref="DataContext"/> it will be bound with.
        /// </summary>
        /// <param name="rootRel">The root of the plan, which must be of this convention.</param>
        /// <param name="prefer">How the caller wants rows represented.</param>
        /// <returns>
        /// A lambda of one <see cref="DataContext"/> parameter whose value is the rows.
        /// <see cref="System.Linq.Expressions.LambdaExpression.Compile()"/> gives a
        /// <c>Func&lt;DataContext, IEnumerable&lt;object&gt;&gt;</c>.
        /// </returns>
        /// <exception cref="java.lang.IllegalStateException">
        /// A node of the plan could not be implemented. The message names the plan; the failure itself is the
        /// inner exception.
        /// </exception>
        public LambdaExpression ImplementRoot(ClrEnumerableRel rootRel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(rootRel);

            ClrEnumerableResult result;

            try
            {
                result = Implement(rootRel, prefer);
            }
            catch (Exception e)
            {
                throw new java.lang.IllegalStateException(
                    $"Unable to implement {org.apache.calcite.plan.RelOptUtil.toString(rootRel, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}", e);
            }

            // a one column result is the value, not a one element row, which is what every caller of a query
            // expects and what EnumerableRelImplementor arranges the same way
            if (prefer == ClrEnumerablePrefer.Array
                && result.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
                result = new ClrEnumerableResult(
                    // object, because nothing reads this but the caller of the query, and it is handed out as
                    // a bare sequence
                    Call(Methods.Slice0.MakeGenericMethod(typeof(object)), result.Expression),
                    result.PhysType,
                    JavaRowFormat.SCALAR);

            // IEnumerable<object>, not the non-generic IEnumerable. Every IEnumerable<T> converts to the
            // latter, so a sequence of the wrong element type would still compile and nothing would say so.
            // The element type is named here for the same reason a node's is named in RequireRowType.
            //
            // The conversion is by variance and cannot fail: a row is never a value type. ClrPhysTypeImpl
            // boxes what the type factory answers, so RowType is a synthetic record, an Object[], a List or
            // a box class; RequireRowType holds every node to it; and the one other shape reaching here is
            // Slice0<object>. There was a boxing pass in front of this for a while, and it was unreachable
            // on every path -- the boxing it looked for has already happened in the physical type.
            var rows = SequenceType(typeof(object));

            return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(DataContext), rows),
                Expression.Convert(result.Expression, rows),
                Root);
        }

        /// <summary>
        /// Returns the expression by which a plan reaches an object that cannot be written into it.
        /// </summary>
        /// <param name="input">The object.</param>
        /// <param name="clazz">The type to give the expression.</param>
        /// <returns>An expression whose value is <paramref name="input"/>.</returns>
        /// <remarks>
        /// The counterpart of <c>EnumerableRelImplementor.stash</c>, which passes the object through the
        /// <see cref="DataContext"/>; an expression tree can hold it, so this is a constant.
        /// </remarks>
        public Expression Stash(object? input, java.lang.Class clazz)
        {
            return Expression.Constant(input, ClrTypes.FromClass(clazz));
        }

        /// <summary>
        /// Registers the variable a correlated sub-query reads its outer row by, for the length of that
        /// sub-query.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <param name="pe">The parameter holding the outer row.</param>
        /// <param name="corrBlock">The block a field read is declared into.</param>
        /// <param name="physType">The outer row's physical type.</param>
        public void RegisterCorrelVariable(string name, J.ParameterExpression pe, J.BlockBuilder corrBlock, PhysType physType)
        {
            corrVars[name] = new CorrelInputGetter(name, pe, corrBlock, physType);
        }

        /// <summary>
        /// Forgets a correlation variable once its scope has ended.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <exception cref="java.lang.IllegalStateException">No such variable is in scope.</exception>
        public void ClearCorrelVariable(string name)
        {
            if (corrVars.Remove(name) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");
        }

        /// <summary>
        /// Returns the getter that reads a field of the row a correlation variable stands for.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <returns></returns>
        /// <exception cref="java.lang.IllegalStateException">No such variable is in scope.</exception>
        public RexToLixTranslator.InputGetter GetCorrelVariableGetter(string name)
        {
            if (corrVars.TryGetValue(name, out var getter) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");

            return getter;
        }

        /// <summary>
        /// Registers on Calcite's implementor every correlation variable in scope here.
        /// </summary>
        /// <param name="enumerable"></param>
        /// <remarks>
        /// A converter runs an <c>EnumerableConvention</c> sub-plan on an implementor of Calcite's, and that
        /// implementor keeps its own correlation variables. So a sub-plan of Calcite's sitting under a
        /// correlate of this convention finds none, and <c>RexToLixTranslator.visitFieldAccess</c> reads a
        /// null getter — a recursive query whose step is a correlate over a transient table is one, because
        /// the transient scan is interpreted and only Calcite has a node for that.
        ///
        /// <para>The registration is replayed rather than the getter handed over, because Calcite builds its
        /// own getter and keeps the map private. It is the same registration: <c>registerCorrelVariable</c>
        /// appends a field read to the block it is given, and the block is the one this convention's
        /// correlate will translate.</para>
        /// </remarks>
        internal void ReplayCorrelVariables(EnumerableRelImplementor enumerable)
        {
            foreach (var pair in corrVars)
                enumerable.registerCorrelVariable(pair.Key, pair.Value.Parameter, pair.Value.Block, pair.Value.PhysType);
        }

        /// <summary>
        /// Creates the result a node's <c>Implement</c> returns.
        /// </summary>
        /// <param name="physType">How the rows are represented.</param>
        /// <param name="expression">The plan, whose value is the rows.</param>
        /// <returns></returns>
        public ClrEnumerableResult Result(ClrPhysType physType, Expression expression)
        {
            RequireRowType(physType, expression);

            // PhysTypeImpl keeps its format package-private, and getFormat is the same value in public
            return new ClrEnumerableResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Refuses a sequence whose rows are not of the type the physical type says they are.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="expression"></param>
        /// <exception cref="java.lang.IllegalStateException">The two disagree.</exception>
        /// <remarks>
        /// Calcite's <c>EnumerableRelImplementor.result</c> records what it is handed and asks nothing, its
        /// sequences being erased. This asks, because a node here can build a sequence of the wrong type and
        /// every node below it will still compile.
        ///
        /// <para>It refuses rather than repairing. Converting the sequence would leave the node wrong and the
        /// plan right, at the cost of a delegate per row, and would hide the next node written that way — as
        /// it did: this method boxed for a while, and three nodes were wrong underneath it with every test
        /// passing.</para>
        ///
        /// <para>A result that is not an <see cref="IEnumerable{T}"/> at all is refused as well. Letting one
        /// through was how seven more nodes would have escaped had any of them handed up an array or an
        /// <c>IOrderedEnumerable</c>: a check with a way out is a check only for the shapes that already
        /// pass.</para>
        /// </remarks>
        void RequireRowType(ClrPhysType physType, Expression expression)
        {
            var expected = physType.RowType;

            // either kind is accepted here, and the caller reads across what does not match: a node is
            // entitled to build only one of the two, and knowing which it built is that caller's business
            // rather than the node's. What is not negotiable is the row.
            if (ClrBuiltInMethod.Enumerable.IsSequence(expression) == false
                && ClrBuiltInMethod.AsyncEnumerable.IsSequence(expression) == false)
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where a sequence of {expected} was wanted.");

            var actual = expression.Type.GetGenericArguments()[0];
            if (actual == expected)
                return;

            throw new java.lang.IllegalStateException($"{Node()} handed up a sequence of {actual} where its row type is {expected}.");
        }

        /// <summary>
        /// Names the node whose <c>Implement</c> called <see cref="Result"/>, for a refusal's message.
        /// </summary>
        /// <returns></returns>
        static string Node()
        {
            var trace = new System.Diagnostics.StackTrace();

            for (int i = 1; i < trace.FrameCount; i++)
            {
                var type = trace.GetFrame(i)?.GetMethod()?.DeclaringType;
                if (type != null && type != typeof(ClrEnumerableRelImplementor))
                    return type.Name;
            }

            return "?";
        }

        /// <summary>
        /// Gets the SQL conformance the query is being planned under.
        /// </summary>
        public SqlConformance Conformance => (SqlConformance)map.getOrDefault("_conformance", SqlConformanceEnum.DEFAULT);

        /// <summary>
        /// Reads a field of the outer row a correlated sub-query was entered with.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="pe"></param>
        /// <param name="corrBlock"></param>
        /// <param name="physType"></param>
        sealed class CorrelInputGetter(string name, J.ParameterExpression pe, J.BlockBuilder corrBlock, PhysType physType) : RexToLixTranslator.InputGetter
        {

            /// <summary>
            /// Gets the parameter holding the outer row.
            /// </summary>
            public J.ParameterExpression Parameter => pe;

            /// <summary>
            /// Gets the block a field read is declared into.
            /// </summary>
            public J.BlockBuilder Block => corrBlock;

            /// <summary>
            /// Gets the outer row's physical type.
            /// </summary>
            public PhysType PhysType => physType;

            /// <inheritdoc />
            public J.Expression field(J.BlockBuilder list, int index, java.lang.reflect.Type storageType)
            {
                return corrBlock.append(name + "_" + index, physType.fieldReference(pe, index, storageType));
            }

        }

    }

}
