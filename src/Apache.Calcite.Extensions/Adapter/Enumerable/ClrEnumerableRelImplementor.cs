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
    /// implements one plan.
    ///
    /// <para><b>There are two call hierarchies here and they are parallel, not one hierarchy over a
    /// flag.</b> <see cref="ImplementRoot"/> and <see cref="VisitChild"/> are the pulled one, and they call
    /// only <see cref="ClrEnumerableRel.Implement"/>; <see cref="ImplementRootAsync"/> and
    /// <see cref="VisitChildAsync"/> are the awaiting one, and they call only
    /// <see cref="ClrEnumerableRel.ImplementAsync"/>. A body calls the visit of its own kind, so the kind
    /// is settled statically at every step and this class holds no mode. There was a <c>bool async</c>
    /// field for a while and it was the same dispatch the operator tables had already been rid of: it made
    /// a node's inputs a runtime question and forced every result to be type-tested on the way back.</para>
    ///
    /// <para>So to run a plan of this convention: cast the planned root to
    /// <see cref="ClrEnumerableRel"/>, pass it to whichever root member you want, and compile the lambda
    /// that comes back into a <c>Func&lt;DataContext, IEnumerable&lt;object&gt;&gt;</c> or a
    /// <c>Func&lt;DataContext, IAsyncEnumerable&lt;object&gt;&gt;</c>. One instance serves both, and the
    /// same planned tree serves both.</para>
    ///
    /// <para>Nothing about a <em>row</em> differs between the two: the physical type, the Rex translation,
    /// the correlation variables and the stash are shared outright, which is why this is one class with two
    /// hierarchies rather than two classes.</para>
    /// </remarks>
    public class ClrEnumerableRelImplementor : IClrRelImplementor
    {

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, CorrelInputGetter> corrVars = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters) :
            this(rexBuilder, internalParameters, Expression.Parameter(typeof(DataContext), "root"))
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
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root)
        {
            this.rexBuilder = rexBuilder ?? throw new ArgumentNullException(nameof(rexBuilder));
            this.map = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));

            Root = root ?? throw new ArgumentNullException(nameof(root));
            Translator = new LixToClrTranslator(map);
            Translator.Bind(DataContext.ROOT, Root);

            AllCorrelateVariables = new DelegateFunction1<string, RexToLixTranslator.InputGetter>(GetCorrelVariableGetter);
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
        /// Implements one input of a node, as a pulled sequence.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's plan, physical type and row format, yielding an
        /// <see cref="IEnumerable{T}"/>.</returns>
        /// <remarks>
        /// What a node's <see cref="ClrEnumerableRel.Implement"/> calls, and it calls the input's
        /// <see cref="ClrEnumerableRel.Implement"/> in turn. The pulled hierarchy is closed: every call in
        /// it reaches a pulled body and answers a pulled sequence, so a body written against the pulled
        /// operators can compose what this returns without asking anything.
        /// </remarks>
        public ClrEnumerableResult VisitChild(ClrEnumerableRel? parent, int ordinal, ClrEnumerableRel child, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.Implement(this, prefer);
        }

        /// <summary>
        /// Implements one input of a node, as an awaited sequence.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's plan, physical type and row format, yielding an
        /// <see cref="IAsyncEnumerable{T}"/>.</returns>
        /// <remarks>
        /// The awaiting counterpart, and the one place <see cref="ClrEnumerableRel.ImplementAsync"/> is
        /// called from a node. The two hierarchies are parallel and separate: a body calls the member of its
        /// own kind and nothing consults a mode, because there is no mode to consult.
        /// </remarks>
        public ClrEnumerableAsyncResult VisitChildAsync(ClrEnumerableRel? parent, int ordinal, ClrEnumerableRel child, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.ImplementAsync(this, prefer);
        }

        /// <summary>
        /// Reads an awaited result across to a pulled one.
        /// </summary>
        /// <param name="result">What the awaiting fork produced.</param>
        /// <returns>The same rows, as an <see cref="IEnumerable{T}"/>.</returns>
        /// <remarks>
        /// A node whose only real body is the awaiting one writes <see cref="ClrEnumerableRel.Implement"/> as
        /// a delegation through this. It <b>blocks a thread per row</b>, because an
        /// <see cref="IEnumerable{T}"/> has nowhere to suspend, so writing it is a decision and it is made
        /// where it can be read.
        ///
        /// <para>Unconditional, and that is the point of there being two result types. The argument is an
        /// awaited result because its type says so, and nothing has to look at the expression to find out.
        /// </para>
        /// </remarks>
        public ClrEnumerableResult Pulled(ClrEnumerableAsyncResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return new ClrEnumerableResult(
                Expression.Call(null, ClrBuiltInMethod.ToEnumerable.MakeGenericMethod(result.PhysType.RowType), result.Expression),
                result.PhysType,
                result.Format);
        }

        /// <summary>
        /// Reads a pulled result across to an awaited one.
        /// </summary>
        /// <param name="result">What the pulled fork produced.</param>
        /// <returns>The same rows, as an <see cref="IAsyncEnumerable{T}"/>.</returns>
        /// <remarks>
        /// The mirror of <see cref="Pulled"/>, and the common one: it is what the default
        /// <see cref="ClrEnumerableRel.ImplementAsync"/> is, and what a node writes when its awaiting body
        /// has to hand a pulled sequence up anyway. <c>ClrEnumerableTableFunctionScan</c> is the one here
        /// that does, because Calcite's own generator builds its window in linq4j and there is nothing to
        /// await. It costs a state machine and no thread.
        /// </remarks>
        public ClrEnumerableAsyncResult Awaited(ClrEnumerableResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return new ClrEnumerableAsyncResult(
                ClrBuiltInMethod.CallAsync(ClrBuiltInMethod.ToAsyncEnumerable.MakeGenericMethod(result.PhysType.RowType), result.Expression),
                result.PhysType,
                result.Format);
        }

        /// <summary>
        /// Implements a whole plan as a function of the <see cref="DataContext"/> it will be bound with,
        /// yielding its rows as an <see cref="IEnumerable{T}"/>.
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
        /// <remarks>
        /// The entry point of the pulled hierarchy, whose counterpart is <see cref="ImplementRootAsync"/>.
        /// Which one a caller calls is the whole of the choice: the convention, the rules, the planned tree
        /// and this instance are the same either way, and nothing between here and a leaf reads a mode
        /// because there is none to read.
        /// </remarks>
        public LambdaExpression ImplementRoot(ClrEnumerableRel rootRel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(rootRel);

            ClrEnumerableResult implemented;

            try
            {
                implemented = rootRel.Implement(this, prefer);
            }
            catch (Exception e)
            {
                throw new java.lang.IllegalStateException(
                    $"Unable to implement {org.apache.calcite.plan.RelOptUtil.toString(rootRel, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}", e);
            }

            var result = implemented;

            // a one column result is the value, not a one element row, which is what every caller of a query
            // expects and what EnumerableRelImplementor arranges the same way
            if (prefer == ClrEnumerablePrefer.Array
                && result.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
                result = new ClrEnumerableResult(
                    // object, because nothing reads this but the caller of the query, and it is handed out as
                    // a bare sequence
                    Expression.Call(null, ClrBuiltInMethod.Slice0.MakeGenericMethod(typeof(object)), result.Expression),
                    result.PhysType,
                    JavaRowFormat.SCALAR);

            return Lambda(result.Expression, typeof(IEnumerable<object>));
        }

        /// <summary>
        /// Implements a whole plan as a function of the <see cref="DataContext"/> it will be bound with,
        /// yielding its rows as an <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        /// <param name="rootRel">The root of the plan, which must be of this convention.</param>
        /// <param name="prefer">How the caller wants rows represented.</param>
        /// <returns>
        /// A lambda of one <see cref="DataContext"/> parameter whose value is the rows.
        /// <see cref="System.Linq.Expressions.LambdaExpression.Compile()"/> gives a
        /// <c>Func&lt;DataContext, IAsyncEnumerable&lt;object&gt;&gt;</c>.
        /// </returns>
        /// <exception cref="java.lang.IllegalStateException">
        /// A node of the plan could not be implemented. The message names the plan; the failure itself is the
        /// inner exception.
        /// </exception>
        public LambdaExpression ImplementRootAsync(ClrEnumerableRel rootRel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(rootRel);

            ClrEnumerableAsyncResult implemented;

            try
            {
                implemented = rootRel.ImplementAsync(this, prefer);
            }
            catch (Exception e)
            {
                throw new java.lang.IllegalStateException(
                    $"Unable to implement {org.apache.calcite.plan.RelOptUtil.toString(rootRel, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}", e);
            }

            var result = implemented;

            if (prefer == ClrEnumerablePrefer.Array
                && result.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
                result = new ClrEnumerableAsyncResult(
                    ClrBuiltInMethod.CallAsync(ClrBuiltInMethod.Slice0Async.MakeGenericMethod(typeof(object)), result.Expression),
                    result.PhysType,
                    JavaRowFormat.SCALAR);

            return Lambda(result.Expression, typeof(IAsyncEnumerable<object>));
        }

        /// <summary>
        /// Wraps a finished result as the lambda a caller compiles.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="rows">The sequence type the lambda returns.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>IEnumerable&lt;object&gt;</c>, not the non-generic <c>IEnumerable</c>. Every
        /// <c>IEnumerable&lt;T&gt;</c> converts to the latter, so a sequence of the wrong element type would
        /// still compile and nothing would say so. The element type is named for the same reason a node's is
        /// named in <c>RequireRowType</c>.
        ///
        /// <para>The conversion is by variance and cannot fail: a row is never a value type.
        /// <c>ClrPhysTypeImpl</c> boxes what the type factory answers, so a row type is a synthetic record,
        /// an <c>Object[]</c>, a <c>List</c> or a box class; <c>RequireRowType</c> holds every node to it;
        /// and the one other shape reaching here is a sliced <c>object</c>. There was a boxing pass in front
        /// of this for a while, and it was unreachable on every path, the boxing it looked for having
        /// already happened in the physical type.</para>
        /// </remarks>
        LambdaExpression Lambda(Expression sequence, Type rows)
        {
            return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(DataContext), rows),
                Expression.Convert(sequence, rows),
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
            RequireRowType(physType, expression, typeof(IEnumerable<>));

            // PhysTypeImpl keeps its format package-private, and getFormat is the same value in public
            return new ClrEnumerableResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Builds the result of a node's <see cref="ClrEnumerableRel.ImplementAsync"/>.
        /// </summary>
        /// <param name="physType">What the node's rows are.</param>
        /// <param name="expression">The sequence yielding them, which must be an
        /// <see cref="IAsyncEnumerable{T}"/> of the physical row type.</param>
        /// <returns></returns>
        /// <exception cref="java.lang.IllegalStateException">The sequence is not that.</exception>
        /// <remarks>
        /// <see cref="Result"/> for the awaiting fork. The kind is required here rather than inferred later:
        /// a node that builds a pulled sequence in its awaiting body is refused by name, instead of being
        /// quietly wrapped and costing a thread per row that nobody asked for. Where the wrap is wanted the
        /// node says so with <see cref="Awaited"/>.
        /// </remarks>
        public ClrEnumerableAsyncResult ResultAsync(ClrPhysType physType, Expression expression)
        {
            RequireRowType(physType, expression, typeof(IAsyncEnumerable<>));

            return new ClrEnumerableAsyncResult(expression, physType, physType.Format);
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
        /// <para>A result that is not a sequence at all is refused as well. Letting one
        /// through was how seven more nodes would have escaped had any of them handed up an array or an
        /// <c>IOrderedEnumerable</c>: a check with a way out is a check only for the shapes that already
        /// pass.</para>
        ///
        /// <para><paramref name="wanted"/> is not a mode. Each of the two result factories passes its own
        /// kind, statically, because each belongs to one fork; the node being checked has already chosen
        /// which factory to call by choosing which body to write it in.</para>
        /// </remarks>
        static void RequireRowType(ClrPhysType physType, Expression expression, Type wanted)
        {
            var expected = physType.RowType;
            var definition = expression.Type.IsGenericType ? expression.Type.GetGenericTypeDefinition() : null;

            if (definition != wanted)
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where a {wanted.Name} of {expected} was wanted.");

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
