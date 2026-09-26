using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

using Apache.Calcite.Extensions.Adapter.Enumerable;
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

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Turns a tree of <see cref="ClrCursorRel"/> into the factory that opens it.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumerableRelImplementor</c>, and used the same way: one instance
    /// implements one plan. It is <see cref="ClrEnumerableRelImplementor"/> with the sequence replaced by
    /// the open, and everything about a <em>row</em> — the physical type, the Rex translation, the
    /// correlation variables and the stash — is the same code, because a row is the same object.
    ///
    /// <para><b>Two call hierarchies, parallel, and one root member that runs both.</b>
    /// <see cref="VisitChild"/> calls only <see cref="ClrCursorRel.Implement"/> and
    /// <see cref="VisitChildAsync"/> only <see cref="ClrCursorRel.ImplementAsync"/>, so a body always
    /// composes eager inputs of its own kind and this class holds no mode. What differs from the enumerable
    /// convention is that a caller does not choose between two root members: <see cref="ImplementRoot"/>
    /// walks the tree once through each hierarchy and hands back a <see cref="ClrCursorFactory"/>
    /// carrying both opens, because the cursor either open produces is the same cursor and a consumer
    /// chooses per open and per advance rather than per plan.</para>
    ///
    /// <para><b>The awaiting hierarchy has a token, and it is a parameter.</b> <see cref="CancellationToken"/>
    /// is the parameter the awaiting root's lambda declares, and every awaiting open in the tree is passed
    /// it by <see cref="ClrCursorBuiltInMethod.CallAsync"/>. A deferred open built by
    /// <see cref="OpenerAsync"/> declares the same parameter again as its own, so that inside it the token
    /// is the one the advance was given — a nested lambda's declaration shadows the enclosing one's, which
    /// the expression compiler and the interpreter both honour.</para>
    /// </remarks>
    public class ClrCursorRelImplementor : IClrRelImplementor
    {

        /// <summary>
        /// The key a caller's <c>FetchOffsetRoundingPolicy</c> is stashed under.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableRelImplementor.FETCH_OFFSET_ROUNDING_POLICY</c>, spelled the same, because every
        /// convention reads one map and a caller sets one key.
        /// </remarks>
        public const string FetchOffsetRoundingPolicy = "_fetchOffsetRoundingPolicy";

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, CorrelInputGetter> corrVars = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        public ClrCursorRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters) :
            this(rexBuilder, internalParameters, Expression.Parameter(typeof(DataContext), "root"), Expression.Parameter(typeof(CancellationToken), "cancellationToken"))
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
        /// <param name="cancellationToken">The parameter an awaiting open's token arrives by, which must be
        /// the one the enclosing plan's awaiting lambda declares.</param>
        public ClrCursorRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root, ParameterExpression cancellationToken) :
            this(rexBuilder, internalParameters, root, cancellationToken, new LixToClrTranslator(internalParameters))
        {

        }

        /// <summary>
        /// Initializes a new instance implementing a sub-plan of a plan already being implemented, sharing
        /// that plan's translator.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        /// <param name="root">The parameter the <see cref="DataContext"/> arrives by, which must be the one
        /// the enclosing plan's lambda declares.</param>
        /// <param name="cancellationToken">The parameter an awaiting open's token arrives by.</param>
        /// <param name="translator">The enclosing plan's translator.</param>
        /// <remarks>
        /// What a converter between the two conventions of this project builds, for the reason
        /// <see cref="ClrEnumerableRelImplementor"/> gives for its own: a variable a node of the enclosing
        /// plan declared — the field read a correlate appends to its block for a correlation variable — is
        /// referenced by the linq4j parameter's identity, which only the translator that declared it can
        /// map to the CLR variable the enclosing tree holds.
        /// </remarks>
        internal ClrCursorRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root, ParameterExpression cancellationToken, LixToClrTranslator translator)
        {
            this.rexBuilder = rexBuilder ?? throw new ArgumentNullException(nameof(rexBuilder));
            this.map = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));

            Root = root ?? throw new ArgumentNullException(nameof(root));
            CancellationToken = cancellationToken ?? throw new ArgumentNullException(nameof(cancellationToken));
            Translator = translator ?? throw new ArgumentNullException(nameof(translator));
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
        /// Gets the parameter an awaiting open's token arrives by.
        /// </summary>
        /// <remarks>
        /// Declared by the awaiting root's lambda, and again by every deferred open
        /// <see cref="OpenerAsync"/> builds. The synchronous root's lambda does not declare it, and no
        /// synchronous open mentions it.
        /// </remarks>
        public ParameterExpression CancellationToken { get; }

        /// <summary>
        /// Gets the linq4j expression standing for the <see cref="DataContext"/>, to hand to a generator of
        /// Calcite's that needs one.
        /// </summary>
        public J.ParameterExpression RootExpression => DataContext.ROOT;

        /// <summary>
        /// Gets the translator that turns a linq4j expression into a CLR one. One serves the whole plan, so a
        /// variable means the same thing wherever a node mentions it.
        /// </summary>
        internal LixToClrTranslator Translator { get; }

        /// <inheritdoc />
        LixToClrTranslator IClrRelImplementor.Translator => Translator;

        /// <summary>
        /// Gets the internal parameters, which reach the query through the <see cref="DataContext"/> it is
        /// bound with rather than through the plan.
        /// </summary>
        public java.util.Map Map => map;

        /// <summary>
        /// Gets the table of implementors a translated call is written with.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableRelImplementor.getRexImplementorTable</c>: whatever a caller stashed under
        /// <c>_rexImplementorTable</c>, and <c>RexImpTable.INSTANCE</c> where none did.
        /// </remarks>
        public RexImplementorTable RexImplementorTable =>
            (RexImplementorTable)map.getOrDefault("_rexImplementorTable", RexImpTable.INSTANCE);

        /// <summary>
        /// Gets the lookup from a correlation variable's name to its getter, to hand to a generator of
        /// Calcite's that translates row expressions.
        /// </summary>
        public Function1 AllCorrelateVariables { get; }

        /// <summary>
        /// Gets the SQL conformance the query is being planned under.
        /// </summary>
        public SqlConformance Conformance => (SqlConformance)map.getOrDefault("_conformance", SqlConformanceEnum.DEFAULT);

        /// <summary>
        /// Implements one input of a node, as an open that acquires synchronously.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's open, physical type and row format.</returns>
        /// <remarks>
        /// What a node's <see cref="ClrCursorRel.Implement"/> calls for an eager input, and it calls the
        /// input's <see cref="ClrCursorRel.Implement"/> in turn. The synchronous hierarchy is closed: every
        /// call in it reaches a synchronous body and answers a synchronous open.
        /// </remarks>
        public ClrCursorResult VisitChild(ClrCursorRel? parent, int ordinal, ClrCursorRel child, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.Implement(this, prefer);
        }

        /// <summary>
        /// Implements one input of a node, as an open that awaits its acquisition.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's open, physical type and row format.</returns>
        /// <remarks>
        /// The awaiting counterpart, and the one place <see cref="ClrCursorRel.ImplementAsync"/> is
        /// called from a node.
        /// </remarks>
        public ClrCursorAsyncResult VisitChildAsync(ClrCursorRel? parent, int ordinal, ClrCursorRel child, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.ImplementAsync(this, prefer);
        }

        /// <summary>
        /// Reads an awaiting open across to a synchronous one.
        /// </summary>
        /// <param name="result">What the awaiting fork produced.</param>
        /// <returns>The same cursor, opened by blocking.</returns>
        /// <remarks>
        /// A node whose only real body is the awaiting one writes <see cref="ClrCursorRel.Implement"/>
        /// as a delegation through this. It <b>blocks a thread for the length of the acquisition</b>, because
        /// a synchronous open has nowhere to suspend, so writing it is a decision and it is made where it
        /// can be read. The open is wrapped as a delegate so that
        /// <see cref="ClrCursors.Block{T}"/> can suppress the synchronization context before it starts.
        /// </remarks>
        public ClrCursorResult Pulled(ClrCursorAsyncResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return new ClrCursorResult(
                Expression.Call(null, ClrCursorBuiltInMethod.Block.MakeGenericMethod(result.PhysType.RowType), OpenerAsync(result)),
                result.PhysType,
                result.Format);
        }

        /// <summary>
        /// Reads a synchronous open across to an awaiting one.
        /// </summary>
        /// <param name="result">What the synchronous fork produced.</param>
        /// <returns>The same cursor, as a completed open.</returns>
        /// <remarks>
        /// The mirror of <see cref="Pulled"/>, and the cheap one: it is what the default
        /// <see cref="ClrCursorRel.ImplementAsync"/> is, and what a node writes when its awaiting body
        /// has nothing to await on the way to its cursor. It allocates nothing.
        /// </remarks>
        public ClrCursorAsyncResult Awaited(ClrCursorResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return new ClrCursorAsyncResult(
                Expression.Call(null, ClrCursorBuiltInMethod.Completed.MakeGenericMethod(result.PhysType.RowType), result.Expression),
                result.PhysType,
                result.Format);
        }

        /// <summary>
        /// Defers a synchronous open, for an operator that acquires a source later than at its own open.
        /// </summary>
        /// <param name="result">The input's open.</param>
        /// <returns>A <c>Func&lt;ClrCursor&lt;TRow&gt;&gt;</c> that runs the open when called.</returns>
        /// <remarks>
        /// Evaluating an open is the acquisition, so an operator that must not acquire a source at its own
        /// open — linq4j's <c>concat</c> acquires each source at its turn inside <c>moveNext</c> — takes
        /// the source as one of these and calls it when the time comes. The deferral is then visible in the
        /// tree as a lambda, exactly where linq4j's is visible as a field read.
        /// </remarks>
        public LambdaExpression Opener(ClrCursorResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return Expression.Lambda(
                typeof(Func<>).MakeGenericType(result.Expression.Type),
                result.Expression);
        }

        /// <summary>
        /// Defers an awaiting open, for an operator that acquires a source later than at its own open.
        /// </summary>
        /// <param name="result">The input's open.</param>
        /// <returns>A <c>Func&lt;CancellationToken, ValueTask&lt;ClrCursor&lt;TRow&gt;&gt;&gt;</c> that
        /// runs the open when called, under the token it is called with.</returns>
        /// <remarks>
        /// <see cref="Opener"/> for the awaiting fork. The lambda declares <see cref="CancellationToken"/>
        /// as its own parameter, shadowing the root's, so that an acquisition that happens inside
        /// <c>ReadAsync</c> is cancelled by that advance's token rather than by the open's.
        /// </remarks>
        public LambdaExpression OpenerAsync(ClrCursorAsyncResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return Expression.Lambda(
                typeof(Func<,>).MakeGenericType(typeof(CancellationToken), result.Expression.Type),
                result.Expression,
                CancellationToken);
        }

        /// <summary>
        /// Implements a whole plan as a <see cref="ClrCursorFactory"/>, which opens it against the
        /// <see cref="DataContext"/> it is bound with.
        /// </summary>
        /// <param name="rootRel">The root of the plan, which must be of this convention.</param>
        /// <param name="prefer">How the caller wants rows represented.</param>
        /// <returns>The factory, carrying both opens and the type of one row.</returns>
        /// <exception cref="java.lang.IllegalStateException">
        /// A node of the plan could not be implemented. The message names the plan; the failure itself is the
        /// inner exception.
        /// </exception>
        /// <remarks>
        /// Both bodies of the root, each walking its own hierarchy, and then one wrapper around the two.
        /// Where the enumerable convention offers two root members and a caller picks one, this offers one
        /// and builds both, because a plan of this convention has no mode for a caller to pick: the cursor
        /// is the same whichever way it is opened, and which way is the caller's business at each open.
        ///
        /// <para>Nothing is compiled here. The factory compiles each open the first time it is asked for it,
        /// so a caller that only ever opens one way pays for one.</para>
        /// </remarks>
        public ClrCursorFactory ImplementRoot(ClrCursorRel rootRel, ClrEnumerablePrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(rootRel);

            ClrCursorResult pulled;
            ClrCursorAsyncResult awaited;

            try
            {
                pulled = rootRel.Implement(this, prefer);
                awaited = rootRel.ImplementAsync(this, prefer);
            }
            catch (Exception e)
            {
                throw new java.lang.IllegalStateException(
                    $"Unable to implement {org.apache.calcite.plan.RelOptUtil.toString(rootRel, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}", e);
            }

            // what one row of the awaiting open is: the physical row type, until the slice below makes it
            // the value
            var rowType = awaited.PhysType.RowType;

            // a one column result is the value, not a one element row, which is what every caller of a query
            // expects and what EnumerableRelImplementor arranges the same way
            if (prefer == ClrEnumerablePrefer.Array
                && pulled.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
            {
                // object, because nothing reads this but the caller of the query, and it is handed out as
                // an untyped cursor
                rowType = typeof(object);

                pulled = new ClrCursorResult(
                    Expression.Call(null, ClrCursorBuiltInMethod.Slice0.MakeGenericMethod(rowType), pulled.Expression),
                    pulled.PhysType,
                    JavaRowFormat.SCALAR);

                awaited = new ClrCursorAsyncResult(
                    ClrCursorBuiltInMethod.CallAsync(this, ClrCursorBuiltInMethod.Slice0Async.MakeGenericMethod(rowType), awaited.Expression),
                    awaited.PhysType,
                    JavaRowFormat.SCALAR);
            }

            // the conversion to the untyped base is by reference and cannot fail: a row is never a value
            // type, and RequireRowType has held every node to a cursor of its physical row type. The awaiting
            // open goes through one continuation instead, a ValueTask being invariant
            var open = Expression.Lambda<Func<DataContext, ClrCursor>>(
                Expression.Convert(pulled.Expression, typeof(ClrCursor)),
                Root);

            var openAsync = Expression.Lambda<Func<DataContext, CancellationToken, ValueTask<ClrCursor>>>(
                Expression.Call(null, ClrCursorBuiltInMethod.Untyped.MakeGenericMethod(rowType), awaited.Expression),
                Root,
                CancellationToken);

            return new ClrCursorFactory(open, openAsync, ElementType(rootRel, prefer));
        }

        /// <summary>
        /// Returns what one row of the plan is, which is the same answer whichever way it is opened.
        /// </summary>
        Type ElementType(ClrCursorRel rel, ClrEnumerablePrefer prefer)
        {
            return ClrTypes.Resolve(ClrPhysTypeImpl.Of(TypeFactory, rel.getRowType(), prefer.PreferArray()).JavaRowType);
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
        /// implementor keeps its own correlation variables; a sub-plan of Calcite's sitting under a correlate
        /// of this convention would otherwise find none. The registration is replayed rather than the getter
        /// handed over, because Calcite builds its own getter and keeps the map private.
        /// </remarks>
        internal void ReplayCorrelVariables(EnumerableRelImplementor enumerable)
        {
            foreach (var pair in corrVars)
                enumerable.registerCorrelVariable(pair.Key, pair.Value.Parameter, pair.Value.Block, pair.Value.PhysType);
        }

        /// <summary>
        /// Registers on the sequence convention's implementor every correlation variable in scope here.
        /// </summary>
        /// <param name="enumerable"></param>
        internal void ReplayCorrelVariables(ClrEnumerableRelImplementor enumerable)
        {
            foreach (var pair in corrVars)
                enumerable.RegisterCorrelVariable(pair.Key, pair.Value.Parameter, pair.Value.Block, pair.Value.PhysType);
        }

        /// <summary>
        /// Creates the result a node's <see cref="ClrCursorRel.Implement"/> returns.
        /// </summary>
        /// <param name="physType">How the rows are represented.</param>
        /// <param name="expression">The open, whose value must be a <c>ClrCursor&lt;TRow&gt;</c> of the
        /// physical row type.</param>
        /// <returns></returns>
        /// <exception cref="java.lang.IllegalStateException">The open is not that.</exception>
        public ClrCursorResult Result(ClrPhysType physType, Expression expression)
        {
            RequireRowType(physType, expression, typeof(ClrCursor<>), "an open");

            // PhysTypeImpl keeps its format package-private, and getFormat is the same value in public
            return new ClrCursorResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Creates the result a node's <see cref="ClrCursorRel.ImplementAsync"/> returns.
        /// </summary>
        /// <param name="physType">How the rows are represented.</param>
        /// <param name="expression">The open, whose value must be a
        /// <c>ValueTask&lt;ClrCursor&lt;TRow&gt;&gt;</c> of the physical row type.</param>
        /// <returns></returns>
        /// <exception cref="java.lang.IllegalStateException">The open is not that.</exception>
        /// <remarks>
        /// <see cref="Result"/> for the awaiting fork. The kind is required here rather than inferred later:
        /// a node that builds a synchronous open in its awaiting body is refused by name, instead of being
        /// quietly wrapped. Where the wrap is wanted the node says so with <see cref="Awaited"/>.
        /// </remarks>
        public ClrCursorAsyncResult ResultAsync(ClrPhysType physType, Expression expression)
        {
            var wanted = typeof(ValueTask<>);
            var definition = expression.Type.IsGenericType ? expression.Type.GetGenericTypeDefinition() : null;
            if (definition != wanted)
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where an awaiting open of {physType.RowType} was wanted.");

            var cursor = expression.Type.GetGenericArguments()[0];
            var cursorDefinition = cursor.IsGenericType ? cursor.GetGenericTypeDefinition() : null;
            if (cursorDefinition != typeof(ClrCursor<>))
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where an awaiting open of {physType.RowType} was wanted.");

            var actual = cursor.GetGenericArguments()[0];
            if (actual != physType.RowType)
                throw new java.lang.IllegalStateException($"{Node()} handed up an open of {actual} where its row type is {physType.RowType}.");

            return new ClrCursorAsyncResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Refuses an open whose cursor is not of the type the physical type says it is.
        /// </summary>
        /// <remarks>
        /// Calcite's <c>EnumerableRelImplementor.result</c> records what it is handed and asks nothing, its
        /// sequences being erased. This asks, because a node here can build a cursor of the wrong type and
        /// every node below it will still compile. It refuses rather than repairing, for the reason the
        /// enumerable convention gives: a check with a way out is a check only for the shapes that already
        /// pass.
        /// </remarks>
        static void RequireRowType(ClrPhysType physType, Expression expression, Type wanted, string what)
        {
            var expected = physType.RowType;
            var definition = expression.Type.IsGenericType ? expression.Type.GetGenericTypeDefinition() : null;

            if (definition != wanted)
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where {what} of {expected} was wanted.");

            var actual = expression.Type.GetGenericArguments()[0];
            if (actual == expected)
                return;

            throw new java.lang.IllegalStateException($"{Node()} handed up {what} of {actual} where its row type is {expected}.");
        }

        /// <summary>
        /// Names the node whose body called a result factory, for a refusal's message.
        /// </summary>
        static string Node()
        {
            var trace = new System.Diagnostics.StackTrace();

            for (int i = 1; i < trace.FrameCount; i++)
            {
                var type = trace.GetFrame(i)?.GetMethod()?.DeclaringType;
                if (type != null && type != typeof(ClrCursorRelImplementor))
                    return type.Name;
            }

            return "?";
        }

        /// <summary>
        /// Reads a field of the outer row a correlated sub-query was entered with.
        /// </summary>
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
