using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Threading;
using System.Threading.Tasks;

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
    /// Implements a tree of <see cref="ClrCursorRel"/> as a <see cref="ClrCursorFactory"/> that opens it.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableRelImplementor</c>. One instance implements one plan.
    ///
    /// <para><see cref="VisitChild"/> calls only <see cref="ClrCursorRel.Implement"/> and
    /// <see cref="VisitChildAsync"/> only <see cref="ClrCursorRel.ImplementAsync"/>, so each implementation
    /// composes inputs opened the same way it is. <see cref="ImplementRoot"/> walks the tree through both and
    /// returns a factory carrying a synchronous and an awaiting open of the same cursor.</para>
    ///
    /// <para><see cref="CancellationToken"/> is a parameter declared by the awaiting root's lambda and passed
    /// to every awaiting open in the tree. An opener built by <see cref="OpenerAsync"/> declares the same
    /// parameter again, so that inside it the token is the one given to the advance that runs it.</para>
    /// </remarks>
    public class ClrCursorRelImplementor
    {

        /// <summary>
        /// The key in the internal parameters under which a <c>FetchOffsetRoundingPolicy</c> is found.
        /// </summary>
        /// <remarks>
        /// The same value as <c>EnumerableRelImplementor.FETCH_OFFSET_ROUNDING_POLICY</c>, so one entry serves
        /// both conventions.
        /// </remarks>
        public const string FetchOffsetRoundingPolicy = "_fetchOffsetRoundingPolicy";

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, CorrelInputGetter> corrVars = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The internal parameters, which must be the map the
        /// <see cref="DataContext"/> serves at run time.</param>
        public ClrCursorRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters) :
            this(rexBuilder, internalParameters, Expression.Parameter(typeof(DataContext), "root"), Expression.Parameter(typeof(CancellationToken), "cancellationToken"))
        {

        }

        /// <summary>
        /// Initializes a new instance implementing a sub-plan of a plan already being implemented.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The internal parameters, which must be the map the
        /// <see cref="DataContext"/> serves at run time.</param>
        /// <param name="root">The <see cref="DataContext"/> parameter declared by the enclosing plan's
        /// lambda.</param>
        /// <param name="cancellationToken">The token parameter declared by the enclosing plan's awaiting
        /// lambda.</param>
        public ClrCursorRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters, ParameterExpression root, ParameterExpression cancellationToken) :
            this(rexBuilder, internalParameters, root, cancellationToken, new LixToClrTranslator(internalParameters))
        {

        }

        /// <summary>
        /// Initializes a new instance implementing a sub-plan of a plan already being implemented, sharing
        /// that plan's translator.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The internal parameters, which must be the map the
        /// <see cref="DataContext"/> serves at run time.</param>
        /// <param name="root">The <see cref="DataContext"/> parameter declared by the enclosing plan's
        /// lambda.</param>
        /// <param name="cancellationToken">The token parameter declared by the enclosing plan's awaiting
        /// lambda.</param>
        /// <param name="translator">The enclosing plan's translator.</param>
        /// <remarks>
        /// Used by a converter that splices a sub-plan into an enclosing tree. The sub-plan can refer to linq4j
        /// variables the enclosing plan declared, such as a correlate's field reads, and only the translator
        /// that translated those declarations can map them to the CLR variables in the enclosing tree.
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
        /// Gets the type factory of the plan's cluster.
        /// </summary>
        public JavaTypeFactory TypeFactory => (JavaTypeFactory)rexBuilder.getTypeFactory();

        /// <summary>
        /// Gets the parameter through which an open receives the <see cref="DataContext"/>.
        /// </summary>
        public ParameterExpression Root { get; }

        /// <summary>
        /// Gets the parameter through which an awaiting open receives its cancellation token.
        /// </summary>
        /// <remarks>
        /// Declared by the awaiting root's lambda and again by every opener <see cref="OpenerAsync"/> builds.
        /// Synchronous opens do not refer to it.
        /// </remarks>
        public ParameterExpression CancellationToken { get; }

        /// <summary>
        /// Gets the linq4j parameter that stands for the <see cref="DataContext"/>, for passing to Calcite code
        /// generators. The translator maps it to <see cref="Root"/>.
        /// </summary>
        public J.ParameterExpression RootExpression => DataContext.ROOT;

        /// <summary>
        /// Gets the translator from linq4j expressions to CLR expressions. One translator serves the whole
        /// plan, so a linq4j variable maps to the same CLR variable wherever it appears.
        /// </summary>
        internal LixToClrTranslator Translator { get; }

        /// <summary>
        /// Gets the internal parameters: the map the <see cref="DataContext"/> serves at run time.
        /// </summary>
        public java.util.Map Map => map;

        /// <summary>
        /// Gets the table of implementors used to translate row expression calls.
        /// </summary>
        /// <remarks>
        /// Mirrors <c>EnumerableRelImplementor.getRexImplementorTable</c>: the value in <see cref="Map"/> under
        /// <c>_rexImplementorTable</c>, or <c>RexImpTable.INSTANCE</c> if there is none.
        /// </remarks>
        public RexImplementorTable RexImplementorTable =>
            (RexImplementorTable)map.getOrDefault("_rexImplementorTable", RexImpTable.INSTANCE);

        /// <summary>
        /// Gets a function from a correlation variable's name to its input getter, for passing to Calcite's
        /// row expression translator. See <see cref="GetCorrelVariableGetter"/>.
        /// </summary>
        public Function1 AllCorrelateVariables { get; }

        /// <summary>
        /// Gets the SQL conformance: the value in <see cref="Map"/> under <c>_conformance</c>, or
        /// <c>SqlConformanceEnum.DEFAULT</c> if there is none.
        /// </summary>
        public SqlConformance Conformance => (SqlConformance)map.getOrDefault("_conformance", SqlConformanceEnum.DEFAULT);

        /// <summary>
        /// Implements an input of a node as an open that acquires synchronously, by calling the input's
        /// <see cref="ClrCursorRel.Implement"/>.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">The input's ordinal in <paramref name="parent"/>.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">The row representation the parent prefers.</param>
        /// <returns>The input's open and physical type.</returns>
        public ClrCursorResult VisitChild(ClrCursorRel? parent, int ordinal, ClrCursorRel child, ClrCursorPrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.Implement(this, prefer);
        }

        /// <summary>
        /// Implements an input of a node as an open that awaits its acquisition, by calling the input's
        /// <see cref="ClrCursorRel.ImplementAsync"/>.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">The input's ordinal in <paramref name="parent"/>.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">The row representation the parent prefers.</param>
        /// <returns>The input's awaiting open and physical type.</returns>
        public ClrCursorAsyncResult VisitChildAsync(ClrCursorRel? parent, int ordinal, ClrCursorRel child, ClrCursorPrefer prefer)
        {
            ArgumentNullException.ThrowIfNull(child);

            return child.ImplementAsync(this, prefer);
        }

        /// <summary>
        /// Converts an awaiting open into a synchronous one that blocks until the awaiting open completes.
        /// </summary>
        /// <param name="result">The awaiting open.</param>
        /// <returns>A synchronous open of the same cursor.</returns>
        /// <remarks>
        /// For a node whose real implementation is <see cref="ClrCursorRel.ImplementAsync"/>, such as an
        /// adapter with only an asynchronous client, to write <see cref="ClrCursorRel.Implement"/> with. The
        /// resulting open blocks the calling thread for the whole acquisition, and runs the awaiting open with
        /// no synchronization context and <see cref="System.Threading.CancellationToken.None"/>, so that it
        /// cannot deadlock on the blocked thread's context.
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
        /// Converts a synchronous open into an awaiting one that runs it and returns an already completed
        /// <see cref="ValueTask{TResult}"/>.
        /// </summary>
        /// <param name="result">The synchronous open.</param>
        /// <returns>An awaiting open of the same cursor.</returns>
        /// <remarks>
        /// The default <see cref="ClrCursorRel.ImplementAsync"/> uses this. It suits a node whose open has
        /// nothing to await.
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
        /// Wraps a synchronous open in a lambda, for an operator that acquires an input after its own open.
        /// </summary>
        /// <param name="result">The input's open.</param>
        /// <returns>A <c>Func&lt;IClrCursor&lt;TRow&gt;&gt;</c> that runs the open when called.</returns>
        /// <remarks>
        /// Evaluating an open acquires the input, so an operator that acquires it later, as linq4j's
        /// <c>concat</c> acquires each source in turn inside <c>moveNext</c>, takes it as an opener instead.
        /// </remarks>
        public LambdaExpression Opener(ClrCursorResult result)
        {
            ArgumentNullException.ThrowIfNull(result);

            return Expression.Lambda(
                typeof(Func<>).MakeGenericType(result.Expression.Type),
                result.Expression);
        }

        /// <summary>
        /// Wraps an awaiting open in a lambda, for an operator that acquires an input after its own open.
        /// </summary>
        /// <param name="result">The input's awaiting open.</param>
        /// <returns>A <c>Func&lt;CancellationToken, ValueTask&lt;IClrCursor&lt;TRow&gt;&gt;&gt;</c> that
        /// runs the open when called, under the token it is called with.</returns>
        /// <remarks>
        /// The lambda declares <see cref="CancellationToken"/> as its own parameter, shadowing the root's, so an
        /// acquisition made inside <c>ReadAsync</c> observes that call's token rather than the open's.
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
        /// Implements a whole plan as a <see cref="ClrCursorFactory"/> that opens it against a
        /// <see cref="DataContext"/>.
        /// </summary>
        /// <param name="rootRel">The root of the plan.</param>
        /// <param name="prefer">The row representation the caller prefers.</param>
        /// <returns>A factory carrying a synchronous and an awaiting open of the plan, and its element type.</returns>
        /// <exception cref="java.lang.IllegalStateException">A node of the plan could not be implemented. The
        /// message contains the plan and the inner exception is the failure.</exception>
        /// <remarks>
        /// Calls both <see cref="ClrCursorRel.Implement"/> and <see cref="ClrCursorRel.ImplementAsync"/> on the
        /// root. Where <paramref name="prefer"/> is <see cref="ClrCursorPrefer.Array"/> and the plan has one
        /// column, each row is the column's value rather than a one-element array, as in
        /// <c>EnumerableRelImplementor.implementRoot</c>.
        ///
        /// <para>Nothing is compiled here; the factory compiles each open the first time it is used.</para>
        /// </remarks>
        public ClrCursorFactory ImplementRoot(ClrCursorRel rootRel, ClrCursorPrefer prefer)
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

            var rowType = awaited.PhysType.RowType;

            // a one-column result is the value, not a one-element row, as EnumerableRelImplementor arranges
            if (prefer == ClrCursorPrefer.Array
                && pulled.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
            {
                // the root is handed out as an untyped cursor, so the sliced value need not be typed
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

            // the synchronous open converts to the untyped cursor by reference; ValueTask is invariant, so
            // the awaiting open is converted by a helper instead
            var open = Expression.Lambda<Func<DataContext, IClrCursor>>(
                Expression.Convert(pulled.Expression, typeof(IClrCursor)),
                Root);

            var openAsync = Expression.Lambda<Func<DataContext, CancellationToken, ValueTask<IClrCursor>>>(
                Expression.Call(null, ClrCursorBuiltInMethod.Untyped.MakeGenericMethod(rowType), awaited.Expression),
                Root,
                CancellationToken);

            return new ClrCursorFactory(open, openAsync, ElementType(rootRel, prefer));
        }

        /// <summary>
        /// Returns the CLR type of the plan's Java row type under the given preference.
        /// </summary>
        Type ElementType(ClrCursorRel rel, ClrCursorPrefer prefer)
        {
            return ClrTypes.Resolve(ClrPhysTypeImpl.Of(TypeFactory, rel.getRowType(), prefer.PreferArray()).JavaRowType);
        }

        /// <summary>
        /// Returns an expression whose value is an object that has no literal form.
        /// </summary>
        /// <param name="input">The object.</param>
        /// <param name="clazz">The Java class whose CLR type the expression has.</param>
        /// <returns>A constant expression holding <paramref name="input"/>.</returns>
        /// <remarks>
        /// Mirrors <c>EnumerableRelImplementor.stash</c>, which passes the object through the
        /// <see cref="DataContext"/> because generated Java source cannot hold it. An expression tree can hold
        /// any object, so this returns a constant.
        /// </remarks>
        public Expression Stash(object? input, java.lang.Class clazz)
        {
            return Expression.Constant(input, ClrTypes.FromClass(clazz));
        }

        /// <summary>
        /// Registers a correlation variable, through which a correlated input reads the current outer row.
        /// Call <see cref="ClearCorrelVariable"/> when the input has been implemented.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <param name="pe">The linq4j parameter holding the outer row.</param>
        /// <param name="corrBlock">The block into which each field read is declared.</param>
        /// <param name="physType">The outer row's physical type.</param>
        public void RegisterCorrelVariable(string name, J.ParameterExpression pe, J.BlockBuilder corrBlock, PhysType physType)
        {
            corrVars[name] = new CorrelInputGetter(name, pe, corrBlock, physType);
        }

        /// <summary>
        /// Removes a correlation variable registered with <see cref="RegisterCorrelVariable"/>.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <exception cref="java.lang.IllegalStateException">No such variable is in scope.</exception>
        public void ClearCorrelVariable(string name)
        {
            if (corrVars.Remove(name) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");
        }

        /// <summary>
        /// Returns the input getter that reads fields of the row a correlation variable stands for.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <returns>A getter that appends each field read to the variable's block.</returns>
        /// <exception cref="java.lang.IllegalStateException">No such variable is in scope.</exception>
        public RexToLixTranslator.InputGetter GetCorrelVariableGetter(string name)
        {
            if (corrVars.TryGetValue(name, out var getter) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");

            return getter;
        }

        /// <summary>
        /// Registers every correlation variable registered here on a Calcite implementor.
        /// </summary>
        /// <param name="enumerable">The implementor of an <c>EnumerableConvention</c> sub-plan.</param>
        /// <remarks>
        /// An <c>EnumerableConvention</c> sub-plan under a correlate of this convention is implemented by a
        /// separate <c>EnumerableRelImplementor</c>, which keeps its own correlation variables in a private map.
        /// Replaying the registrations lets the sub-plan read the outer row.
        /// </remarks>
        internal void ReplayCorrelVariables(EnumerableRelImplementor enumerable)
        {
            foreach (var pair in corrVars)
                enumerable.registerCorrelVariable(pair.Key, pair.Value.Parameter, pair.Value.Block, pair.Value.PhysType);
        }

        /// <summary>
        /// Creates the result a node's <see cref="ClrCursorRel.Implement"/> returns.
        /// </summary>
        /// <param name="physType">The physical type of the rows.</param>
        /// <param name="expression">The open, of type <c>IClrCursor&lt;TRow&gt;</c> where <c>TRow</c> is
        /// <paramref name="physType"/>'s row type.</param>
        /// <returns>The result, whose format is the physical type's.</returns>
        /// <exception cref="java.lang.IllegalStateException"><paramref name="expression"/> is not of that type.
        /// The message names the calling node.</exception>
        public ClrCursorResult Result(ClrPhysType physType, Expression expression)
        {
            RequireRowType(physType, expression, typeof(IClrCursor<>), "an open");

            return new ClrCursorResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Creates the result a node's <see cref="ClrCursorRel.ImplementAsync"/> returns.
        /// </summary>
        /// <param name="physType">The physical type of the rows.</param>
        /// <param name="expression">The awaiting open, of type <c>ValueTask&lt;IClrCursor&lt;TRow&gt;&gt;</c>
        /// where <c>TRow</c> is <paramref name="physType"/>'s row type.</param>
        /// <returns>The result, whose format is the physical type's.</returns>
        /// <exception cref="java.lang.IllegalStateException"><paramref name="expression"/> is not of that type.
        /// The message names the calling node.</exception>
        /// <remarks>
        /// A synchronous open is refused rather than wrapped; convert it explicitly with <see cref="Awaited"/>.
        /// </remarks>
        public ClrCursorAsyncResult ResultAsync(ClrPhysType physType, Expression expression)
        {
            var wanted = typeof(ValueTask<>);
            var definition = expression.Type.IsGenericType ? expression.Type.GetGenericTypeDefinition() : null;
            if (definition != wanted)
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where an awaiting open of {physType.RowType} was wanted.");

            var cursor = expression.Type.GetGenericArguments()[0];
            var cursorDefinition = cursor.IsGenericType ? cursor.GetGenericTypeDefinition() : null;
            if (cursorDefinition != typeof(IClrCursor<>))
                throw new java.lang.IllegalStateException($"{Node()} handed up a {expression.Type} where an awaiting open of {physType.RowType} was wanted.");

            var actual = cursor.GetGenericArguments()[0];
            if (actual != physType.RowType)
                throw new java.lang.IllegalStateException($"{Node()} handed up an open of {actual} where its row type is {physType.RowType}.");

            return new ClrCursorAsyncResult(expression, physType, physType.Format);
        }

        /// <summary>
        /// Throws unless an expression is of the generic type <paramref name="wanted"/> over the physical row
        /// type.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableRelImplementor.result</c> checks nothing, because Java's sequences are erased. Here a
        /// cursor of the wrong element type would still compile into the plan, so it is refused at the node
        /// that built it.
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
        /// Returns the name of the first type on the call stack other than this one, which is the node that
        /// called a result factory, for an error message.
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
        /// Input getter for a correlation variable. Each field read is appended to the variable's block as a
        /// declaration named <c>{name}_{index}</c>.
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
