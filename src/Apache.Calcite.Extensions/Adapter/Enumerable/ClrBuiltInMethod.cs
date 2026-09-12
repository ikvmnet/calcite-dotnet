using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;
using System.Threading;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// The methods a plan of the <see cref="ClrEnumerableConvention"/> calling convention is built from, for
    /// one kind of sequence.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>BuiltInMethod</c>, named the same way, so a node reads as its Calcite
    /// original does. A generic one is the open definition, and a node closes it over the row type it is
    /// working with.
    ///
    /// <para><b>There are two instances, and a node never names either.</b> <see cref="Enumerable"/> is the
    /// operators over <see cref="IEnumerable{T}"/> in <see cref="ClrEnumerableDefaults"/>, and
    /// <see cref="AsyncEnumerable"/> is the same operators over <see cref="IAsyncEnumerable{T}"/> in
    /// <see cref="ClrAsyncEnumerableDefaults"/>, name for name. A node reads the table off
    /// <see cref="ClrEnumerableRelImplementor.Methods"/>, so the kind of sequence a plan runs as is decided
    /// by whoever constructed the implementor and by nothing in the plan. The planner never sees the
    /// difference, because there is no difference to see: the rows, the physical type, the Rex translation
    /// and the tree are the same, and only the operator each call lands on changes.</para>
    ///
    /// <para><b>Every asynchronous operator ends in a <see cref="CancellationToken"/>, and
    /// <see cref="Call"/> supplies it.</b> An expression tree does not apply a default argument, so the
    /// token is appended here, as <c>default</c>, rather than written out at forty call sites. That is not a
    /// token being thrown away: it is what <c>[EnumeratorCancellation]</c> reads, the compiler's iterator
    /// using the token given to <c>GetAsyncEnumerator</c> in place of a parameter that arrived as
    /// <c>default</c>. Passing <c>default</c> from the plan is what lets the caller's token reach every
    /// operator without the plan carrying one.</para>
    ///
    /// <para>A member is added when the node that calls it is written, so that a name here always has an
    /// operator in both tables and a caller.</para>
    /// </remarks>
    public sealed class ClrBuiltInMethod
    {

        /// <summary>
        /// The operators over <see cref="IEnumerable{T}"/>.
        /// </summary>
        public static readonly ClrBuiltInMethod Enumerable = new(typeof(ClrEnumerableDefaults), typeof(IEnumerable<>), false);

        /// <summary>
        /// The operators over <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        public static readonly ClrBuiltInMethod AsyncEnumerable = new(typeof(ClrAsyncEnumerableDefaults), typeof(IAsyncEnumerable<>), true);

        /// <summary>
        /// Returns the table whose operators take the given sequence.
        /// </summary>
        /// <param name="sequence">An expression whose type is an <see cref="IEnumerable{T}"/> or an
        /// <see cref="IAsyncEnumerable{T}"/>.</param>
        /// <returns></returns>
        /// <exception cref="ArgumentException">The expression is neither.</exception>
        /// <remarks>
        /// For the one member of <see cref="ClrPhysType"/> that takes a sequence rather than a row and has
        /// no implementor to ask, <c>ConvertTo</c>.
        /// </remarks>
        public static ClrBuiltInMethod For(Expression sequence)
        {
            ArgumentNullException.ThrowIfNull(sequence);

            var type = sequence.Type;
            if (type.IsGenericType)
            {
                var definition = type.GetGenericTypeDefinition();
                if (definition == Enumerable.SequenceDefinition)
                    return Enumerable;
                if (definition == AsyncEnumerable.SequenceDefinition)
                    return AsyncEnumerable;
            }

            throw new ArgumentException($"{type} is not a sequence of either kind.", nameof(sequence));
        }

        readonly Type defaults;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="defaults">The class holding the operators.</param>
        /// <param name="sequenceDefinition">The open generic interface the operators take and return.</param>
        /// <param name="async">Whether every operator ends in a <see cref="CancellationToken"/>.</param>
        ClrBuiltInMethod(Type defaults, Type sequenceDefinition, bool async)
        {
            this.defaults = defaults ?? throw new ArgumentNullException(nameof(defaults));
            SequenceDefinition = sequenceDefinition ?? throw new ArgumentNullException(nameof(sequenceDefinition));
            Async = async;

            Slice0 = Of(nameof(ClrEnumerableDefaults.Slice0));
            Calc = Of(nameof(ClrEnumerableDefaults.Calc));
            Where = Of(nameof(ClrEnumerableDefaults.Where));
            Select = Of(nameof(ClrEnumerableDefaults.Select));
            OrderBy = Of(nameof(ClrEnumerableDefaults.OrderBy));
            Skip = Of(nameof(ClrEnumerableDefaults.Skip));
            Take = Of(nameof(ClrEnumerableDefaults.Take), null, typeof(int));
            Concat = Of(nameof(ClrEnumerableDefaults.Concat));
            Union = Of(nameof(ClrEnumerableDefaults.Union));
            Intersect = Of(nameof(ClrEnumerableDefaults.Intersect));
            Except = Of(nameof(ClrEnumerableDefaults.Except));
            Distinct = Of(nameof(ClrEnumerableDefaults.Distinct));
            HashJoin = Of(nameof(ClrEnumerableDefaults.HashJoin));
            SemiJoin = Of(nameof(ClrEnumerableDefaults.SemiJoin));
            MergeUnion = Of(nameof(ClrEnumerableDefaults.MergeUnion));
            MergeJoin = Of(nameof(ClrEnumerableDefaults.MergeJoin));
            AsofJoin = Of(nameof(ClrEnumerableDefaults.AsofJoin));
            NestedLoopJoin = Of(nameof(ClrEnumerableDefaults.NestedLoopJoin));
            LeftMarkNestedLoopJoin = Of(nameof(ClrEnumerableDefaults.LeftMarkNestedLoopJoin));
            LeftMarkHashJoin = Of(nameof(ClrEnumerableDefaults.LeftMarkHashJoin));
            CorrelateLeftMarkJoin = Of(nameof(ClrEnumerableDefaults.CorrelateLeftMarkJoin));
            CorrelateBatchJoin = Of(nameof(ClrEnumerableDefaults.CorrelateBatchJoin));
            CorrelateJoin = Of(nameof(ClrEnumerableDefaults.CorrelateJoin));
            GroupBy = Of(nameof(ClrEnumerableDefaults.GroupBy));
            GroupByMultiple = Of(nameof(ClrEnumerableDefaults.GroupByMultiple));
            SortedGroupBy = Of(nameof(ClrEnumerableDefaults.SortedGroupBy));
            SingletonAggregate = Of(nameof(ClrEnumerableDefaults.SingletonAggregate));
            Window = Of(nameof(ClrEnumerableDefaults.Window));
            SelectMany = Of(nameof(ClrEnumerableDefaults.SelectMany));
            OrderByWithFetchAndOffset = Of(nameof(ClrEnumerableDefaults.OrderByWithFetchAndOffset));
            SingletonJavaList = Of(nameof(ClrEnumerableDefaults.SingletonJavaList));
            SingletonJavaMap = Of(nameof(ClrEnumerableDefaults.SingletonJavaMap));
            FromJavaList = Of(nameof(ClrEnumerableDefaults.FromJavaList));
            CombineQueryResults = Of(nameof(ClrEnumerableDefaults.CombineQueryResults));
            LazyCollectionSpool = Of(nameof(ClrEnumerableDefaults.LazyCollectionSpool));
            RepeatUnion = Of(nameof(ClrEnumerableDefaults.RepeatUnion));
            AsEnumerable = Of(nameof(ClrEnumerableDefaults.AsEnumerable));
            Singleton = Of(nameof(ClrEnumerableDefaults.Singleton));
            Empty = Of(nameof(ClrEnumerableDefaults.Empty));

            FromJava = Static(typeof(JavaSequences), async ? nameof(JavaSequences.FromJavaAsync) : nameof(JavaSequences.FromJava));
            Bridge = Static(typeof(ClrSequences), async ? nameof(ClrSequences.ToAsyncEnumerable) : nameof(ClrSequences.ToEnumerable));
        }

        /// <summary>
        /// Gets whether the operators of this table take and return an <see cref="IAsyncEnumerable{T}"/>,
        /// and so end in a <see cref="CancellationToken"/>.
        /// </summary>
        public bool Async { get; }

        /// <summary>
        /// Gets the open generic interface the operators take and return: <see cref="IEnumerable{T}"/> or
        /// <see cref="IAsyncEnumerable{T}"/>.
        /// </summary>
        public Type SequenceDefinition { get; }

        /// <summary>
        /// Returns the type of a sequence of the given rows, of this table's kind.
        /// </summary>
        /// <param name="rowType"></param>
        /// <returns></returns>
        public Type SequenceType(Type rowType)
        {
            ArgumentNullException.ThrowIfNull(rowType);

            return SequenceDefinition.MakeGenericType(rowType);
        }

        /// <summary>
        /// Returns whether the expression is a sequence of this table's kind.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        public bool IsSequence(Expression expression)
        {
            ArgumentNullException.ThrowIfNull(expression);

            return expression.Type.IsGenericType && expression.Type.GetGenericTypeDefinition() == SequenceDefinition;
        }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Slice0"/>.
        /// </summary>
        public MethodInfo Slice0 { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Calc"/>.
        /// </summary>
        public MethodInfo Calc { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Where"/>.
        /// </summary>
        public MethodInfo Where { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Select{TSource, TResult}"/>.
        /// </summary>
        public MethodInfo Select { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderBy"/>.
        /// </summary>
        public MethodInfo OrderBy { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Skip"/>.
        /// </summary>
        public MethodInfo Skip { get; }

        /// <summary>
        /// <c>Take</c>, the <see cref="int"/> overload.
        /// </summary>
        /// <remarks>
        /// A plan's fetch is an <see cref="int"/>. The <see cref="long"/> one is the row limit a caller asks a
        /// prepared statement for, and is applied there.
        /// </remarks>
        public MethodInfo Take { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Concat"/>.
        /// </summary>
        public MethodInfo Concat { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Union"/>.
        /// </summary>
        public MethodInfo Union { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Intersect"/>.
        /// </summary>
        public MethodInfo Intersect { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Except"/>.
        /// </summary>
        public MethodInfo Except { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Distinct"/>.
        /// </summary>
        public MethodInfo Distinct { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.HashJoin"/>.
        /// </summary>
        public MethodInfo HashJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SemiJoin"/>.
        /// </summary>
        public MethodInfo SemiJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeUnion"/>.
        /// </summary>
        public MethodInfo MergeUnion { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.MergeJoin"/>.
        /// </summary>
        public MethodInfo MergeJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsofJoin"/>.
        /// </summary>
        public MethodInfo AsofJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.NestedLoopJoin"/>.
        /// </summary>
        public MethodInfo NestedLoopJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkNestedLoopJoin"/>.
        /// </summary>
        public MethodInfo LeftMarkNestedLoopJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LeftMarkHashJoin"/>.
        /// </summary>
        public MethodInfo LeftMarkHashJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateLeftMarkJoin"/>.
        /// </summary>
        public MethodInfo CorrelateLeftMarkJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateBatchJoin"/>.
        /// </summary>
        public MethodInfo CorrelateBatchJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CorrelateJoin"/>.
        /// </summary>
        public MethodInfo CorrelateJoin { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupBy"/>.
        /// </summary>
        public MethodInfo GroupBy { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.GroupByMultiple"/>.
        /// </summary>
        public MethodInfo GroupByMultiple { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SortedGroupBy"/>.
        /// </summary>
        public MethodInfo SortedGroupBy { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonAggregate"/>.
        /// </summary>
        public MethodInfo SingletonAggregate { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Window"/>.
        /// </summary>
        public MethodInfo Window { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SelectMany"/>.
        /// </summary>
        public MethodInfo SelectMany { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.OrderByWithFetchAndOffset"/>.
        /// </summary>
        public MethodInfo OrderByWithFetchAndOffset { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonJavaList"/>.
        /// </summary>
        public MethodInfo SingletonJavaList { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.SingletonJavaMap"/>.
        /// </summary>
        public MethodInfo SingletonJavaMap { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.FromJavaList"/>.
        /// </summary>
        public MethodInfo FromJavaList { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.CombineQueryResults"/>.
        /// </summary>
        public MethodInfo CombineQueryResults { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.LazyCollectionSpool"/>.
        /// </summary>
        public MethodInfo LazyCollectionSpool { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.RepeatUnion"/>.
        /// </summary>
        public MethodInfo RepeatUnion { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.AsEnumerable"/>.
        /// </summary>
        public MethodInfo AsEnumerable { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Singleton"/>.
        /// </summary>
        public MethodInfo Singleton { get; }

        /// <summary>
        /// <see cref="ClrEnumerableDefaults.Empty"/>.
        /// </summary>
        public MethodInfo Empty { get; }

        /// <summary>
        /// <see cref="JavaSequences.FromJava"/> or <see cref="JavaSequences.FromJavaAsync"/>: reads a linq4j
        /// <c>Enumerable</c> as a sequence of this kind.
        /// </summary>
        /// <remarks>
        /// Not an operator of the defaults but of the interop, and the one place a plan of this convention
        /// reads a sequence of Calcite's: a table's own expression, an <c>EnumerableConvention</c> sub-plan
        /// under a converter, the interpreter, a table function. Neither direction suspends, because a linq4j
        /// <c>Enumerable</c> is pulled; the asynchronous one costs a state machine and no thread.
        /// </remarks>
        public MethodInfo FromJava { get; }

        /// <summary>
        /// <see cref="ClrSequences.ToEnumerable{TSource}"/> or
        /// <see cref="ClrSequences.ToAsyncEnumerable{TSource}"/>: reads a sequence of the <em>other</em> kind
        /// as one of this kind.
        /// </summary>
        /// <remarks>
        /// The two directions are not the same cost, and a node that calls this says which it is paying at
        /// the site. Reading an <see cref="IEnumerable{T}"/> asynchronously costs a state machine and no
        /// thread, because the source is pulled. Reading an <see cref="IAsyncEnumerable{T}"/> synchronously
        /// blocks the calling thread once per row, because an <see cref="IEnumerable{T}"/> has nowhere to
        /// suspend. The second is the sync-over-async an asynchronous plan exists to avoid, and it is taken
        /// only where the alternative is a query that does not run: a synchronous plan over a table that
        /// yields its rows asynchronously, and a table function whose cursor input is asynchronous.
        /// </remarks>
        public MethodInfo Bridge { get; }

        /// <summary>
        /// Builds the call to an operator of this table.
        /// </summary>
        /// <param name="method">The operator, with its type arguments already applied.</param>
        /// <param name="arguments">The arguments, less the cancellation token.</param>
        /// <returns></returns>
        /// <exception cref="InvalidOperationException">The operator's parameters do not match the arguments,
        /// or, for the asynchronous table, it does not end in a <see cref="CancellationToken"/>.</exception>
        /// <remarks>
        /// A node calls this where its Calcite original writes <c>Expressions.call(...)</c>, and for the
        /// synchronous table it is exactly <see cref="Expression.Call(Expression, MethodInfo, Expression[])"/>.
        /// For the asynchronous one it appends the token, for the reason the class remarks give.
        /// </remarks>
        public MethodCallExpression Call(MethodInfo method, params Expression[] arguments)
        {
            ArgumentNullException.ThrowIfNull(method);
            ArgumentNullException.ThrowIfNull(arguments);

            var parameters = method.GetParameters();

            if (Async == false)
            {
                if (parameters.Length != arguments.Length)
                    throw new InvalidOperationException($"{method.Name} takes {parameters.Length} arguments and was given {arguments.Length}.");

                return Expression.Call(null, method, arguments);
            }

            if (parameters.Length != arguments.Length + 1)
                throw new InvalidOperationException($"{method.Name} takes {parameters.Length} arguments and was given {arguments.Length} plus a token.");
            if (parameters[^1].ParameterType != typeof(CancellationToken))
                throw new InvalidOperationException($"{method.Name} does not end in a {nameof(CancellationToken)}.");

            var all = new Expression[arguments.Length + 1];
            arguments.CopyTo(all, 0);
            all[^1] = Expression.Default(typeof(CancellationToken));

            return Expression.Call(null, method, all);
        }

        /// <summary>
        /// Returns the named operator, picked out by parameter type where the name is overloaded.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="parameterTypes">One entry per parameter, less the token, <see langword="null"/>
        /// where the parameter is generic and so has no <see cref="Type"/> to name. Empty where the name is
        /// enough.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>BuiltInMethod</c> names a method by its parameter types — <c>TAKE(ExtendedEnumerable.class,
        /// "take", int.class)</c> — because a name alone does not pick an overload, and
        /// <c>EnumerableDefaults.take</c> has two. <see cref="Type.GetMethod(string, BindingFlags)"/> throws
        /// on the ambiguity rather than reporting it, so the choice is made here.
        /// </remarks>
        MethodInfo Of(string name, params Type?[] parameterTypes)
        {
            MethodInfo? found = null;

            foreach (var method in defaults.GetMethods(BindingFlags.Public | BindingFlags.Static))
            {
                if (method.Name != name || Matches(method) == false)
                    continue;

                if (found != null)
                    throw new InvalidOperationException($"'{name}' is ambiguous in {defaults.Name}; name its parameter types.");

                found = method;
            }

            return found ?? throw new InvalidOperationException($"'{name}' is missing from {defaults.Name}.");

            bool Matches(MethodInfo method)
            {
                if (parameterTypes.Length == 0)
                    return true;

                var parameters = method.GetParameters();
                if (parameters.Length != parameterTypes.Length + (Async ? 1 : 0))
                    return false;

                for (var i = 0; i < parameterTypes.Length; i++)
                    if (parameterTypes[i] != null && parameters[i].ParameterType != parameterTypes[i])
                        return false;

                return true;
            }
        }

        /// <summary>
        /// Returns the named public static method of a type that is not the defaults.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        static MethodInfo Static(Type type, string name)
        {
            return type.GetMethod(name, BindingFlags.Public | BindingFlags.Static)
                ?? throw new InvalidOperationException($"'{name}' is missing from {type.Name}.");
        }

    }

}
