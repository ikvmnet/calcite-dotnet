using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Linq4j.Function;
using Apache.Calcite.Extensions.Linq4j.Tree;

using IKVM.Runtime;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;
using org.apache.calcite.sql;
using org.apache.calcite.util;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Implementation of <see cref="Match"/> in the <see cref="ClrCursorConvention"/> calling convention.
    /// </summary>
    /// <remarks>
    /// <c>EnumerableMatch</c>, member for member. What that node writes by hand — the key selector, the
    /// matcher's construction, the class around each predicate and the emitter's loop — is written here as an
    /// expression tree; what Calcite's generators write — each pattern definition's condition, and each
    /// measure — is theirs, translated where it is produced.
    ///
    /// <para><b>The two input getters are Calcite's own objects.</b> <c>EnumerableMatch.PrevInputGetter</c> and
    /// <c>PassedRowsInputGetter</c> are package private types that <c>RexToLixTranslator</c> and
    /// <c>RexImpTable</c> cast to by name, so a class of the same shape fails; each is built through a delegate
    /// over its constructor, and <c>setIndex</c> and the translator's package private <c>translate</c> are
    /// called the same way.</para>
    ///
    /// <para><b>Three of Calcite's defects are reproduced, not repaired.</b> The measures row is built with
    /// <c>new</c> on the row's class, so a row whose class has no constructor of no arguments — an
    /// <c>ARRAY</c> row, or one measure of an <c>INTEGER</c> — fails here as it fails Janino, while one
    /// measure of a <c>VARCHAR</c> builds a <c>String</c>. The row emitted is the measures and nothing else,
    /// whatever the node's row type says. And one block collects the conditions of every pattern definition,
    /// so each symbol's predicate starts with the statements of the symbols before it; <c>BlockBuilder</c>
    /// turns each earlier <c>return cond;</c> into <c>cond;</c>, which Java refuses unless it is a call, and
    /// the translator refuses it the same way.</para>
    /// </remarks>
    public class ClrCursorMatch : Match, ClrCursorRel
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorMatch"/>.
        /// </summary>
        public static ClrCursorMatch Create(
            RelNode input,
            RelDataType rowType,
            RexNode pattern,
            bool strictStart,
            bool strictEnd,
            java.util.Map patternDefinitions,
            java.util.Map measures,
            RexNode after,
            java.util.Map subsets,
            bool allRows,
            ImmutableBitSet partitionKeys,
            RelCollation orderKeys,
            RexNode? interval)
        {
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(ClrCursorConvention.Instance);

            return new ClrCursorMatch(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval);
        }

        /// <summary>
        /// Initializes a new instance. Use <see cref="Create"/> unless you know what you are doing.
        /// </summary>
        public ClrCursorMatch(
            RelOptCluster cluster,
            RelTraitSet traitSet,
            RelNode input,
            RelDataType rowType,
            RexNode pattern,
            bool strictStart,
            bool strictEnd,
            java.util.Map patternDefinitions,
            java.util.Map measures,
            RexNode after,
            java.util.Map subsets,
            bool allRows,
            ImmutableBitSet partitionKeys,
            RelCollation orderKeys,
            RexNode? interval) :
            base(cluster, traitSet, input, rowType, pattern, strictStart, strictEnd, patternDefinitions, measures, after, subsets, allRows, partitionKeys, orderKeys, interval)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrCursorMatch(getCluster(), traitSet, (RelNode)inputs.get(0), getRowType(), getPattern(), isStrictStart(), isStrictEnd(), getPatternDefinitions(), getMeasures(), getAfter(), getSubsets(), isAllRows(), getPartitionKeys(), getOrderKeys(), getInterval());
        }

        static java.lang.Class Nested(java.lang.Class declaring, string name) =>
            declaring.getDeclaredClasses().Single(c => c.getSimpleName() == name);

        static java.lang.reflect.Method Method(java.lang.Class declaring, string name, params java.lang.Class[] parameters)
        {
            var method = declaring.getDeclaredMethod(name, parameters);
            method.setAccessible(true);
            return method;
        }

        static java.lang.reflect.Constructor Constructor(java.lang.Class declaring)
        {
            var constructor = declaring.getDeclaredConstructors().Single();
            constructor.setAccessible(true);
            return constructor;
        }

        static readonly java.lang.Class PrevInputGetterClass = Nested((java.lang.Class)typeof(EnumerableMatch), "PrevInputGetter");

        static readonly java.lang.Class PassedRowsInputGetterClass = Nested((java.lang.Class)typeof(EnumerableMatch), "PassedRowsInputGetter");

        static readonly java.lang.Class MaxHistoryFutureVisitorClass = Nested((java.lang.Class)typeof(EnumerableMatch), "MaxHistoryFutureVisitor");

        /// <summary>
        /// <c>new PrevInputGetter(row, physType)</c>.
        /// </summary>
        static readonly MH<object, object, object> NewPrevInputGetter =
            (MH<object, object, object>)JavaDelegates.FromMethod(Constructor(PrevInputGetterClass));

        /// <summary>
        /// <c>new PassedRowsInputGetter(row, passedRows, physType)</c>.
        /// </summary>
        static readonly MH<object, object, object, object> NewPassedRowsInputGetter =
            (MH<object, object, object, object>)JavaDelegates.FromMethod(Constructor(PassedRowsInputGetterClass));

        /// <summary>
        /// <c>PassedRowsInputGetter.setIndex(Expression)</c>.
        /// </summary>
        static readonly MHV<object, object> SetIndex =
            (MHV<object, object>)JavaDelegates.FromMethod(Method(PassedRowsInputGetterClass, "setIndex", (java.lang.Class)typeof(J.Expression)));

        /// <summary>
        /// <c>new MaxHistoryFutureVisitor()</c>.
        /// </summary>
        static readonly MH<object> NewMaxHistoryFutureVisitor =
            (MH<object>)JavaDelegates.FromMethod(Constructor(MaxHistoryFutureVisitorClass));

        /// <summary>
        /// <c>MaxHistoryFutureVisitor.getHistory()</c>.
        /// </summary>
        static readonly MH<object, int> GetHistory =
            (MH<object, int>)JavaDelegates.FromMethod(Method(MaxHistoryFutureVisitorClass, "getHistory"));

        /// <summary>
        /// <c>MaxHistoryFutureVisitor.getFuture()</c>.
        /// </summary>
        static readonly MH<object, int> GetFuture =
            (MH<object, int>)JavaDelegates.FromMethod(Method(MaxHistoryFutureVisitorClass, "getFuture"));

        /// <summary>
        /// <c>RexToLixTranslator.translate(RexNode)</c>, which is package private; <c>translateList</c> is not the
        /// same call, since it passes a storage type and post-processes.
        /// </summary>
        static readonly MH<object, object, object> Translate =
            (MH<object, object, object>)JavaDelegates.FromMethod(Method((java.lang.Class)typeof(RexToLixTranslator), "translate", (java.lang.Class)typeof(RexNode)));

        static readonly MethodInfo PatternBuilder = ClrTypes.Resolve(BuiltInMethod.PATTERN_BUILDER.method);
        static readonly MethodInfo PatternBuilderSymbol = ClrTypes.Resolve(BuiltInMethod.PATTERN_BUILDER_SYMBOL.method);
        static readonly MethodInfo PatternBuilderSeq = ClrTypes.Resolve(BuiltInMethod.PATTERN_BUILDER_SEQ.method);
        static readonly MethodInfo PatternToAutomaton = ClrTypes.Resolve(BuiltInMethod.PATTERN_TO_AUTOMATON.method);
        static readonly MethodInfo MatcherBuilder = ClrTypes.Resolve(BuiltInMethod.MATCHER_BUILDER.method);
        static readonly MethodInfo MatcherBuilderAdd = ClrTypes.Resolve(BuiltInMethod.MATCHER_BUILDER_ADD.method);
        static readonly MethodInfo MatcherBuilderBuild = ClrTypes.Resolve(BuiltInMethod.MATCHER_BUILDER_BUILD.method);
        static readonly MethodInfo ListGet = ClrTypes.Resolve(BuiltInMethod.LIST_GET.method);
        static readonly MethodInfo CollectionSize = ClrTypes.Resolve(BuiltInMethod.COLLECTION_SIZE.method);
        static readonly MethodInfo ConsumerAccept = ClrTypes.Resolve(BuiltInMethod.CONSUMER_ACCEPT.method);

        static readonly ConstructorInfo DelegatePredicateOfMemory =
            typeof(DelegatePredicate<org.apache.calcite.linq4j.MemoryFactory.Memory>).GetConstructors().Single();

        static readonly ConstructorInfo NewDelegateEmitter =
            typeof(DelegateEmitter).GetConstructors().Single();

        /// <summary>
        /// What both bodies build alike: everything but the input and the call.
        /// </summary>
        /// <param name="EmitType">The physical type of the rows emitted.</param>
        /// <param name="KeySelector"></param>
        /// <param name="Variables">The variables the matcher is built into.</param>
        /// <param name="Statements">Builds the matcher, ahead of the input being opened.</param>
        /// <param name="Matcher"></param>
        /// <param name="Emitter"></param>
        /// <param name="History"></param>
        /// <param name="Future"></param>
        record struct Parts(ClrPhysType EmitType, LambdaExpression KeySelector, List<ParameterExpression> Variables, List<Expression> Statements, Expression Matcher, Expression Emitter, int History, int Future);

        /// <summary>
        /// Builds what <c>EnumerableMatch.implement</c> builds before its final call.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="inputPhysType">The physical type of the input as it was implemented.</param>
        /// <param name="format">The format the input was implemented in.</param>
        /// <returns></returns>
        Parts Prepare(ClrCursorRelImplementor implementor, ClrPhysType inputPhysType, JavaRowFormat format)
        {
            var typeFactory = implementor.TypeFactory;

            // Calcite's physType is the input's row type in the input's format, optimized; the translators read
            // a row with it, so it is Calcite's, and the loop holds a row of its class
            var physType = PhysTypeImpl.of(typeFactory, getInput().getRowType(), format);
            var clrPhysType = ClrPhysTypeImpl.Of(typeFactory, getInput().getRowType(), format);

            var keyPhysType = inputPhysType.Project(getPartitionKeys().asList(), JavaRowFormat.LIST);
            var keyRow = Expression.Parameter(inputPhysType.RowType, "row_");
            var keySelector = inputPhysType.GenerateSelector(keyRow, getPartitionKeys().asList(), keyPhysType.Format);

            // the row emitted is the measures, each nullable, and nothing else
            var typeBuilder = typeFactory.builder();
            var measures = getMeasures().entrySet().iterator();
            while (measures.hasNext())
            {
                var entry = (java.util.Map.Entry)measures.next();
                typeBuilder.add((string)entry.getKey(), ((RexNode)entry.getValue()).getType()).nullable(true);
            }

            var emitRowType = typeBuilder.build();
            var emitType = ClrPhysTypeImpl.Of(typeFactory, emitRowType, format);
            var emitJavaRowType = PhysTypeImpl.of(typeFactory, emitRowType, format).getJavaRowType();

            // the row the predicates are written against is named row_, and so is each predicate's parameter,
            // which is a Memory around it: Java resolves the one name to the parameter, and binding the one to
            // the other says the same thing
            var row = J.Expressions.parameter(physType.getJavaRowType(), "row_");

            var variables = new List<ParameterExpression>();
            var statements = new List<Expression>();
            var matcher = ImplementMatcher(implementor, physType, row, variables, statements);
            var emitter = ImplementEmitter(implementor, emitType, emitJavaRowType, physType, clrPhysType);

            // how far back and forward a definition reads
            var visitor = (RexVisitor)NewMaxHistoryFutureVisitor();
            var definitions = getPatternDefinitions().values().iterator();
            while (definitions.hasNext())
                ((RexNode)definitions.next()).accept(visitor);

            return new Parts(emitType, keySelector, variables, statements, matcher, emitter, GetHistory(visitor), GetFuture(visitor));
        }

        /// <inheritdoc />
        public ClrCursorResult Implement(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var result = implementor.VisitChild(this, 0, (ClrCursorRel)getInput(), pref);
            var parts = Prepare(implementor, result.PhysType, result.Format);

            var call = Expression.Call(
                null,
                ClrCursorBuiltInMethod.Match.MakeGenericMethod(result.PhysType.RowType, parts.KeySelector.ReturnType, parts.EmitType.RowType),
                result.Expression,
                parts.KeySelector,
                parts.Matcher,
                parts.Emitter,
                Expression.Constant(parts.History),
                Expression.Constant(parts.Future));

            return implementor.Result(parts.EmitType, Expression.Block(parts.Variables, [.. parts.Statements, call]));
        }

        /// <inheritdoc />
        public ClrCursorAsyncResult ImplementAsync(ClrCursorRelImplementor implementor, ClrCursorPrefer pref)
        {
            var result = implementor.VisitChildAsync(this, 0, (ClrCursorRel)getInput(), pref);
            var parts = Prepare(implementor, result.PhysType, result.Format);

            var call = ClrCursorBuiltInMethod.CallAsync(
                implementor,
                ClrCursorBuiltInMethod.MatchAsync.MakeGenericMethod(result.PhysType.RowType, parts.KeySelector.ReturnType, parts.EmitType.RowType),
                result.Expression,
                parts.KeySelector,
                parts.Matcher,
                parts.Emitter,
                Expression.Constant(parts.History),
                Expression.Constant(parts.Future));

            return implementor.ResultAsync(parts.EmitType, Expression.Block(parts.Variables, [.. parts.Statements, call]));
        }

        /// <summary>
        /// Builds the matcher: the automaton of the pattern, and one predicate per pattern definition.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="row"></param>
        /// <param name="variables">Receives the variables the matcher is built into.</param>
        /// <param name="statements">Receives the statements that build it.</param>
        /// <returns>The variable holding the matcher.</returns>
        /// <remarks>
        /// <c>EnumerableMatch.implementMatcher</c>. Its one <c>BlockBuilder</c> for the conditions is created
        /// outside the loop over the definitions, and each predicate's body is that builder's block as it stands:
        /// the second predicate is the first one's statements, then its own. Appending the second condition turns
        /// the first one's <c>return cond;</c> into <c>cond;</c> — <c>BlockBuilder.append</c> does that to a
        /// trailing return — so the predicate still answers its own condition, and compiles only where Java
        /// allows the first to stand as a statement. Reproduced; the translator refuses what Janino refuses.
        /// </remarks>
        Expression ImplementMatcher(ClrCursorRelImplementor implementor, PhysType physType, J.ParameterExpression row, List<ParameterExpression> variables, List<Expression> statements)
        {
            var patternBuilder = Expression.Variable(PatternBuilder.ReturnType, "patternBuilder");
            variables.Add(patternBuilder);
            statements.Add(Expression.Assign(patternBuilder, Expression.Call(null, PatternBuilder)));

            var automaton = Expression.Variable(PatternToAutomaton.ReturnType, "automaton");
            variables.Add(automaton);
            statements.Add(Expression.Assign(automaton, Expression.Call(ImplementPattern(patternBuilder, getPattern()), PatternToAutomaton)));

            var matcherBuilder = Expression.Variable(MatcherBuilder.ReturnType, "matcherBuilder");
            variables.Add(matcherBuilder);
            statements.Add(Expression.Assign(matcherBuilder, Expression.Call(null, MatcherBuilder, automaton)));

            var builder2 = new J.BlockBuilder();
            Expression matcherBuilderExpression = matcherBuilder;

            var definitions = getPatternDefinitions().entrySet().iterator();
            while (definitions.hasNext())
            {
                var entry = (java.util.Map.Entry)definitions.next();

                // translate Rex to linq4j
                var rexBuilder = new RexBuilder(implementor.TypeFactory);
                var rexProgramBuilder = new RexProgramBuilder(physType.getRowType(), rexBuilder);
                rexProgramBuilder.addCondition((RexNode)entry.getValue());

                var inputGetter1 = (RexToLixTranslator.InputGetter)NewPrevInputGetter(row, physType);

                var condition = RexToLixTranslator.translateCondition(
                    rexProgramBuilder.getProgram(),
                    (JavaTypeFactory)getCluster().getTypeFactory(),
                    builder2,
                    inputGetter1,
                    implementor.AllCorrelateVariables,
                    implementor.Conformance,
                    false,
                    implementor.RexImplementorTable);

                builder2.add(J.Expressions.return_(null, condition));

                matcherBuilderExpression = Expression.Call(
                    matcherBuilderExpression,
                    MatcherBuilderAdd,
                    Expression.Constant((string)entry.getKey()),
                    ImplementPredicate(implementor, row, builder2.toBlock()));
            }

            var matcher = Expression.Variable(MatcherBuilderBuild.ReturnType, "matcher");
            variables.Add(matcher);
            statements.Add(Expression.Assign(matcher, Expression.Call(matcherBuilderExpression, MatcherBuilderBuild)));

            return matcher;
        }

        /// <summary>
        /// Builds the predicate a pattern definition becomes.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="row">The row the condition was translated against.</param>
        /// <param name="body"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableMatch.implementPredicate</c>, which declares a <c>Predicate</c> whose one parameter is a
        /// <c>MemoryFactory.Memory</c> named <c>row_</c>. It also builds an assignment to that parameter and
        /// discards it, and a bridge method only Java needs.
        /// </remarks>
        static Expression ImplementPredicate(ClrCursorRelImplementor implementor, J.ParameterExpression row, J.BlockStatement body)
        {
            var memory = Expression.Parameter(typeof(org.apache.calcite.linq4j.MemoryFactory.Memory), "row_");
            implementor.Translator.Bind(row, memory);

            var lambda = Expression.Lambda<Func<org.apache.calcite.linq4j.MemoryFactory.Memory, bool>>(
                implementor.Translator.TranslateBody(body, typeof(bool)),
                memory);

            return Expression.New(DelegatePredicateOfMemory, lambda);
        }

        /// <summary>
        /// Builds the pattern: for <c>(A B)</c>, <c>patternBuilder.symbol("A").symbol("B").seq()</c>.
        /// </summary>
        /// <param name="patternBuilder"></param>
        /// <param name="pattern"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableMatch.implementPattern</c>, which takes a symbol or a concatenation and nothing else:
        /// a quantifier or an alternation fails here as it fails there.
        /// </remarks>
        static Expression ImplementPattern(Expression patternBuilder, RexNode pattern)
        {
            switch (pattern.getKind().name())
            {
                case nameof(SqlKind.LITERAL):
                    var symbol = (string)((RexLiteral)pattern).getValueAs((java.lang.Class)typeof(string));
                    return Expression.Call(patternBuilder, PatternBuilderSymbol, Expression.Constant(symbol));

                case nameof(SqlKind.PATTERN_CONCAT):
                    var concat = (RexCall)pattern;
                    for (int i = 0; i < concat.operands.size(); i++)
                    {
                        patternBuilder = ImplementPattern(patternBuilder, (RexNode)concat.operands.get(i));
                        if (i > 0)
                            patternBuilder = Expression.Call(patternBuilder, PatternBuilderSeq);
                    }

                    return patternBuilder;

                default:
                    throw new java.lang.AssertionError("unknown kind: " + pattern);
            }
        }

        /// <summary>
        /// Builds the emitter, which turns one match into the rows it contributes.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType">The physical type of the rows emitted.</param>
        /// <param name="javaRowType">The Java type of the rows emitted, which says what building one means.</param>
        /// <param name="inputPhysType">Calcite's physical type of an input row, which the measures read with.</param>
        /// <param name="clrInputPhysType">The same, answering for the row the loop holds.</param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableMatch.implementEmitter</c>: for each row of the match, the row, a new measures row, each
        /// measure assigned into it, and the row handed to the consumer. The loop and the row are this side's;
        /// the measures are Calcite's, and each one's statements are taken in where it wrote them, between the
        /// assignments before it and its own.
        ///
        /// <para>The measures are written into a <c>BlockBuilder</c> that does not optimize, where Calcite's
        /// does, because the block is consumed apart from the expressions that read it: an optimizing one
        /// drops or inlines a declaration no statement of its own reads, and the assignment reading it is
        /// here.</para>
        /// </remarks>
        Expression ImplementEmitter(ClrCursorRelImplementor implementor, ClrPhysType physType, java.lang.reflect.Type javaRowType, PhysType inputPhysType, ClrPhysType clrInputPhysType)
        {
            var rows = Expression.Parameter(typeof(java.util.List), "rows");
            var rowStates = Expression.Parameter(typeof(java.util.List), "rowStates");
            var symbols = Expression.Parameter(typeof(java.util.List), "symbols");
            var match = Expression.Parameter(typeof(int), "match");
            var consumer = Expression.Parameter(typeof(java.util.function.Consumer), "consumer");
            var i = Expression.Variable(typeof(int), "i");
            var row = Expression.Variable(clrInputPhysType.RowType, "row");
            var result = Expression.Variable(physType.RowType, "result");

            var rows_ = J.Expressions.parameter(J.Types.of((java.lang.Class)typeof(java.util.List), inputPhysType.getJavaRowType()), "rows");
            var symbols_ = J.Expressions.parameter((java.lang.Class)typeof(java.util.List), "symbols");
            var i_ = J.Expressions.parameter(java.lang.Integer.TYPE, "i");
            var row_ = J.Expressions.parameter(inputPhysType.getJavaRowType(), "row");

            implementor.Translator.Bind(rows_, rows);
            implementor.Translator.Bind(symbols_, symbols);
            implementor.Translator.Bind(i_, i);
            implementor.Translator.Bind(row_, row);

            var builder2 = new J.BlockBuilder(false);
            var declared = new List<ParameterExpression> { row, result };
            var body = new List<Expression>
            {
                // the loop variable
                Expression.Assign(row, ClrEnumUtils.Convert(Expression.Call(rows, ListGet, i), row.Type)),
            };

            var inputGetter = NewPassedRowsInputGetter(row_, rows_, inputPhysType);
            var translator = RexToLixTranslator.forAggregation(
                (JavaTypeFactory)getCluster().getTypeFactory(),
                builder2,
                (RexToLixTranslator.InputGetter)inputGetter,
                implementor.Conformance);

            body.Add(Expression.Assign(result, NewRow(physType, javaRowType)));

            var measures = getMeasures().values().iterator();
            var written = 0;
            for (int k = 0; measures.hasNext(); k++)
            {
                var measure = (RexNode)measures.next();
                var value = ImplementMeasure(translator, inputGetter, rows_, symbols_, i_, row_, measure);

                // the statements the translator wrote for this measure, in its place
                var statements = builder2.toBlock().statements;
                implementor.Translator.TranslateStatements(J.Expressions.block(statements.subList(written, statements.size())), out var measureDeclared, out var measureBody);
                written = statements.size();
                declared.AddRange(measureDeclared);
                body.AddRange(measureBody);

                var field = physType.FieldReference(result, k);
                body.Add(Expression.Assign(field, ClrEnumUtils.Convert(implementor.Translator.Translate(value), field.Type)));
            }

            body.Add(Expression.Call(consumer, ConsumerAccept, Expression.Convert(result, typeof(object))));

            // an explicit for (int i = ...), because which of the rows are already passed is read by index
            var end = Expression.Label("end");
            var loop = Expression.Block(
                [i],
                Expression.Assign(i, Expression.Constant(0)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.LessThan(i, Expression.Call(rows, CollectionSize)),
                        Expression.Block(declared, [.. body, Expression.PreIncrementAssign(i)]),
                        Expression.Break(end)),
                    end));

            var emit = Expression.Lambda<Action<java.util.List, java.util.List, java.util.List, int, java.util.function.Consumer>>(
                loop, rows, rowStates, symbols, match, consumer);

            return Expression.New(NewDelegateEmitter, emit);
        }

        /// <summary>
        /// Builds a new measures row, as <c>Expressions.new_(physType.getJavaRowType())</c> does.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="javaRowType">The row's Java type, which says what <c>new</c> means.</param>
        /// <returns></returns>
        /// <remarks>
        /// A record is a class of this side's making, and its constructor is the one Java's would be. Anything
        /// else is a Java class and <c>new</c> means its constructor of no arguments, which is Java's to answer:
        /// <c>String</c> has one, and a one-measure row of a <c>VARCHAR</c> runs under Calcite on the strength of
        /// it, while <c>System.String</c> has none. <c>Integer</c> has none in either, and neither has an array,
        /// so Janino refuses those rows and so does this.
        /// </remarks>
        static Expression NewRow(ClrPhysType physType, java.lang.reflect.Type javaRowType)
        {
            if (javaRowType is not java.lang.Class clazz)
                return Expression.New(physType.RowType);

            java.lang.reflect.Constructor? constructor = null;
            if (clazz.isArray() == false && clazz.isPrimitive() == false)
            {
                try
                {
                    constructor = clazz.getConstructor();
                }
                catch (java.lang.NoSuchMethodException)
                {

                }
            }

            if (constructor == null)
                throw new NotSupportedException($"EnumerableMatch builds a MATCH_RECOGNIZE measures row with 'new {clazz.getName()}()', and {clazz.getName()} has no constructor of no arguments.");

            var create = (MH<object>)JavaDelegates.FromMethod(constructor);
            return ClrEnumUtils.Convert(Expression.Invoke(Expression.Constant(create)), physType.RowType);
        }

        /// <summary>
        /// Translates one measure.
        /// </summary>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableMatch.implementMeasure</c>. A match function goes to its <c>MatchImplementor</c>; under
        /// <c>RUNNING</c> or <c>FINAL</c> the getter's index is cleared first, and anything else is translated as
        /// it stands.
        /// </remarks>
        static J.Expression ImplementMeasure(RexToLixTranslator translator, object inputGetter, J.ParameterExpression rows_, J.ParameterExpression symbols_, J.ParameterExpression i_, J.ParameterExpression row_, RexNode value)
        {
            switch (value.getKind().name())
            {
                case nameof(SqlKind.LAST):
                case nameof(SqlKind.PREV):
                case nameof(SqlKind.CLASSIFIER):
                    return Implementor((RexCall)value).implement(translator, (RexCall)value, row_, rows_, symbols_, i_);

                case nameof(SqlKind.RUNNING):
                case nameof(SqlKind.FINAL):
                    // see CALCITE-3341: FINAL is not yet what it should be
                    var operands = ((RexCall)value).getOperands();

                    switch (((RexNode)operands.get(0)).getKind().name())
                    {
                        case nameof(SqlKind.LAST):
                        case nameof(SqlKind.PREV):
                        case nameof(SqlKind.CLASSIFIER):
                            var call = (RexCall)operands.get(0);
                            var matchImplementor = Implementor(call);
                            SetIndex(inputGetter, null!);
                            return matchImplementor.implement(translator, call, row_, rows_, symbols_, i_);
                    }

                    return (J.Expression)Translate(translator, operands.get(0));

                default:
                    return (J.Expression)Translate(translator, value);
            }
        }

        /// <summary>
        /// Returns the implementor of a match function.
        /// </summary>
        /// <param name="call"></param>
        /// <returns></returns>
        static MatchImplementor Implementor(RexCall call)
        {
            var matchFunction = (SqlMatchFunction)call.getOperator();
            return RexImpTable.INSTANCE.get(matchFunction)
                ?? throw new java.lang.NullPointerException("no implementor for match function " + matchFunction);
        }

    }

}
