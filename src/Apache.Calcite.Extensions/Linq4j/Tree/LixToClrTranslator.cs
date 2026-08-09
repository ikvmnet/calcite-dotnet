using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Translates a linq4j tree into a <see cref="System.Linq.Expressions"/> tree.
    /// </summary>
    /// <remarks>
    /// Named as Calcite names the same kind of thing. <c>RexToLixTranslator</c> translates a Rex expression
    /// into a linq4j one and <c>LixToRelTranslator</c> goes the other way, Lix being Calcite's word for a
    /// linq4j expression; this carries one the last step, to the tree that runs here.
    ///
    /// <para>Calcite generates code as linq4j trees and hands them to Janino. Everything that generates one is
    /// reused here rather than rewritten — <c>RexToLixTranslator</c>, <c>RexImpTable</c> and every
    /// expression-producing member of <c>PhysType</c> — so this is the layer that has to exist for that reuse
    /// to be possible, and the only place the two tree models are allowed to meet.</para>
    ///
    /// <para>linq4j's model was taken from this one, so most of it is one node for one node. Three things are
    /// not: a Java cast is not a CLR conversion (see <see cref="ClrEnumUtils.Convert"/>), an anonymous class is not
    /// something an expression tree can declare (see <see cref="New"/>), and a variable declared part way
    /// through a block has to be hoisted to the block that will hold it.</para>
    ///
    /// <para>A translator carries the scope its tree is translated in, so one is used for one tree.</para>
    /// </remarks>
    sealed class LixToClrTranslator
    {

        /// <summary>
        /// A function being translated, and the label its returns leave by.
        /// </summary>
        /// <param name="Return"></param>
        sealed record Frame(LabelTarget Return);

        /// <summary>
        /// A loop being translated, and the labels its breaks and continues leave by.
        /// </summary>
        /// <param name="Break"></param>
        /// <param name="Continue"></param>
        sealed record Loop(LabelTarget Break, LabelTarget Continue);

        // keyed by reference: linq4j uses one ParameterExpression object everywhere a variable is mentioned,
        // and two variables that merely share a name are two variables
        readonly Dictionary<J.ParameterExpression, ParameterExpression> variables = new(ReferenceEqualityComparer.Instance);
        readonly Stack<Frame> frames = new();
        readonly Stack<Loop> loops = new();

        // Java resolves a name, and a lambda's parameter shadows anything outside it that shares one.
        // Calcite relies on that: a generator builds part of a lambda's body against a ParameterExpression
        // it made itself and another generator makes the lambda's parameter, both named the same thing, and
        // the source Janino compiles says that name twice. Keyed by reference there are two variables, one
        // of them free.
        readonly Stack<Dictionary<string, ParameterExpression>> scopes = new();
        readonly java.util.Map? stashed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="internalParameters">The values passed to the executor rather than written into the
        /// plan, or <see langword="null"/> where the caller has none.</param>
        public LixToClrTranslator(java.util.Map? internalParameters = null)
        {
            stashed = internalParameters;
        }

        /// <summary>
        /// Binds a linq4j variable to the one the translated tree will use for it.
        /// </summary>
        /// <param name="parameter"></param>
        /// <param name="target"></param>
        /// <remarks>
        /// Every tree translated has at least one of these: <c>DataContext.ROOT</c>, which
        /// <c>RexToLixTranslator</c> reaches for a dynamic parameter, for <c>CURRENT_TIMESTAMP</c> and for
        /// <c>USER</c>. A tree translated against a row has that row's parameter as well.
        /// </remarks>
        public void Bind(J.ParameterExpression parameter, ParameterExpression target)
        {
            ArgumentNullException.ThrowIfNull(parameter);
            ArgumentNullException.ThrowIfNull(target);

            variables[parameter] = target;
        }

        /// <summary>
        /// Translates a linq4j node into the CLR expression it means.
        /// </summary>
        /// <param name="node">A linq4j expression or statement.</param>
        /// <returns>The translated expression.</returns>
        /// <exception cref="NotSupportedException">The node has no CLR counterpart.</exception>
        /// <remarks>
        /// An expression is run through <c>OptimizeShuttle</c> first, which is what
        /// <c>BlockBuilder.append</c> does to everything Calcite compiles. That pass is required rather than
        /// cosmetic: a generator may write <c>field == null</c> against a primitive field — Janino rejects it
        /// and the CLR would convert a null to an <c>int</c> and throw — and the shuttle folds it away.
        ///
        /// <para>Statements are translated as they arrive, because a statement the shuttle removes becomes
        /// <c>OptimizeShuttle.EMPTY_STATEMENT</c>, which only a <c>BlockBuilder</c> filters out.</para>
        /// </remarks>
        public Expression Translate(J.Node node)
        {
            ArgumentNullException.ThrowIfNull(node);

            return Visit(node is J.Expression e ? e.accept(Optimizer) : node);
        }

        /// <summary>
        /// The shuttle <c>BlockBuilder</c> runs over everything Calcite compiles.
        /// </summary>
        static readonly J.Shuttle Optimizer = new J.OptimizeShuttle();

        /// <summary>
        /// Translates a node, which has already been optimised.
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression Visit(J.Node node)
        {
            ArgumentNullException.ThrowIfNull(node);

            return node switch
            {
                J.ParameterExpression e => Stashed(e) ?? Variable(e),
                J.ConstantExpression e => Constant(e),
                J.BinaryExpression e => Binary(e),
                J.UnaryExpression e => Unary(e),
                J.TernaryExpression e => Ternary(e),
                J.MethodCallExpression e => Call(e),
                J.MemberExpression e => Member(e),
                J.NewExpression e => New(e),
                J.NewArrayExpression e => NewArray(e),
                J.IndexExpression e => Index(e),
                J.TypeBinaryExpression e => TypeBinary(e),
                J.FunctionExpression e => Function(e),
                J.DefaultExpression e => Expression.Default(ClrTypes.Resolve(e.getType())),
                J.BlockStatement s => Block(s),
                J.DeclarationStatement s => Declaration(s),
                J.ConditionalStatement s => Conditional(s),
                J.GotoStatement s => Goto(s),
                J.ForStatement s => For(s),
                J.ForEachStatement s => ForEach(s),
                J.WhileStatement s => While(s),
                J.TryStatement s => Try(s),
                J.ThrowStatement s => Expression.Throw(Visit(s.expression)),
                _ => throw new NotSupportedException($"Cannot translate a linq4j {node.GetType().Name}.")
            };
        }

        /// <summary>
        /// Translates the body of a function, whose returns leave by a label rather than by falling off the end.
        /// </summary>
        /// <param name="body"></param>
        /// <param name="returnType"></param>
        /// <returns></returns>
        /// <remarks>
        /// A linq4j block returns from wherever it likes, and an expression tree yields the value of its last
        /// expression, so a return becomes a jump to a label placed after the block.
        /// </remarks>
        public Expression TranslateBody(J.BlockStatement body, Type returnType)
        {
            ArgumentNullException.ThrowIfNull(body);
            ArgumentNullException.ThrowIfNull(returnType);

            var label = Expression.Label(returnType, "return");
            frames.Push(new Frame(label));

            try
            {
                var block = Block(body);
                var end = returnType == typeof(void)
                    ? Expression.Label(label)
                    : Expression.Label(label, Expression.Default(returnType));

                return Expression.Block(returnType, [block, end]);
            }
            finally
            {
                frames.Pop();
            }
        }

        /// <summary>
        /// Translates a node in a position that takes a statement rather than a value.
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        Expression Statement(J.Node node)
        {
            return Void(Visit(node));
        }

        /// <summary>
        /// Discards the value of an expression standing in a statement's place.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        static Expression Void(Expression expression)
        {
            return expression.Type == typeof(void) ? expression : Expression.Block(typeof(void), expression);
        }

        /// <summary>
        /// Returns the variable a linq4j parameter stands for, declaring it if this is the first mention.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        ParameterExpression Variable(J.ParameterExpression parameter)
        {
            // by name first, innermost out, and ahead of what the object is already bound to: inside a lambda
            // a mention of something named as one of its parameters *is* that parameter, whatever object it
            // was built from and whatever that object means outside. Java has no way to say otherwise — the
            // parameter shadows the outer variable and the name is all the generated source carries.
            foreach (var scope in scopes)
                if (scope.TryGetValue(parameter.name, out var shadowed))
                    return shadowed;

            if (variables.TryGetValue(parameter, out var variable))
                return variable;

            return variables[parameter] = Expression.Parameter(ClrTypes.Resolve(parameter.getType()), parameter.name);
        }

        /// <summary>
        /// Translates the body of a lambda, with its parameters in scope by name.
        /// </summary>
        /// <param name="parameters"></param>
        /// <param name="body"></param>
        /// <param name="returnType"></param>
        /// <returns></returns>
        Expression Scoped(ParameterExpression[] parameters, J.BlockStatement body, Type returnType)
        {
            var scope = new Dictionary<string, ParameterExpression>(parameters.Length);
            foreach (var parameter in parameters)
                scope[parameter.Name!] = parameter;

            scopes.Push(scope);

            try
            {
                return TranslateBody(body, returnType);
            }
            finally
            {
                scopes.Pop();
            }
        }

        /// <summary>
        /// Returns the value a stashed variable stands for, or <see langword="null"/> where the variable is
        /// an ordinary one.
        /// </summary>
        /// <param name="parameter"></param>
        /// <returns></returns>
        /// <remarks>
        /// <c>EnumerableRelImplementor.stash</c> puts an object on the internal-parameter map and returns a
        /// variable named for it; <c>implementRoot</c> then declares that variable at the top of the method
        /// it generates, reading the object back with <c>root.get(name)</c>. A sub-plan translated on its own
        /// — which is what a converter hands over — never sees that declaration, so the variable arrives
        /// free.
        ///
        /// <para>The object is on the map, and the map is shared with Calcite's implementor precisely so that
        /// what one side stashes reaches the other. An expression tree can hold the object, so it does: the
        /// same answer <c>ClrEnumerableRelImplementor.Stash</c> gives for a value stashed on this side.
        /// A variable declared inside the block is not on the map and is unaffected.</para>
        /// </remarks>
        Expression? Stashed(J.ParameterExpression parameter)
        {
            if (stashed == null || variables.ContainsKey(parameter))
                return null;

            var value = stashed.get(parameter.name);
            if (value == null)
                return null;

            return Expression.Constant(value, ClrTypes.Resolve(parameter.getType()));
        }

        /// <summary>
        /// Translates a constant.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <remarks>
        /// linq4j holds the value of a primitive constant boxed, as Java must, so a constant typed <c>int</c>
        /// arrives as a <c>java.lang.Integer</c> and the CLR would refuse it.
        /// </remarks>
        Expression Constant(J.ConstantExpression expression)
        {
            var type = ClrTypes.Resolve(expression.getType());
            var value = expression.value;

            if (value == null)
                return Expression.Constant(null, type.IsValueType ? typeof(object) : type);

            // a Class used as a value stays a Class. Schemas.tableExpression passes the element type of a
            // QueryableTable to Schemas.queryable, which takes a Class, so turning it into a System.Type here
            // leaves the call unable to be built.
            if (type.IsValueType && value.GetType() != type)
                return Expression.Constant(JavaValues.Unwrap(value, type), type);

            return Expression.Constant(value, type);
        }

        /// <summary>
        /// Translates a variable declaration, which has already had its variable hoisted by <see cref="Block"/>.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        Expression Declaration(J.DeclarationStatement statement)
        {
            var variable = Variable(statement.parameter);
            if (statement.initializer == null)
                return Expression.Empty();

            return Expression.Assign(variable, ClrEnumUtils.Convert(Visit(statement.initializer), variable.Type));
        }

        /// <summary>
        /// Translates a block, hoisting every variable declared in it.
        /// </summary>
        /// <param name="block"></param>
        /// <returns></returns>
        /// <remarks>
        /// Java declares a variable where it is first assigned; an expression tree declares every variable of
        /// a block up front. The declaration stays where it was, as an assignment.
        /// </remarks>
        Expression Block(J.BlockStatement block)
        {
            TranslateStatements(block, out var declared, out var body);

            if (body.Count == 0)
                body.Add(Expression.Empty());

            return Expression.Block(typeof(void), declared, body);
        }

        /// <summary>
        /// Translates the statements of a block without closing it, so a caller can put something of its own
        /// in the same scope.
        /// </summary>
        /// <param name="block"></param>
        /// <param name="declared"></param>
        /// <param name="body"></param>
        /// <remarks>
        /// A correlate needs this. The variables holding the fields of the outer row are declared in a block of
        /// Calcite's making, and the inner sub-plan reads them, so the two have to end up in one scope.
        /// </remarks>
        public void TranslateStatements(J.BlockStatement block, out List<ParameterExpression> declared, out List<Expression> body)
        {
            ArgumentNullException.ThrowIfNull(block);

            declared = [];
            body = [];

            var statements = block.statements;
            for (int i = 0; i < statements.size(); i++)
            {
                var statement = (J.Node)statements.get(i);
                if (statement is J.DeclarationStatement declaration)
                    declared.Add(Variable(declaration.parameter));

                body.Add(Statement(statement));
            }
        }

        /// <summary>
        /// Translates a chain of if / else if / else.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        Expression Conditional(J.ConditionalStatement statement)
        {
            var list = statement.expressionList;
            var count = list.size();

            // an odd length ends in the else, an even one has none
            Expression? result = null;
            var i = count;
            if (count % 2 == 1)
            {
                result = Statement((J.Node)list.get(count - 1));
                i = count - 1;
            }

            while (i > 0)
            {
                var then = Statement((J.Node)list.get(i - 1));
                var test = ClrEnumUtils.Convert(Visit((J.Node)list.get(i - 2)), typeof(bool));
                result = result == null ? Expression.IfThen(test, then) : Expression.IfThenElse(test, then, result);
                i -= 2;
            }

            return result ?? Expression.Empty();
        }

        /// <summary>
        /// Translates a return, break, continue, or a bare expression standing as a statement.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression Goto(J.GotoStatement statement)
        {
            switch (statement.kind.name())
            {
                case nameof(J.GotoExpressionKind.Sequence):
                    // linq4j writes an expression used as a statement this way
                    return statement.expression == null ? Expression.Empty() : Void(Visit(statement.expression));

                case nameof(J.GotoExpressionKind.Return):
                    if (frames.Count == 0)
                        throw new NotSupportedException("A return outside a function body has nowhere to go.");

                    var label = frames.Peek().Return;
                    if (label.Type == typeof(void))
                        return Expression.Return(label);

                    if (statement.expression == null)
                        throw new NotSupportedException($"A return with no value cannot yield '{label.Type}'.");

                    return Expression.Return(label, ClrEnumUtils.Convert(Visit(statement.expression), label.Type));

                case nameof(J.GotoExpressionKind.Break):
                    if (loops.Count == 0)
                        throw new NotSupportedException("A break outside a loop has nowhere to go.");

                    return Expression.Break(loops.Peek().Break);

                case nameof(J.GotoExpressionKind.Continue):
                    if (loops.Count == 0)
                        throw new NotSupportedException("A continue outside a loop has nowhere to go.");

                    return Expression.Continue(loops.Peek().Continue);

                default:
                    throw new NotSupportedException($"Cannot translate a {statement.kind.name()}.");
            }
        }

        /// <summary>
        /// Translates a for loop.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        /// <remarks>
        /// The continue label sits before the post expression rather than at the top of the loop, because a
        /// Java continue still advances the loop.
        /// </remarks>
        Expression For(J.ForStatement statement)
        {
            var declared = new List<ParameterExpression>();
            var initializers = new List<Expression>();

            var declarations = statement.declarations;
            for (int i = 0; i < declarations.size(); i++)
            {
                var declaration = (J.DeclarationStatement)declarations.get(i);
                declared.Add(Variable(declaration.parameter));
                initializers.Add(Statement(declaration));
            }

            var loop = new Loop(Expression.Label("break"), Expression.Label("continue"));
            loops.Push(loop);

            Expression body;
            try
            {
                body = Statement(statement.body);
            }
            finally
            {
                loops.Pop();
            }

            var step = new List<Expression> { body, Expression.Label(loop.Continue) };
            if (statement.post != null)
                step.Add(Statement(statement.post));

            Expression iteration = statement.condition == null
                ? Expression.Block(typeof(void), step)
                : Expression.IfThenElse(
                    ClrEnumUtils.Convert(Visit(statement.condition), typeof(bool)),
                    Expression.Block(typeof(void), step),
                    Expression.Break(loop.Break));

            initializers.Add(Expression.Loop(iteration, loop.Break));

            return Expression.Block(typeof(void), declared, initializers);
        }

        /// <summary>
        /// Translates a while loop.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        Expression While(J.WhileStatement statement)
        {
            var loop = new Loop(Expression.Label("break"), Expression.Label("continue"));
            loops.Push(loop);

            Expression body;
            try
            {
                body = Statement(statement.body);
            }
            finally
            {
                loops.Pop();
            }

            // the continue label is the top of the loop, where the condition is tested again
            return Expression.Loop(
                Expression.IfThenElse(
                    ClrEnumUtils.Convert(Visit(statement.condition), typeof(bool)),
                    body,
                    Expression.Break(loop.Break)),
                loop.Break,
                loop.Continue);
        }

        /// <summary>
        /// Translates a for-each loop over an array or an <see cref="java.lang.Iterable"/>.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        Expression ForEach(J.ForEachStatement statement)
        {
            var element = Variable(statement.parameter);
            var source = Visit(statement.iterable);

            var loop = new Loop(Expression.Label("break"), Expression.Label("continue"));
            loops.Push(loop);

            Expression body;
            try
            {
                body = Statement(statement.body);
            }
            finally
            {
                loops.Pop();
            }

            if (source.Type.IsArray)
            {
                var array = Expression.Variable(source.Type, "array");
                var index = Expression.Variable(typeof(int), "index");

                // the continue label sits before the step and not at the top of the loop, for the reason it
                // does in For: a Java continue still advances the loop, and here the advance is the index
                return Expression.Block(typeof(void), [array, index, element],
                    Expression.Assign(array, source),
                    Expression.Assign(index, Expression.Constant(0)),
                    Expression.Loop(
                        Expression.IfThenElse(
                            Expression.LessThan(index, Expression.ArrayLength(array)),
                            Expression.Block(typeof(void),
                                Expression.Assign(element, ClrEnumUtils.Convert(Expression.ArrayAccess(array, index), element.Type)),
                                body,
                                Expression.Label(loop.Continue),
                                Expression.PostIncrementAssign(index)),
                            Expression.Break(loop.Break)),
                        loop.Break));
            }

            var iterator = Expression.Variable(typeof(java.util.Iterator), "iterator");

            return Expression.Block(typeof(void), [iterator, element],
                Expression.Assign(iterator,
                    Expression.Call(ClrEnumUtils.Convert(source, typeof(java.lang.Iterable)), IterableIterator)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.Call(iterator, IteratorHasNext),
                        Expression.Block(typeof(void),
                            Expression.Assign(element,
                                ClrEnumUtils.Convert(Expression.Call(iterator, IteratorNext), element.Type)),
                            body),
                        Expression.Break(loop.Break)),
                    loop.Break,
                    loop.Continue));
        }

        /// <summary>
        /// Translates a try / catch / finally.
        /// </summary>
        /// <param name="statement"></param>
        /// <returns></returns>
        Expression Try(J.TryStatement statement)
        {
            var body = Statement(statement.body);

            var blocks = statement.catchBlocks;
            var handlers = new CatchBlock[blocks.size()];
            for (int i = 0; i < blocks.size(); i++)
            {
                var block = (J.CatchBlock)blocks.get(i);
                handlers[i] = Expression.Catch(Variable(block.parameter), Statement(block.body));
            }

            return statement.fynally == null
                ? Expression.TryCatch(body, handlers)
                : Expression.TryCatchFinally(body, Statement(statement.fynally), handlers);
        }

        /// <summary>
        /// Translates a conditional expression.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Ternary(J.TernaryExpression expression)
        {
            var type = ClrTypes.Resolve(expression.getType());

            return Expression.Condition(
                ClrEnumUtils.Convert(Visit(expression.expression0), typeof(bool)),
                ClrEnumUtils.Convert(Visit(expression.expression1), type),
                ClrEnumUtils.Convert(Visit(expression.expression2), type),
                type);
        }

        /// <summary>
        /// Translates a member access.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Member(J.MemberExpression expression)
        {
            return ClrTypes.Resolve(expression.expression == null ? null : Visit(expression.expression), expression.field);
        }

        /// <summary>
        /// Translates an array element access.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Index(J.IndexExpression expression)
        {
            var array = Visit(expression.array);

            var indexes = expression.indexExpressions;
            var resolved = new Expression[indexes.size()];
            for (int i = 0; i < indexes.size(); i++)
                resolved[i] = ClrEnumUtils.Convert(Visit((J.Node)indexes.get(i)), typeof(int));

            // ArrayAccess rather than ArrayIndex, because linq4j assigns to one of these
            return Expression.ArrayAccess(array, resolved);
        }

        /// <summary>
        /// Translates an instanceof test.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression TypeBinary(J.TypeBinaryExpression expression)
        {
            return Expression.TypeIs(Visit(expression.expression), ClrTypes.Resolve(expression.type));
        }

        /// <summary>
        /// Translates an array creation.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression NewArray(J.NewArrayExpression expression)
        {
            var type = ClrTypes.Resolve(expression.getType());
            var element = type.GetElementType() ?? throw new NotSupportedException($"'{type}' is not an array.");

            if (expression.expressions != null)
            {
                var items = expression.expressions;
                var resolved = new Expression[items.size()];
                for (int i = 0; i < items.size(); i++)
                    resolved[i] = ClrEnumUtils.Convert(Visit((J.Node)items.get(i)), element);

                return Expression.NewArrayInit(element, resolved);
            }

            if (expression.bound == null)
                throw new NotSupportedException("An array creation needs either its elements or a bound.");

            return Expression.NewArrayBounds(element, ClrEnumUtils.Convert(Visit(expression.bound), typeof(int)));
        }

        /// <summary>
        /// Translates a method call.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Call(J.MethodCallExpression expression)
        {
            var method = ClrTypes.TryResolve(expression.method);
            var target = expression.targetExpression == null ? null : Visit(expression.targetExpression);

            var arguments = expression.expressions;

            // no CLR method of that name and signature is on that type at all, which is what a remapped class
            // or a ghost interface leaves behind. IKVM knows which method it compiled; a name search does not
            if (method == null)
                return Invoke(expression.method, target, arguments);

            // a method IKVM moved off a remapped class is static and takes the receiver first, so what Java
            // called the target is argument zero
            var offset = target != null && method.IsStatic ? 1 : 0;
            var translated = new Expression[arguments.size() + offset];
            if (offset == 1)
                translated[0] = target!;

            for (int i = 0; i < arguments.size(); i++)
                translated[i + offset] = Visit((J.Node)arguments.get(i));

            // the overload is chosen by the receiver and by what is being passed, not by the method the tree
            // names: Janino resolves both from the source text and never looks at that method
            var argumentTypes = new Type[arguments.size()];
            for (int i = 0; i < argumentTypes.Length; i++)
                argumentTypes[i] = translated[i + offset].Type;

            if (target != null && method.IsStatic == false)
                method = ClrTypes.RebindReceiver(method, target.Type, argumentTypes);

            method = ClrTypes.Rebind(method, Array.ConvertAll(translated, e => e.Type));

            var parameters = method.GetParameters();
            var resolved = new Expression[translated.Length];
            for (int i = 0; i < translated.Length; i++)
                resolved[i] = Coerce(translated[i], parameters[i].ParameterType);

            if (method.IsStatic)
                return Expression.Call(null, method, resolved);

            return Expression.Call(ClrEnumUtils.Convert(target!, method.DeclaringType!), method, resolved);
        }

        /// <summary>
        /// Translates a method call as an invocation of a delegate over the method.
        /// </summary>
        /// <param name="method"></param>
        /// <param name="target"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        /// <remarks>
        /// The route for a method the CLR type system cannot be asked about — one of a remapped class, whose
        /// Java methods went elsewhere, or of a ghost interface, which declares nothing. The delegate is
        /// IKVM's own resolution of the member and takes the receiver first where the method has one, so the
        /// receiver moves the same way it does for a method that landed on a <c>Helper</c> class.
        ///
        /// <para>Its signature is objects and primitives — see <c>JavaDelegates.FromMethod</c> — so the
        /// conversions here are reference conversions and the boxing of a primitive argument, both of which
        /// <see cref="Coerce"/> already does. A primitive never crosses as an object, which is the one thing
        /// that would put a <c>java.lang.Integer</c> where a CLR int belongs.</para>
        /// </remarks>
        Expression Invoke(java.lang.reflect.Method method, Expression? target, java.util.List arguments)
        {
            var del = JavaDelegates.FromMethod(method);
            var parameters = del.GetType().GetMethod("Invoke")!.GetParameters();

            var offset = target != null ? 1 : 0;
            var resolved = new Expression[arguments.size() + offset];
            if (offset == 1)
                resolved[0] = Coerce(target!, parameters[0].ParameterType);

            for (int i = 0; i < arguments.size(); i++)
                resolved[i + offset] = Coerce(Visit((J.Node)arguments.get(i)), parameters[i + offset].ParameterType);

            return Expression.Invoke(Expression.Constant(del, del.GetType()), resolved);
        }

        /// <summary>
        /// Translates a constructor call, or an anonymous class.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression New(J.NewExpression expression)
        {
            var type = ClrTypes.Resolve(expression.type);

            if (expression.memberDeclarations == null)
                return Construct(type, expression.arguments);

            return Anonymous(type, expression);
        }

        /// <summary>
        /// Translates an ordinary constructor call.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="arguments"></param>
        /// <returns></returns>
        Expression Construct(Type type, java.util.List arguments)
        {
            var resolved = new Expression[arguments.size()];
            for (int i = 0; i < arguments.size(); i++)
                resolved[i] = Visit((J.Node)arguments.get(i));

            var constructor = type.GetConstructor(Array.ConvertAll(resolved, e => e.Type));
            if (constructor != null)
                return Expression.New(constructor, resolved);

            foreach (var candidate in type.GetConstructors())
            {
                var parameters = candidate.GetParameters();
                if (parameters.Length != resolved.Length)
                    continue;

                var arms = new Expression[resolved.Length];
                for (int i = 0; i < resolved.Length; i++)
                    arms[i] = Coerce(resolved[i], parameters[i].ParameterType);

                return Expression.New(candidate, arms);
            }

            throw new NotSupportedException($"'{type}' has no constructor taking {resolved.Length} arguments.");
        }

        /// <summary>
        /// Translates an anonymous class into the lambda it stands for.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// <c>PhysType.generateComparator</c>, <c>generateCollationKey</c> and <c>comparer</c> all end in an
        /// anonymous <c>Comparator</c>, which an expression tree cannot declare because it cannot declare a
        /// class at all. The class has one method that matters, so it becomes that method as a lambda; the
        /// bridge Java needs for erasure is dropped, since a delegate has no erasure to bridge.
        ///
        /// <para>A field is the other thing such a class can declare. linq4j puts one there itself:
        /// <c>DeterministicCodeOptimizer</c> hoists a sub-expression it can prove constant into a field so the
        /// generated class computes it once, which is how a MATCH_RECOGNIZE predicate ends up holding
        /// <c>$L4J$C$0_1 = 0 * -1</c>. A lambda has no fields, so each becomes a variable of the block that
        /// builds the lambda: assigned once where the class would have been constructed, and closed over.
        /// </para>
        /// </remarks>
        Expression Anonymous(Type type, J.NewExpression expression)
        {
            if (expression.arguments.size() > 0)
                throw new NotSupportedException($"An anonymous '{type}' cannot be translated with constructor arguments.");

            var members = expression.memberDeclarations!;
            var methods = new List<J.MethodDeclaration>();
            var fields = new List<J.FieldDeclaration>();

            for (int i = 0; i < members.size(); i++)
                switch (members.get(i))
                {
                    case J.MethodDeclaration method:
                        methods.Add(method);
                        break;
                    case J.FieldDeclaration field:
                        fields.Add(field);
                        break;
                    default:
                        throw new NotSupportedException($"An anonymous '{type}' declares a {members.get(i).GetType().Name}, which is neither a method nor a field.");
                }

            if (methods.Count == 0)
                throw new NotSupportedException($"An anonymous '{type}' declares no method.");

            Expression wrapped;

            if (AnonymousClasses.MethodsOf(type) != null)
            {
                // several methods over shared state, so one lambda each rather than one for the class
                var declared = new Dictionary<string, LambdaExpression>();
                foreach (var method in methods)
                    declared[method.name] = Lambda(method);

                wrapped = AnonymousClasses.WrapClass(type, declared);
            }
            else
            {
                // the value still has to be the interface it was declared against, because the same operator
                // takes one that never was an anonymous class
                wrapped = AnonymousClasses.Wrap(type, Lambda(methods.Count == 1 ? methods[0] : Unbridged(type, methods)));
            }

            if (fields.Count == 0)
                return wrapped;

            var variables = new List<ParameterExpression>(fields.Count);
            var body = new List<Expression>(fields.Count + 1);

            foreach (var field in fields)
            {
                var variable = Variable(field.parameter);
                variables.Add(variable);

                if (field.initializer != null)
                    body.Add(Expression.Assign(variable, ClrEnumUtils.Convert(Visit(field.initializer), variable.Type)));
            }

            body.Add(wrapped);

            return Expression.Block(wrapped.Type, variables, body);
        }

        /// <summary>
        /// Translates one method of an anonymous class into the lambda that stands for it.
        /// </summary>
        /// <param name="declaration"></param>
        /// <returns></returns>
        LambdaExpression Lambda(J.MethodDeclaration declaration)
        {
            var parameters = new ParameterExpression[declaration.parameters.size()];
            for (int i = 0; i < parameters.Length; i++)
                parameters[i] = Variable((J.ParameterExpression)declaration.parameters.get(i));

            return Expression.Lambda(Scoped(parameters, declaration.body, ClrTypes.Resolve(declaration.resultType)), parameters);
        }

        /// <summary>
        /// Picks the declaration that carries the body, from a set that also holds the bridge Java erasure needs.
        /// </summary>
        /// <param name="type"></param>
        /// <param name="methods"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        static J.MethodDeclaration Unbridged(Type type, List<J.MethodDeclaration> methods)
        {
            // a bridge takes the interface's erased parameters, which are Object; the one that matters takes
            // the row's own type
            var candidates = methods.FindAll(m => AllObject(m) == false);
            if (candidates.Count == 1)
                return candidates[0];

            throw new NotSupportedException($"An anonymous '{type}' declares {methods.Count} methods and no single one of them carries the body.");
        }

        /// <summary>
        /// Returns whether every parameter of a declaration is <see cref="object"/>.
        /// </summary>
        /// <param name="method"></param>
        /// <returns></returns>
        static bool AllObject(J.MethodDeclaration method)
        {
            if (method.parameters.size() == 0)
                return false;

            for (int i = 0; i < method.parameters.size(); i++)
                if (ClrTypes.Resolve(((J.ParameterExpression)method.parameters.get(i)).getType()) != typeof(object))
                    return false;

            return true;
        }

        /// <summary>
        /// Rank of each CLR primitive in Java's binary numeric promotion.
        /// </summary>
        static readonly Dictionary<Type, int> Ranks = new()
        {
            [typeof(bool)] = 0,
            [typeof(sbyte)] = 1,
            [typeof(byte)] = 1,
            [typeof(short)] = 2,
            [typeof(ushort)] = 2,
            [typeof(char)] = 2,
            [typeof(int)] = 3,
            [typeof(uint)] = 3,
            [typeof(long)] = 4,
            [typeof(ulong)] = 4,
            [typeof(float)] = 5,
            [typeof(double)] = 6,
        };

        /// <summary>
        /// Type each rank promotes to. Anything narrower than an int becomes one, as Java does.
        /// </summary>
        static readonly Type[] Promoted = [typeof(bool), typeof(int), typeof(int), typeof(int), typeof(long), typeof(float), typeof(double)];

        /// <summary>
        /// Translates a binary operator.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression Binary(J.BinaryExpression expression)
        {
            var left = Visit(expression.expression0);
            var right = Visit(expression.expression1);
            var op = Operator(expression.getNodeType());

            if (op == ExpressionType.Assign)
                return Expression.Assign(left, ClrEnumUtils.Convert(right, left.Type));

            if (CompoundAssignments.Contains(op))
                return Expression.MakeBinary(op, left, ClrEnumUtils.Convert(right, left.Type));

            // a shift takes its distance as an int however wide the value being shifted is
            if (op is ExpressionType.LeftShift or ExpressionType.RightShift)
                return Expression.MakeBinary(op, left, ClrEnumUtils.Convert(right, typeof(int)));

            if (op == ExpressionType.Add && ClrTypes.Resolve(expression.getType()) == typeof(string))
                return Expression.Call(Concat, ClrEnumUtils.Convert(left, typeof(object)), ClrEnumUtils.Convert(right, typeof(object)));

            // Java's && and || take booleans and unbox a Boolean to get one; the CLR has no operator for two
            // references, so the unboxing that Java leaves implicit is written out. A condition over a
            // nullable column is a Boolean, and a disjunction of a hundred of them is what a batch nested
            // loop join builds.
            if (op is ExpressionType.AndAlso or ExpressionType.OrElse)
                return Expression.MakeBinary(op, ClrEnumUtils.Convert(left, typeof(bool)), ClrEnumUtils.Convert(right, typeof(bool)));

            Promote(ref left, ref right, op);

            return Expression.MakeBinary(op, left, right);
        }

        /// <summary>
        /// Concatenation, which is what Java's <c>+</c> means when either side is a string.
        /// </summary>
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(object), typeof(object)])
            ?? throw new InvalidOperationException("String has no Concat(object, object).");

        /// <summary>
        /// The three members a for-each over a <c>java.lang.Iterable</c> is written against.
        /// </summary>
        static readonly MethodInfo IterableIterator = typeof(java.lang.Iterable).GetMethod("iterator")
            ?? throw new InvalidOperationException("java.lang.Iterable has no iterator().");

        /// <inheritdoc cref="IterableIterator" />
        static readonly MethodInfo IteratorHasNext = typeof(java.util.Iterator).GetMethod("hasNext")
            ?? throw new InvalidOperationException("java.util.Iterator has no hasNext().");

        /// <inheritdoc cref="IterableIterator" />
        static readonly MethodInfo IteratorNext = typeof(java.util.Iterator).GetMethod("next")
            ?? throw new InvalidOperationException("java.util.Iterator has no next().");

        /// <summary>
        /// The operators that assign to their left operand, which therefore must be left alone rather than
        /// promoted.
        /// </summary>
        /// <remarks>
        /// Written out rather than tested by the operator's name. Three of these end in <c>Checked</c> and not
        /// in <c>Assign</c>, so a name test lets them fall through to <see cref="Promote"/>, which may wrap the
        /// left operand in a conversion and leave nothing to assign to.
        /// </remarks>
        static readonly HashSet<ExpressionType> CompoundAssignments =
        [
            ExpressionType.AddAssign,
            ExpressionType.AddAssignChecked,
            ExpressionType.AndAssign,
            ExpressionType.DivideAssign,
            ExpressionType.ExclusiveOrAssign,
            ExpressionType.LeftShiftAssign,
            ExpressionType.ModuloAssign,
            ExpressionType.MultiplyAssign,
            ExpressionType.MultiplyAssignChecked,
            ExpressionType.OrAssign,
            ExpressionType.RightShiftAssign,
            ExpressionType.SubtractAssign,
            ExpressionType.SubtractAssignChecked,
        ];

        /// <summary>
        /// Brings two operands to the type Java would evaluate them at.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="op"></param>
        static void Promote(ref Expression left, ref Expression right, ExpressionType op)
        {
            if (left.Type == right.Type)
            {
                // two boxes of one type, which Java unboxes for anything but == and !=: those compare
                // references, and the CLR does the same, so they are left as they are. Everything else needs
                // a primitive and has none, which is the unboxing Java left implicit
                if (op is not (ExpressionType.Equal or ExpressionType.NotEqual) && ClrPrimitive.PrimitiveClass(left.Type) is Type primitive)
                {
                    left = ClrEnumUtils.Convert(left, primitive);
                    right = ClrEnumUtils.Convert(right, primitive);
                }

                return;
            }

            var l = Ranks.TryGetValue(left.Type, out var lr) ? lr : -1;
            var r = Ranks.TryGetValue(right.Type, out var rr) ? rr : -1;

            if (l >= 0 && r >= 0)
            {
                var type = Promoted[Math.Max(l, r)];
                left = ClrEnumUtils.Convert(left, type);
                right = ClrEnumUtils.Convert(right, type);
                return;
            }

            // Java unboxes the other side when one is a primitive, whatever the operator
            if (l >= 0)
            {
                right = ClrEnumUtils.Convert(right, left.Type);
                return;
            }

            if (r >= 0)
            {
                left = ClrEnumUtils.Convert(left, right.Type);
                return;
            }

            // two references, which only == and != can be applied to, and which have to meet at a common type
            if (left.Type.IsAssignableFrom(right.Type))
                right = Expression.Convert(right, left.Type);
            else if (right.Type.IsAssignableFrom(left.Type))
                left = Expression.Convert(left, right.Type);
            else
            {
                left = Expression.Convert(left, typeof(object));
                right = Expression.Convert(right, typeof(object));
            }
        }

        /// <summary>
        /// Translates a unary operator.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression Unary(J.UnaryExpression expression)
        {
            var operand = Visit(expression.expression);

            switch (expression.getNodeType().name())
            {
                // Java has no checked conversion: a narrowing cast truncates, which is what ClrEnumUtils does.
                // Expression.ConvertChecked would throw on overflow, and also demands a type where the
                // arithmetic operators take none.
                case nameof(J.ExpressionType.Convert):
                case nameof(J.ExpressionType.ConvertChecked):
                    return ClrEnumUtils.Convert(operand, ClrTypes.Resolve(expression.getType()));

                // Java's ! is only ever applied to a boolean; its bitwise complement is a separate operator
                case nameof(J.ExpressionType.Not):
                    return Expression.Not(ClrEnumUtils.Convert(operand, typeof(bool)));

                default:
                    var op = Operator(expression.getNodeType());

                    // null is the documented way to say "no conversion type", and these operators take none.
                    // The parameter is annotated non-nullable all the same, so the suppression is the BCL's
                    return Expression.MakeUnary(op, Widen(operand), null!);
            }
        }

        /// <summary>
        /// Promotes an operand narrower than an int, as Java does before a unary operator.
        /// </summary>
        /// <param name="operand"></param>
        /// <returns></returns>
        static Expression Widen(Expression operand)
        {
            if (Ranks.TryGetValue(operand.Type, out var rank) == false)
                return operand;

            return ClrEnumUtils.Convert(operand, Promoted[rank]);
        }

        /// <summary>
        /// Returns the CLR operator a linq4j one stands for.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// linq4j took this enumeration from the CLR's, so nearly every operator is the same word in both.
        /// Nearly is the reason each is written out: <c>Mod</c> is <c>Modulo</c> here and linq4j has a
        /// <c>Modulo</c> of its own besides, and there is no checked divide to be had. Parsing the name
        /// into the CLR enumeration reads those two as a failure and anything the two runtimes happen to
        /// spell alike as a success, which is a match by coincidence rather than by decision.
        ///
        /// <para>Dispatch is on the name and not the ordinal, which is not stable across versions of
        /// either, and the labels are <c>nameof</c> so that an operator leaving linq4j stops compiling
        /// here rather than throwing when some plan reaches it.</para>
        /// </remarks>
        static ExpressionType Operator(J.ExpressionType type)
        {
            return type.name() switch
            {
                nameof(J.ExpressionType.Add) => ExpressionType.Add,
                nameof(J.ExpressionType.AddChecked) => ExpressionType.AddChecked,
                nameof(J.ExpressionType.And) => ExpressionType.And,
                nameof(J.ExpressionType.AndAlso) => ExpressionType.AndAlso,
                nameof(J.ExpressionType.Call) => ExpressionType.Call,
                nameof(J.ExpressionType.Conditional) => ExpressionType.Conditional,
                nameof(J.ExpressionType.Convert) => ExpressionType.Convert,
                nameof(J.ExpressionType.Divide) => ExpressionType.Divide,
                nameof(J.ExpressionType.DivideChecked) => ExpressionType.Divide,  // the CLR has no checked divide
                nameof(J.ExpressionType.Mod) => ExpressionType.Modulo,  // linq4j has both Mod and Modulo
                nameof(J.ExpressionType.Equal) => ExpressionType.Equal,
                nameof(J.ExpressionType.ExclusiveOr) => ExpressionType.ExclusiveOr,
                nameof(J.ExpressionType.GreaterThan) => ExpressionType.GreaterThan,
                nameof(J.ExpressionType.GreaterThanOrEqual) => ExpressionType.GreaterThanOrEqual,
                nameof(J.ExpressionType.LeftShift) => ExpressionType.LeftShift,
                nameof(J.ExpressionType.LessThan) => ExpressionType.LessThan,
                nameof(J.ExpressionType.LessThanOrEqual) => ExpressionType.LessThanOrEqual,
                nameof(J.ExpressionType.MemberAccess) => ExpressionType.MemberAccess,
                nameof(J.ExpressionType.Modulo) => ExpressionType.Modulo,
                nameof(J.ExpressionType.Multiply) => ExpressionType.Multiply,
                nameof(J.ExpressionType.MultiplyChecked) => ExpressionType.MultiplyChecked,
                nameof(J.ExpressionType.Negate) => ExpressionType.Negate,
                nameof(J.ExpressionType.UnaryPlus) => ExpressionType.UnaryPlus,
                nameof(J.ExpressionType.NegateChecked) => ExpressionType.NegateChecked,
                nameof(J.ExpressionType.Not) => ExpressionType.Not,
                nameof(J.ExpressionType.NotEqual) => ExpressionType.NotEqual,
                nameof(J.ExpressionType.Or) => ExpressionType.Or,
                nameof(J.ExpressionType.OrElse) => ExpressionType.OrElse,
                nameof(J.ExpressionType.RightShift) => ExpressionType.RightShift,
                nameof(J.ExpressionType.Subtract) => ExpressionType.Subtract,
                nameof(J.ExpressionType.SubtractChecked) => ExpressionType.SubtractChecked,
                nameof(J.ExpressionType.TypeIs) => ExpressionType.TypeIs,
                nameof(J.ExpressionType.Assign) => ExpressionType.Assign,
                nameof(J.ExpressionType.AddAssign) => ExpressionType.AddAssign,
                nameof(J.ExpressionType.AndAssign) => ExpressionType.AndAssign,
                nameof(J.ExpressionType.DivideAssign) => ExpressionType.DivideAssign,
                nameof(J.ExpressionType.ExclusiveOrAssign) => ExpressionType.ExclusiveOrAssign,
                nameof(J.ExpressionType.LeftShiftAssign) => ExpressionType.LeftShiftAssign,
                nameof(J.ExpressionType.ModuloAssign) => ExpressionType.ModuloAssign,
                nameof(J.ExpressionType.MultiplyAssign) => ExpressionType.MultiplyAssign,
                nameof(J.ExpressionType.OrAssign) => ExpressionType.OrAssign,
                nameof(J.ExpressionType.RightShiftAssign) => ExpressionType.RightShiftAssign,
                nameof(J.ExpressionType.SubtractAssign) => ExpressionType.SubtractAssign,
                nameof(J.ExpressionType.AddAssignChecked) => ExpressionType.AddAssignChecked,
                nameof(J.ExpressionType.MultiplyAssignChecked) => ExpressionType.MultiplyAssignChecked,
                nameof(J.ExpressionType.SubtractAssignChecked) => ExpressionType.SubtractAssignChecked,
                nameof(J.ExpressionType.PreIncrementAssign) => ExpressionType.PreIncrementAssign,
                nameof(J.ExpressionType.PreDecrementAssign) => ExpressionType.PreDecrementAssign,
                nameof(J.ExpressionType.PostIncrementAssign) => ExpressionType.PostIncrementAssign,
                nameof(J.ExpressionType.PostDecrementAssign) => ExpressionType.PostDecrementAssign,
                nameof(J.ExpressionType.OnesComplement) => ExpressionType.OnesComplement,
                _ => throw new NotSupportedException($"There is no CLR operator for a linq4j {type.name()}."),
            };
        }

        /// <summary>
        /// Translates a lambda.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Function(J.FunctionExpression expression)
        {
            var body = expression.body ?? throw new NotSupportedException("A lambda with no body cannot be translated.");

            var parameters = new ParameterExpression[expression.parameterList.size()];
            for (int i = 0; i < parameters.Length; i++)
                parameters[i] = Variable((J.ParameterExpression)expression.parameterList.get(i));

            var lambda = Expression.Lambda(Scoped(parameters, body, ClrTypes.Resolve(body.getType())), parameters);

            // linq4j declares a lambda against one of its functional interfaces, and a block of Calcite's making
            // uses it as that interface, including where it is passed as an object. So it is one from here, and
            // a node of this convention that wants the delegate asks for it back through TranslateSelector.
            var declared = ClrTypes.Resolve(expression.getType());

            return AnonymousClasses.Handles(declared) ? AnonymousClasses.Wrap(declared, lambda) : lambda;
        }

        /// <summary>
        /// Brings a value to the type it is being passed as.
        /// </summary>
        /// <param name="value"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <remarks>
        /// A lambda is left a lambda everywhere else, because the operators of this convention take delegates.
        /// A block of Calcite's making takes one of linq4j's functional interfaces, and that is decided here,
        /// where the value meets the parameter it is passed as, rather than where the lambda was built.
        /// </remarks>
        static Expression Coerce(Expression value, Type type)
        {
            if (value is LambdaExpression lambda && AnonymousClasses.Handles(type))
                return AnonymousClasses.Wrap(type, lambda);

            return ClrEnumUtils.Convert(value, type);
        }

    }

}
