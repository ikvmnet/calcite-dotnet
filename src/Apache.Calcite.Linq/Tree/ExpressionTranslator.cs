using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// Translates a linq4j tree into a <see cref="System.Linq.Expressions"/> tree.
    /// </summary>
    /// <remarks>
    /// Calcite generates code as linq4j trees and hands them to Janino. Everything that generates one is
    /// reused here rather than rewritten — <c>RexToLixTranslator</c>, <c>RexImpTable</c> and every
    /// expression-producing member of <c>PhysType</c> — so this is the layer that has to exist for that reuse
    /// to be possible, and the only place the two tree models are allowed to meet.
    ///
    /// <para>linq4j's model was taken from this one, so most of it is one node for one node. Three things are
    /// not: a Java cast is not a CLR conversion (see <see cref="JavaCast"/>), an anonymous class is not
    /// something an expression tree can declare (see <see cref="New"/>), and a variable declared part way
    /// through a block has to be hoisted to the block that will hold it.</para>
    ///
    /// <para>A translator carries the scope its tree is translated in, so one is used for one tree.</para>
    /// </remarks>
    public sealed class ExpressionTranslator
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
        public ExpressionTranslator(java.util.Map? internalParameters = null)
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
        /// Translates a node.
        /// </summary>
        /// <param name="node"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// An expression arriving from outside is optimised first, because every tree Calcite gives Janino has
        /// been: a node hands its tree to <c>BlockBuilder.append</c>, which runs <c>OptimizeShuttle</c> over
        /// it. That shuttle's own class comment says why it is not a tweak — "without optimization,
        /// expressions such as <c>false == null</c> will be left in, which are invalid to Janino (because it
        /// does not automatically box primitives)".
        ///
        /// <para>An expression tree is stricter still. <c>PhysType.generateNullAwareAccessor</c> writes
        /// <c>field == null ? null : List1(field)</c> for every key, and where the field is a primitive that
        /// comparison is what the shuttle folds to <c>false</c>; left in, the CLR converts a null to an
        /// <c>int</c> and throws. Translating what Janino would have been given rather than what the
        /// generator wrote is the whole of it.</para>
        ///
        /// <para>Only an expression. A statement the shuttle rewrites away becomes
        /// <c>OptimizeShuttle.EMPTY_STATEMENT</c>, which <c>BlockBuilder</c> filters and a bare block does
        /// not, and the blocks translated here come from a <c>BlockBuilder</c> that has already run it — or
        /// from one deliberately built not to.</para>
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
                J.DefaultExpression e => Expression.Default(TypeResolver.Resolve(e.getType())),
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
        /// Translates a selector, which <c>PhysType</c> does not always give as a lambda.
        /// </summary>
        /// <param name="selector"></param>
        /// <param name="sourceType"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// Where a projection is the identity, <c>PhysType.generateSelector</c> returns a call to
        /// <c>Functions.identitySelector</c> rather than a lambda, and its value is a linq4j <c>Function1</c>
        /// that no delegate can be made from. The identity is the one thing a caller can supply for itself.
        /// </remarks>
        public LambdaExpression TranslateSelector(J.Expression selector, Type sourceType)
        {
            ArgumentNullException.ThrowIfNull(selector);
            ArgumentNullException.ThrowIfNull(sourceType);

            var translated = Translate(selector);

            var lambda = SamAdapters.Unwrap(translated);
            if (lambda != null)
                return lambda;

            if (translated is MethodCallExpression call && call.Method == IdentitySelector)
            {
                var row = Expression.Parameter(sourceType, "row");
                return Expression.Lambda(row, row);
            }

            throw new NotSupportedException($"A selector of '{translated.Type}' is neither a lambda nor the identity.");
        }

        /// <summary>
        /// <c>Functions.identitySelector</c>, which is what a projection that changes nothing comes back as.
        /// </summary>
        static readonly MethodInfo IdentitySelector = MethodResolver.Resolve(org.apache.calcite.util.BuiltInMethod.IDENTITY_SELECTOR.method);

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

            return variables[parameter] = Expression.Parameter(TypeResolver.Resolve(parameter.getType()), parameter.name);
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

            return Expression.Constant(value, TypeResolver.Resolve(parameter.getType()));
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
            var type = TypeResolver.Resolve(expression.getType());
            var value = expression.value;

            if (value == null)
                return Expression.Constant(null, type.IsValueType ? typeof(object) : type);

            // a Class used as a value stays a Class. Schemas.tableExpression passes the element type of a
            // QueryableTable to Schemas.queryable, which takes a Class, so turning it into a System.Type here
            // leaves the call unable to be built.
            if (type.IsValueType && value.GetType() != type)
                return Expression.Constant(JavaCast.Unwrap(value, type), type);

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

            return Expression.Assign(variable, JavaCast.To(Visit(statement.initializer), variable.Type));
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
                var test = JavaCast.To(Visit((J.Node)list.get(i - 2)), typeof(bool));
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

                    return Expression.Return(label, JavaCast.To(Visit(statement.expression), label.Type));

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
                    JavaCast.To(Visit(statement.condition), typeof(bool)),
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
                    JavaCast.To(Visit(statement.condition), typeof(bool)),
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

                return Expression.Block(typeof(void), [array, index, element],
                    Expression.Assign(array, source),
                    Expression.Assign(index, Expression.Constant(0)),
                    Expression.Loop(
                        Expression.IfThenElse(
                            Expression.LessThan(index, Expression.ArrayLength(array)),
                            Expression.Block(typeof(void),
                                Expression.Assign(element, JavaCast.To(Expression.ArrayAccess(array, index), element.Type)),
                                body,
                                Expression.PostIncrementAssign(index)),
                            Expression.Break(loop.Break)),
                        loop.Break,
                        loop.Continue));
            }

            var iterator = Expression.Variable(typeof(java.util.Iterator), "iterator");

            return Expression.Block(typeof(void), [iterator, element],
                Expression.Assign(iterator,
                    Expression.Call(JavaCast.To(source, typeof(java.lang.Iterable)), typeof(java.lang.Iterable).GetMethod("iterator")!)),
                Expression.Loop(
                    Expression.IfThenElse(
                        Expression.Call(iterator, typeof(java.util.Iterator).GetMethod("hasNext")!),
                        Expression.Block(typeof(void),
                            Expression.Assign(element,
                                JavaCast.To(Expression.Call(iterator, typeof(java.util.Iterator).GetMethod("next")!), element.Type)),
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
            var type = TypeResolver.Resolve(expression.getType());

            return Expression.Condition(
                JavaCast.To(Visit(expression.expression0), typeof(bool)),
                JavaCast.To(Visit(expression.expression1), type),
                JavaCast.To(Visit(expression.expression2), type),
                type);
        }

        /// <summary>
        /// Translates a member access.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Member(J.MemberExpression expression)
        {
            return FieldResolver.Resolve(expression.expression == null ? null : Visit(expression.expression), expression.field);
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
                resolved[i] = JavaCast.To(Visit((J.Node)indexes.get(i)), typeof(int));

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
            return Expression.TypeIs(Visit(expression.expression), TypeResolver.Resolve(expression.type));
        }

        /// <summary>
        /// Translates an array creation.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression NewArray(J.NewArrayExpression expression)
        {
            var type = TypeResolver.Resolve(expression.getType());
            var element = type.GetElementType() ?? throw new NotSupportedException($"'{type}' is not an array.");

            if (expression.expressions != null)
            {
                var items = expression.expressions;
                var resolved = new Expression[items.size()];
                for (int i = 0; i < items.size(); i++)
                    resolved[i] = JavaCast.To(Visit((J.Node)items.get(i)), element);

                return Expression.NewArrayInit(element, resolved);
            }

            if (expression.bound == null)
                throw new NotSupportedException("An array creation needs either its elements or a bound.");

            return Expression.NewArrayBounds(element, JavaCast.To(Visit(expression.bound), typeof(int)));
        }

        /// <summary>
        /// Translates a method call.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        Expression Call(J.MethodCallExpression expression)
        {
            var method = MethodResolver.Resolve(expression.method);
            var target = expression.targetExpression == null ? null : Visit(expression.targetExpression);

            var arguments = expression.expressions;

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
                method = MethodResolver.RebindReceiver(method, target.Type, argumentTypes);

            method = MethodResolver.Rebind(method, Array.ConvertAll(translated, e => e.Type));

            var parameters = method.GetParameters();
            var resolved = new Expression[translated.Length];
            for (int i = 0; i < translated.Length; i++)
                resolved[i] = Coerce(translated[i], parameters[i].ParameterType);

            if (method.IsStatic)
                return Expression.Call(null, method, resolved);

            return Expression.Call(JavaCast.To(target!, method.DeclaringType!), method, resolved);
        }

        /// <summary>
        /// Translates a constructor call, or an anonymous class.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        Expression New(J.NewExpression expression)
        {
            var type = TypeResolver.Resolve(expression.type);

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

            if (SamAdapters.MethodsOf(type) != null)
            {
                // several methods over shared state, so one lambda each rather than one for the class
                var declared = new Dictionary<string, LambdaExpression>();
                foreach (var method in methods)
                    declared[method.name] = Lambda(method);

                wrapped = SamAdapters.WrapClass(type, declared);
            }
            else
            {
                // the value still has to be the interface it was declared against, because the same operator
                // takes one that never was an anonymous class
                wrapped = SamAdapters.Wrap(type, Lambda(methods.Count == 1 ? methods[0] : Unbridged(type, methods)));
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
                    body.Add(Expression.Assign(variable, JavaCast.To(Visit(field.initializer), variable.Type)));
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

            return Expression.Lambda(Scoped(parameters, declaration.body, TypeResolver.Resolve(declaration.resultType)), parameters);
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
                if (TypeResolver.Resolve(((J.ParameterExpression)method.parameters.get(i)).getType()) != typeof(object))
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
                return Expression.Assign(left, JavaCast.To(right, left.Type));

            if (op.ToString().EndsWith("Assign", StringComparison.Ordinal))
                return Expression.MakeBinary(op, left, JavaCast.To(right, left.Type));

            // a shift takes its distance as an int however wide the value being shifted is
            if (op is ExpressionType.LeftShift or ExpressionType.RightShift)
                return Expression.MakeBinary(op, left, JavaCast.To(right, typeof(int)));

            if (op == ExpressionType.Add && TypeResolver.Resolve(expression.getType()) == typeof(string))
                return Expression.Call(Concat, JavaCast.To(left, typeof(object)), JavaCast.To(right, typeof(object)));

            // Java's && and || take booleans and unbox a Boolean to get one; the CLR has no operator for two
            // references, so the unboxing that Java leaves implicit is written out. A condition over a
            // nullable column is a Boolean, and a disjunction of a hundred of them is what a batch nested
            // loop join builds.
            if (op is ExpressionType.AndAlso or ExpressionType.OrElse)
                return Expression.MakeBinary(op, JavaCast.To(left, typeof(bool)), JavaCast.To(right, typeof(bool)));

            Promote(ref left, ref right, op);

            return Expression.MakeBinary(op, left, right);
        }

        /// <summary>
        /// Concatenation, which is what Java's <c>+</c> means when either side is a string.
        /// </summary>
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(object), typeof(object)])!;

        /// <summary>
        /// Brings two operands to the type Java would evaluate them at.
        /// </summary>
        /// <param name="left"></param>
        /// <param name="right"></param>
        /// <param name="op"></param>
        static void Promote(ref Expression left, ref Expression right, ExpressionType op)
        {
            if (left.Type == right.Type)
                return;

            var l = Ranks.TryGetValue(left.Type, out var lr) ? lr : -1;
            var r = Ranks.TryGetValue(right.Type, out var rr) ? rr : -1;

            if (l >= 0 && r >= 0)
            {
                var type = Promoted[Math.Max(l, r)];
                left = JavaCast.To(left, type);
                right = JavaCast.To(right, type);
                return;
            }

            // Java unboxes the other side when one is a primitive, whatever the operator
            if (l >= 0)
            {
                right = JavaCast.To(right, left.Type);
                return;
            }

            if (r >= 0)
            {
                left = JavaCast.To(left, right.Type);
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
                // Java has no checked conversion: a narrowing cast truncates, which is what JavaCast does.
                // Expression.ConvertChecked would throw on overflow, and also demands a type where the
                // arithmetic operators take none.
                case nameof(J.ExpressionType.Convert):
                case nameof(J.ExpressionType.ConvertChecked):
                    return JavaCast.To(operand, TypeResolver.Resolve(expression.getType()));

                // Java's ! is only ever applied to a boolean; its bitwise complement is a separate operator
                case nameof(J.ExpressionType.Not):
                    return Expression.Not(JavaCast.To(operand, typeof(bool)));

                default:
                    var op = Operator(expression.getNodeType());
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

            return JavaCast.To(operand, Promoted[rank]);
        }

        /// <summary>
        /// Returns the CLR operator a linq4j one stands for.
        /// </summary>
        /// <param name="type"></param>
        /// <returns></returns>
        /// <exception cref="NotSupportedException"></exception>
        /// <remarks>
        /// linq4j took its operators from this enumeration, so the two agree by name. They are matched by
        /// name rather than by ordinal, which is not stable across versions of either.
        /// </remarks>
        static ExpressionType Operator(J.ExpressionType type)
        {
            var name = type.name();

            // the two linq4j spells differently, or has and the CLR does not
            if (name == nameof(J.ExpressionType.Mod))
                name = nameof(ExpressionType.Modulo);
            else if (name == nameof(J.ExpressionType.DivideChecked))
                name = nameof(ExpressionType.Divide);

            if (Enum.TryParse<ExpressionType>(name, false, out var op) == false)
                throw new NotSupportedException($"There is no CLR operator for a linq4j {name}.");

            return op;
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

            var lambda = Expression.Lambda(Scoped(parameters, body, TypeResolver.Resolve(body.getType())), parameters);

            // linq4j declares a lambda against one of its functional interfaces, and a block of Calcite's making
            // uses it as that interface, including where it is passed as an object. So it is one from here, and
            // a node of this convention that wants the delegate asks for it back through TranslateSelector.
            var declared = TypeResolver.Resolve(expression.getType());

            return SamAdapters.Handles(declared) ? SamAdapters.Wrap(declared, lambda) : lambda;
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
            if (value is LambdaExpression lambda && SamAdapters.Handles(type))
                return SamAdapters.Wrap(type, lambda);

            return JavaCast.To(value, type);
        }

    }

}
