using System;
using System.Collections.Generic;
using System.Linq.Expressions;
using System.Reflection;

using Apache.Calcite.Extensions.Adapter.Cursor;
using Apache.Calcite.Extensions.Interop;
using Apache.Calcite.Extensions.Runtime;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Linq4j.Tree
{

    /// <summary>
    /// Translates a linq4j expression tree into a <see cref="System.Linq.Expressions"/> tree.
    /// </summary>
    /// <remarks>
    /// Calcite's code generators (<c>RexToLixTranslator</c>, <c>RexImpTable</c>, the expression-producing
    /// members of <c>PhysType</c>) produce linq4j trees for Janino; this translates their output so it can be
    /// compiled as a CLR expression tree. ("Lix" is Calcite's name for a linq4j expression.)
    ///
    /// <para>Most linq4j nodes map one to one. The exceptions are Java casts, which go through
    /// <see cref="ClrEnumUtils.Convert(Expression, Type)"/> because a Java cast is not a CLR conversion; anonymous classes,
    /// which become lambdas (see <see cref="New"/>); and variables declared part way through a block, which
    /// are hoisted to the start of the block.</para>
    ///
    /// <para>A translator holds the variable bindings and scopes of the tree it is translating, so an instance
    /// is used for one tree, or for trees that share variables.</para>
    /// </remarks>
    sealed class LixToClrTranslator
    {

        /// <summary>
        /// A function being translated, and the label its returns leave by.
        /// </summary>
        /// <param name="Return">The label a <c>return</c> jumps to, typed as the function's return type.</param>
        sealed record Frame(LabelTarget Return);

        /// <summary>
        /// A loop being translated, and the labels its breaks and continues leave by.
        /// </summary>
        /// <param name="Break">The label a <c>break</c> jumps to, after the loop.</param>
        /// <param name="Continue">The label a <c>continue</c> jumps to.</param>
        sealed record Loop(LabelTarget Break, LabelTarget Continue);

        // keyed by reference: linq4j uses one ParameterExpression object for every mention of a variable, and
        // two variables can share a name
        readonly Dictionary<J.ParameterExpression, ParameterExpression> variables = new(ReferenceEqualityComparer.Instance);
        readonly Stack<Frame> frames = new();
        readonly Stack<Loop> loops = new();

        // the parameters of the lambdas being translated, by name. Java resolves names, so a lambda parameter
        // hides any outer variable of the same name, and Calcite relies on that: one generator builds part of
        // a lambda body against a ParameterExpression of its own, another creates the lambda's parameter with
        // the same name, and in the Java source they are one variable
        readonly Stack<Dictionary<string, ParameterExpression>> scopes = new();
        readonly java.util.Map? stashed;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="internalParameters">The implementor's internal parameters, which hold the values
        /// stashed with <c>stash</c>, or <see langword="null"/> if there are none.</param>
        public LixToClrTranslator(java.util.Map? internalParameters = null)
        {
            stashed = internalParameters;
        }

        /// <summary>
        /// Binds a linq4j variable to the CLR variable the translated tree uses for it.
        /// </summary>
        /// <remarks>
        /// Callers bind at least <c>DataContext.ROOT</c>, which <c>RexToLixTranslator</c> uses for dynamic
        /// parameters, <c>CURRENT_TIMESTAMP</c> and <c>USER</c>, and the row parameter of a tree translated
        /// against a row.
        /// </remarks>
        /// <param name="parameter">The linq4j variable, matched by reference.</param>
        /// <param name="target">The CLR variable or parameter every mention of <paramref name="parameter"/> translates to.</param>
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
        /// An expression is first run through <c>OptimizeShuttle</c>, as <c>BlockBuilder.append</c> runs every
        /// tree Calcite compiles. The trees depend on it: a generator may write <c>field == null</c> for a
        /// primitive field, which the shuttle folds away and which would otherwise convert a null to a
        /// primitive and throw.
        ///
        /// <para>Statements are not optimized, because a statement the shuttle removes becomes
        /// <c>OptimizeShuttle.EMPTY_STATEMENT</c>, which only a <c>BlockBuilder</c> filters out.</para>
        /// </remarks>
        public Expression Translate(J.Node node)
        {
            ArgumentNullException.ThrowIfNull(node);

            return Visit(node is J.Expression e ? e.accept(Optimizer) : node);
        }

        /// <summary>
        /// The <c>OptimizeShuttle</c> that <see cref="Translate"/> runs over an expression.
        /// </summary>
        static readonly J.Shuttle Optimizer = new J.OptimizeShuttle();

        /// <summary>
        /// Translates a node, without optimizing it.
        /// </summary>
        /// <param name="node">The linq4j node.</param>
        /// <returns>The equivalent CLR expression.</returns>
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
        /// Translates the body of a function, turning each <c>return</c> into a jump to a label after the
        /// body.
        /// </summary>
        /// <remarks>
        /// A linq4j block can return from anywhere, while an expression tree block yields the value of its
        /// last expression.
        /// </remarks>
        /// <param name="body">The function's body.</param>
        /// <param name="returnType">The function's CLR return type, or <see cref="void"/>.</param>
        /// <returns>A block of <paramref name="returnType"/> ending in the return label; falling off the end yields the type's default.</returns>
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
        /// <param name="node">The linq4j node.</param>
        /// <returns>The translated expression, of type <see cref="void"/>.</returns>
        Expression Statement(J.Node node)
        {
            return Void(Visit(node));
        }

        /// <summary>
        /// Discards the value of an expression used as a statement.
        /// </summary>
        /// <param name="expression">The expression.</param>
        /// <returns><paramref name="expression"/> if it is already of type <see cref="void"/>, otherwise a <see cref="void"/> block around it.</returns>
        static Expression Void(Expression expression)
        {
            return expression.Type == typeof(void) ? expression : Expression.Block(typeof(void), expression);
        }

        /// <summary>
        /// Returns the variable a linq4j parameter stands for, creating it on first mention.
        /// </summary>
        /// <param name="parameter">The linq4j parameter.</param>
        /// <returns>The lambda parameter in scope with that name, the bound variable, or a new variable of the resolved type.</returns>
        ParameterExpression Variable(J.ParameterExpression parameter)
        {
            // lambda parameters by name first, innermost scope out, and before the reference bindings: in the
            // Java source a lambda parameter hides every outer variable of its name, whichever object the
            // mention was built from
            foreach (var scope in scopes)
                if (scope.TryGetValue(parameter.name, out var shadowed))
                    return shadowed;

            if (variables.TryGetValue(parameter, out var variable))
                return variable;

            return variables[parameter] = Expression.Parameter(ClrTypes.Resolve(parameter.getType()), parameter.name);
        }

        /// <summary>
        /// Translates the body of a lambda with its parameters in scope by name.
        /// </summary>
        /// <param name="parameters">The lambda's parameters, which shadow outer variables of the same names.</param>
        /// <param name="body">The lambda's body.</param>
        /// <param name="returnType">The lambda's CLR return type.</param>
        /// <returns>The translated body, as <see cref="TranslateBody"/> returns it.</returns>
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
        /// Returns a constant holding the value a stashed variable stands for, or <see langword="null"/> if the
        /// variable is not stashed or is already bound.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableRelImplementor.stash</c> puts an object in the internal parameters and returns a
        /// variable named for it, which <c>implementRoot</c> declares at the top of the generated method. A
        /// sub-plan translated on its own, as at a converter, never sees that declaration, so the object is
        /// looked up in the shared internal parameters and embedded as a constant, as
        /// <c>ClrCursorRelImplementor.Stash</c> does.
        /// </remarks>
        /// <param name="parameter">The linq4j variable.</param>
        /// <returns>A constant of the variable's resolved type, or <see langword="null"/>.</returns>
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
        /// <remarks>
        /// linq4j holds the value of a primitive constant in a Java box, so a constant typed <c>int</c> holds a
        /// <c>java.lang.Integer</c>, which is unboxed. Any other value, a <c>java.lang.Class</c> included, is
        /// kept as it is: <c>Schemas.queryable</c>, for one, takes a <c>Class</c>.
        /// </remarks>
        /// <param name="expression">The linq4j constant.</param>
        /// <returns>A constant of the resolved type.</returns>
        Expression Constant(J.ConstantExpression expression)
        {
            var type = ClrTypes.Resolve(expression.getType());
            var value = expression.value;

            if (value == null)
                return Expression.Constant(null, type.IsValueType ? typeof(object) : type);

            if (type.IsValueType && value.GetType() != type)
                return Expression.Constant(JavaValues.Unwrap(value, type), type);

            return Expression.Constant(value, type);
        }

        /// <summary>
        /// Translates a variable declaration as an assignment; <see cref="Block"/> declares the variable.
        /// </summary>
        /// <param name="statement">The declaration.</param>
        /// <returns>An assignment of the converted initializer to the variable, or an empty expression where there is no initializer.</returns>
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
        /// <remarks>
        /// Java declares a variable at the point of its declaration statement; an expression tree block declares
        /// its variables up front. The declaration statement stays where it was, as an assignment.
        /// </remarks>
        /// <param name="block">The linq4j block.</param>
        /// <returns>A <see cref="void"/> block that declares the block's variables; an empty block holds an empty expression.</returns>
        Expression Block(J.BlockStatement block)
        {
            TranslateStatements(block, out var declared, out var body);

            if (body.Count == 0)
                body.Add(Expression.Empty());

            return Expression.Block(typeof(void), declared, body);
        }

        /// <summary>
        /// Translates the statements of a block without closing it, so that a caller can add expressions in
        /// the same scope.
        /// </summary>
        /// <remarks>
        /// A correlate uses this: Calcite's block declares the variables holding the outer row's fields, and
        /// the inner sub-plan reads them.
        /// </remarks>
        /// <param name="block">The linq4j block.</param>
        /// <param name="declared">Receives the variables the block declares, which the caller must declare in the block it builds.</param>
        /// <param name="body">Receives the translated statements, in order.</param>
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
        /// <param name="statement">The linq4j conditional statement.</param>
        /// <returns>A nested chain of <c>if</c> / <c>else</c> expressions of type <see cref="void"/>.</returns>
        Expression Conditional(J.ConditionalStatement statement)
        {
            var list = statement.expressionList;
            var count = list.size();

            // the list alternates condition and statement; an odd length ends with the else
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
        /// Translates a return, break, continue, or an expression used as a statement.
        /// </summary>
        /// <param name="statement">The linq4j goto statement.</param>
        /// <returns>A jump to the enclosing frame's or loop's label, or the translated expression.</returns>
        Expression Goto(J.GotoStatement statement)
        {
            switch (statement.kind.name())
            {
                case nameof(J.GotoExpressionKind.Sequence):
                    // linq4j represents an expression statement as a Sequence goto
                    if (statement.expression == null)
                        return Expression.Empty();

                    if (IsStatementExpression(statement.expression) == false)
                        throw new NotSupportedException($"'{statement.expression}' is not allowed as an expression statement: Java takes an assignment, an increment or decrement, a method invocation or an object allocation there, and Janino refuses anything else.");

                    return Void(Visit(statement.expression));

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
        /// Returns whether Java allows an expression to stand as a statement.
        /// </summary>
        /// <param name="expression"></param>
        /// <returns></returns>
        /// <remarks>
        /// An expression tree evaluates anything as a statement and discards the value; Java takes only an
        /// assignment, an increment or decrement, a method invocation or a class instance creation, and Janino
        /// refuses the rest. linq4j produces the rest without meaning to: <c>BlockBuilder.append</c> turns a
        /// trailing <c>return expr;</c> into <c>expr;</c> whenever anything is appended after it, which is
        /// harmless for a call and fatal for a comparison. <c>EnumerableMatch</c> is where it shows — every
        /// pattern definition's condition goes into one builder, so a second definition leaves the first one's
        /// comparison standing as a statement, and Calcite refuses the query. Accepting it here would answer a
        /// query Calcite cannot.
        /// </remarks>
        static bool IsStatementExpression(J.Expression expression)
        {
            return expression switch
            {
                J.MethodCallExpression => true,
                J.NewExpression => true,
                J.BinaryExpression e => e.getNodeType().name().EndsWith("Assign", StringComparison.Ordinal),
                J.UnaryExpression e => e.getNodeType().name().EndsWith("Assign", StringComparison.Ordinal),
                _ => false,
            };
        }

        /// <summary>
        /// Translates a for loop.
        /// </summary>
        /// <remarks>
        /// The continue label is placed before the post expression rather than at the top of the loop, because
        /// a Java <c>continue</c> still runs it.
        /// </remarks>
        /// <param name="statement">The linq4j for statement.</param>
        /// <returns>A block declaring the loop's variables and running the loop.</returns>
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
        /// <param name="statement">The linq4j while statement.</param>
        /// <returns>The translated loop.</returns>
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

            // continue goes to the top of the loop, where the condition is tested again
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
        /// <param name="statement">The linq4j for-each statement.</param>
        /// <returns>A loop that indexes an array, or one that drives the <c>Iterable</c>'s iterator.</returns>
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

                // continue goes to the index increment rather than the top of the loop, as in For
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
        /// <param name="statement">The linq4j try statement.</param>
        /// <returns>The translated try expression.</returns>
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
        /// <param name="expression">The linq4j conditional expression.</param>
        /// <returns>A conditional of the resolved type, with both branches converted to it.</returns>
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
        /// <param name="expression">The linq4j member expression; a static member has no target.</param>
        /// <returns>The field or property read, as <see cref="ClrTypes.Resolve(Expression, J.PseudoField)"/> resolves it.</returns>
        Expression Member(J.MemberExpression expression)
        {
            return ClrTypes.Resolve(expression.expression == null ? null : Visit(expression.expression), expression.field);
        }

        /// <summary>
        /// Translates an array element access.
        /// </summary>
        /// <param name="expression">The linq4j index expression.</param>
        /// <returns>The array access, each index converted to <see cref="int"/>.</returns>
        Expression Index(J.IndexExpression expression)
        {
            var array = Visit(expression.array);

            var indexes = expression.indexExpressions;
            var resolved = new Expression[indexes.size()];
            for (int i = 0; i < indexes.size(); i++)
                resolved[i] = ClrEnumUtils.Convert(Visit((J.Node)indexes.get(i)), typeof(int));

            // ArrayAccess rather than ArrayIndex, because linq4j also uses an index expression as an assignment
            // target
            return Expression.ArrayAccess(array, resolved);
        }

        /// <summary>
        /// Translates an instanceof test.
        /// </summary>
        /// <param name="expression">The linq4j <c>instanceof</c> expression.</param>
        /// <returns>A type test against the resolved type.</returns>
        Expression TypeBinary(J.TypeBinaryExpression expression)
        {
            return Expression.TypeIs(Visit(expression.expression), ClrTypes.Resolve(expression.type));
        }

        /// <summary>
        /// Translates an array creation.
        /// </summary>
        /// <param name="expression">The linq4j array creation.</param>
        /// <returns>An array initialized from the elements, each converted to the element type, or one created with the given bound.</returns>
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
        /// <param name="expression">The linq4j method call.</param>
        /// <returns>The CLR call, or a delegate invocation where the method has no CLR counterpart.</returns>
        Expression Call(J.MethodCallExpression expression)
        {
            var method = ClrTypes.TryResolve(expression.method);
            var target = expression.targetExpression == null ? null : Visit(expression.targetExpression);

            var arguments = expression.expressions;

            // no CLR method of that name and signature exists, as for a method of a remapped class or a ghost
            // interface, so the call goes through a delegate IKVM binds to the member
            if (method == null)
                return Invoke(expression.method, target, arguments);

            // a method IKVM moved off a remapped class is static and takes the receiver as its first argument
            var offset = target != null && method.IsStatic ? 1 : 0;
            var translated = new Expression[arguments.size() + offset];
            if (offset == 1)
                translated[0] = target!;

            for (int i = 0; i < arguments.size(); i++)
                translated[i + offset] = Visit((J.Node)arguments.get(i));

            // the recorded method is advisory: Java chooses the overload from the receiver and argument types
            var argumentTypes = new Type[arguments.size()];
            for (int i = 0; i < argumentTypes.Length; i++)
                argumentTypes[i] = translated[i + offset].Type;

            if (target != null && method.IsStatic == false)
                method = ClrTypes.RebindReceiver(method, target.Type, argumentTypes);

            method = ClrTypes.Rebind(method, Array.ConvertAll(translated, e => e.Type));

            var parameters = method.GetParameters();
            var resolved = BindVarArgs(parameters, translated);
            if (resolved == null)
            {
                resolved = new Expression[translated.Length];
                for (int i = 0; i < translated.Length; i++)
                    resolved[i] = Coerce(translated[i], parameters[i].ParameterType);
            }

            if (method.IsStatic)
                return Expression.Call(null, method, resolved);

            return Expression.Call(ClrEnumUtils.Convert(target!, method.DeclaringType!), method, resolved);
        }

        /// <summary>
        /// Translates a method call as an invocation of a delegate IKVM binds to the method.
        /// </summary>
        /// <remarks>
        /// Used for a method with no CLR method of the same name and signature, such as one of a remapped
        /// class or a ghost interface. The delegate from <c>JavaDelegates.FromMethod</c> takes the receiver
        /// first and types every parameter as <see cref="object"/> or a primitive, so each argument needs at
        /// most a reference conversion or a Java boxing, which <see cref="Coerce"/> performs.
        /// </remarks>
        /// <param name="method">The Java method.</param>
        /// <param name="target">The receiver, or <see langword="null"/> for a static method.</param>
        /// <param name="arguments">The linq4j argument expressions.</param>
        /// <returns>An invocation of the method's delegate.</returns>
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
        /// Translates a constructor call or an anonymous class.
        /// </summary>
        /// <param name="expression">The linq4j <c>new</c> expression.</param>
        /// <returns>The constructor call, or the anonymous class as <see cref="Anonymous"/> translates it.</returns>
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
        /// <param name="type">The type to construct.</param>
        /// <param name="arguments">The linq4j argument expressions.</param>
        /// <returns>A <c>new</c> expression calling the first constructor the arguments fit, packing varargs if they must.</returns>
        /// <exception cref="NotSupportedException">No constructor takes the arguments.</exception>
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

                // a varargs call is left to the pass below, which packs the trailing arguments into the array
                if (IsVarArgs(parameters, resolved))
                    continue;

                var arms = new Expression[resolved.Length];
                for (int i = 0; i < resolved.Length; i++)
                    arms[i] = Coerce(resolved[i], parameters[i].ParameterType);

                return Expression.New(candidate, arms);
            }

            foreach (var candidate in type.GetConstructors())
                if (BindVarArgs(candidate.GetParameters(), resolved) is Expression[] packed)
                    return Expression.New(candidate, packed);

            throw new NotSupportedException($"'{type}' has no constructor taking {resolved.Length} arguments.");
        }

        /// <summary>
        /// Returns whether the arguments bind to the parameters as a varargs call.
        /// </summary>
        /// <remarks>
        /// As in Java: the last parameter is an array, and either the argument count differs from the parameter
        /// count or the last argument is not assignable to the array type. A call that passes the array itself
        /// is an ordinary call.
        /// </remarks>
        /// <param name="parameters">The candidate method's parameters.</param>
        /// <param name="arguments">The translated arguments.</param>
        /// <returns><see langword="true"/> if the trailing arguments must be packed into the last parameter's array.</returns>
        static bool IsVarArgs(ParameterInfo[] parameters, Expression[] arguments)
        {
            if (parameters.Length == 0 || arguments.Length < parameters.Length - 1)
                return false;

            var last = parameters[^1].ParameterType;
            if (last.IsArray == false)
                return false;

            return arguments.Length != parameters.Length || last.IsAssignableFrom(arguments[^1].Type) == false;
        }

        /// <summary>
        /// Returns the arguments of a varargs call with the trailing ones packed into an array, or
        /// <see langword="null"/> if the call is not a varargs call.
        /// </summary>
        /// <remarks>
        /// The Java compiler packs varargs; a linq4j tree carries them individually. <c>CAST(x AS VARIANT)</c>
        /// produces such calls through <c>RuntimeTypeInformation.createExpression</c>, for example
        /// <c>new GenericSqlTypeRtti(MAP, key, value)</c> and <c>new RowSqlTypeRtti(entry, entry)</c>. Each
        /// packed element is coerced to the array's element type, since the row case passes
        /// <c>AbstractMap.SimpleEntry</c> values for a <c>Map.Entry[]</c>.
        /// </remarks>
        /// <param name="parameters">The candidate method's parameters.</param>
        /// <param name="arguments">The translated arguments.</param>
        /// <returns>One argument per parameter, the last an array of the trailing arguments, or <see langword="null"/>.</returns>
        Expression[]? BindVarArgs(ParameterInfo[] parameters, Expression[] arguments)
        {
            if (IsVarArgs(parameters, arguments) == false)
                return null;

            var element = parameters[^1].ParameterType.GetElementType()!;
            var arms = new Expression[parameters.Length];
            for (int i = 0; i < parameters.Length - 1; i++)
                arms[i] = Coerce(arguments[i], parameters[i].ParameterType);

            var rest = new Expression[arguments.Length - parameters.Length + 1];
            for (int i = 0; i < rest.Length; i++)
                rest[i] = Coerce(arguments[parameters.Length - 1 + i], element);

            arms[^1] = Expression.NewArrayInit(element, rest);
            return arms;
        }

        /// <summary>
        /// Translates an anonymous class into a lambda, or a lambda per method, wrapped in an adapter of the
        /// declared type.
        /// </summary>
        /// <remarks>
        /// An expression tree cannot declare a class. A class of one method, such as the anonymous
        /// <c>Comparator</c> <c>PhysType.generateComparator</c> produces, becomes that method as a lambda; an
        /// erasure bridge method alongside it is dropped. A multi-method type known to
        /// <see cref="AnonymousClasses"/>, such as <c>Enumerator</c>, becomes a lambda per method.
        ///
        /// <para>The class's fields, such as the constants linq4j's <c>DeterministicCodeOptimizer</c> hoists into
        /// fields, become variables of a block that assigns them once and then creates the adapter, whose
        /// lambdas close over them.</para>
        /// </remarks>
        /// <param name="type">The declared type of the anonymous class.</param>
        /// <param name="expression">The linq4j <c>new</c> expression with member declarations.</param>
        /// <returns>An adapter of <paramref name="type"/>, or a block that assigns the class's fields and then yields it.</returns>
        /// <exception cref="NotSupportedException">The class has constructor arguments, declares no method, or declares a member that is neither a method nor a field.</exception>
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
                // a multi-method type: one lambda per method
                var declared = new Dictionary<string, LambdaExpression>();
                foreach (var method in methods)
                    declared[method.name] = Lambda(method);

                wrapped = AnonymousClasses.WrapClass(type, declared);
            }
            else
            {
                // wrapped as the declared interface, since consumers also receive values of it that were never
                // anonymous classes
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
        /// Translates one method of an anonymous class into a lambda.
        /// </summary>
        /// <param name="declaration">The method declaration.</param>
        /// <returns>A lambda over the method's parameters, whose body is translated with them in scope by name.</returns>
        LambdaExpression Lambda(J.MethodDeclaration declaration)
        {
            var parameters = new ParameterExpression[declaration.parameters.size()];
            for (int i = 0; i < parameters.Length; i++)
                parameters[i] = Variable((J.ParameterExpression)declaration.parameters.get(i));

            return Expression.Lambda(Scoped(parameters, declaration.body, ClrTypes.Resolve(declaration.resultType)), parameters);
        }

        /// <summary>
        /// Picks the method that carries the body from a set that also holds erasure bridge methods.
        /// </summary>
        /// <param name="type">The declared type, named in the exception.</param>
        /// <param name="methods">The declarations of one method name.</param>
        /// <returns>The one declaration whose parameters are not all <see cref="object"/>.</returns>
        /// <exception cref="NotSupportedException">No single declaration qualifies.</exception>
        static J.MethodDeclaration Unbridged(Type type, List<J.MethodDeclaration> methods)
        {
            // a bridge takes the interface's erased parameters, all Object; the real method takes the row type
            var candidates = methods.FindAll(m => AllObject(m) == false);
            if (candidates.Count == 1)
                return candidates[0];

            throw new NotSupportedException($"An anonymous '{type}' declares {methods.Count} methods and no single one of them carries the body.");
        }

        /// <summary>
        /// Returns whether every parameter of a declaration is <see cref="object"/>.
        /// </summary>
        /// <param name="method">The method declaration.</param>
        /// <returns><see langword="true"/> if it has at least one parameter and every parameter resolves to <see cref="object"/>.</returns>
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
        /// The type each rank promotes to; as in Java, anything narrower than <c>int</c> becomes <c>int</c>.
        /// </summary>
        static readonly Type[] Promoted = [typeof(bool), typeof(int), typeof(int), typeof(int), typeof(long), typeof(float), typeof(double)];

        /// <summary>
        /// Translates a binary operator.
        /// </summary>
        /// <param name="expression">The linq4j binary expression.</param>
        /// <returns>The CLR binary expression, with operands converted as Java would evaluate them.</returns>
        Expression Binary(J.BinaryExpression expression)
        {
            var left = Visit(expression.expression0);
            var right = Visit(expression.expression1);
            var op = Operator(expression.getNodeType());

            if (op == ExpressionType.Assign)
                return Expression.Assign(left, ClrEnumUtils.Convert(right, left.Type));

            if (CompoundAssignments.Contains(op))
                return Expression.MakeBinary(op, left, ClrEnumUtils.Convert(right, left.Type));

            // a shift distance is an int whatever the width of the value shifted
            if (op is ExpressionType.LeftShift or ExpressionType.RightShift)
                return Expression.MakeBinary(op, left, ClrEnumUtils.Convert(right, typeof(int)));

            if (op == ExpressionType.Add && ClrTypes.Resolve(expression.getType()) == typeof(string))
                return Expression.Call(Concat, ClrEnumUtils.Convert(left, typeof(object)), ClrEnumUtils.Convert(right, typeof(object)));

            // Java unboxes a Boolean operand of && and || implicitly; the CLR operators need bool, so the
            // unboxing is explicit. A condition over a nullable column is a Boolean.
            if (op is ExpressionType.AndAlso or ExpressionType.OrElse)
                return Expression.MakeBinary(op, ClrEnumUtils.Convert(left, typeof(bool)), ClrEnumUtils.Convert(right, typeof(bool)));

            Promote(ref left, ref right, op);

            return Expression.MakeBinary(op, left, right);
        }

        /// <summary>
        /// String concatenation, which Java's <c>+</c> means when the result is a string.
        /// </summary>
        static readonly MethodInfo Concat = typeof(string).GetMethod(nameof(string.Concat), [typeof(object), typeof(object)])
            ?? throw new InvalidOperationException("String has no Concat(object, object).");

        /// <summary>
        /// The members a for-each loop over a <c>java.lang.Iterable</c> calls.
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
        /// The compound assignment operators, whose left operand must not be promoted.
        /// </summary>
        /// <remarks>
        /// Listed explicitly rather than matched by name, because three end in <c>Checked</c>.
        /// <see cref="Promote"/> may wrap an operand in a conversion, which cannot be assigned to.
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
        /// Converts two operands to the type Java would evaluate the operator at.
        /// </summary>
        /// <param name="left">The left operand; replaced by its converted form.</param>
        /// <param name="right">The right operand; replaced by its converted form.</param>
        /// <param name="op">The operator, which decides whether two boxes of one type are unboxed.</param>
        static void Promote(ref Expression left, ref Expression right, ExpressionType op)
        {
            if (left.Type == right.Type)
            {
                // two boxes of one type: Java compares references for == and != and unboxes for anything else
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

            // with one primitive operand, Java unboxes the other whatever the operator
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

            // two references, which only == and != apply to, converted to a common type
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
        /// <param name="expression">The linq4j unary expression.</param>
        /// <returns>The conversion for a cast, the logical negation for <c>!</c>, or the CLR operator over the promoted operand.</returns>
        Expression Unary(J.UnaryExpression expression)
        {
            var operand = Visit(expression.expression);

            switch (expression.getNodeType().name())
            {
                // a Java narrowing cast truncates, as ClrEnumUtils.Convert does; ConvertChecked would throw on
                // overflow
                case nameof(J.ExpressionType.Convert):
                case nameof(J.ExpressionType.ConvertChecked):
                    return ClrEnumUtils.Convert(operand, ClrTypes.Resolve(expression.getType()));

                // Java's ! applies only to a boolean; bitwise complement is a separate operator
                case nameof(J.ExpressionType.Not):
                    return Expression.Not(ClrEnumUtils.Convert(operand, typeof(bool)));

                default:
                    var op = Operator(expression.getNodeType());

                    // null means "no conversion type", which these operators take, although the parameter is
                    // annotated non-nullable
                    return Expression.MakeUnary(op, Widen(operand), null!);
            }
        }

        /// <summary>
        /// Promotes an operand narrower than <c>int</c>, as Java does before a unary operator.
        /// </summary>
        /// <param name="operand">The operand.</param>
        /// <returns>The operand converted to the type Java promotes it to if it is a ranked primitive, otherwise unchanged.</returns>
        static Expression Widen(Expression operand)
        {
            if (Ranks.TryGetValue(operand.Type, out var rank) == false)
                return operand;

            return ClrEnumUtils.Convert(operand, Promoted[rank]);
        }

        /// <summary>
        /// Returns the CLR operator a linq4j one stands for.
        /// </summary>
        /// <remarks>
        /// The two enumerations mostly share names but not entirely: linq4j has both <c>Mod</c> and
        /// <c>Modulo</c>, and the CLR has no checked divide. So each operator is mapped explicitly rather than
        /// by parsing its name. Dispatch is on the Java enum's name, since ordinals are not stable across
        /// versions, with <c>nameof</c> labels so that an operator removed from linq4j fails to compile here.
        /// </remarks>
        /// <param name="type">The linq4j operator.</param>
        /// <returns>The CLR operator.</returns>
        /// <exception cref="NotSupportedException">The operator has no CLR counterpart.</exception>
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
        /// <param name="expression">The linq4j function expression.</param>
        /// <returns>The lambda, wrapped as its declared interface where <see cref="AnonymousClasses"/> has an adapter for it.</returns>
        /// <exception cref="NotSupportedException">The function has no body.</exception>
        Expression Function(J.FunctionExpression expression)
        {
            var body = expression.body ?? throw new NotSupportedException("A lambda with no body cannot be translated.");

            var parameters = new ParameterExpression[expression.parameterList.size()];
            for (int i = 0; i < parameters.Length; i++)
                parameters[i] = Variable((J.ParameterExpression)expression.parameterList.get(i));

            var lambda = Expression.Lambda(Scoped(parameters, body, ClrTypes.Resolve(body.getType())), parameters);

            // a lambda declared against a linq4j functional interface is used as that interface by Calcite's
            // code, including where it is passed as an object, so it is wrapped; an operator that wants the
            // delegate recovers it with AnonymousClasses.Unwrap
            var declared = ClrTypes.Resolve(expression.getType());

            return AnonymousClasses.Handles(declared) ? AnonymousClasses.Wrap(declared, lambda) : lambda;
        }

        /// <summary>
        /// Converts a value to the type of the parameter it is passed as.
        /// </summary>
        /// <remarks>
        /// A lambda passed where one of the interfaces <see cref="AnonymousClasses"/> adapts is expected is
        /// wrapped as that interface; anything else goes through <see cref="ClrEnumUtils.Convert(Expression, Type)"/>.
        /// </remarks>
        /// <param name="value">The argument.</param>
        /// <param name="type">The parameter type.</param>
        /// <returns><paramref name="value"/> wrapped or converted to <paramref name="type"/>.</returns>
        static Expression Coerce(Expression value, Type type)
        {
            if (value is LambdaExpression lambda && AnonymousClasses.Handles(type))
                return AnonymousClasses.Wrap(type, lambda);

            return ClrEnumUtils.Convert(value, type);
        }

    }

}
