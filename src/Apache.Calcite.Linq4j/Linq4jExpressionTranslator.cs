using System;
using System.Collections.Generic;
using System.Reflection;

using FastExpressionCompiler.LightExpression;

using java.util;

using org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq4j
{

    /// <summary>
    /// Translates a linq4j expression tree into an equivalent CLR
    /// <see cref="FastExpressionCompiler.LightExpression.Expression"/> tree.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Trees are built using <see cref="FastExpressionCompiler.LightExpression.Expression"/>
    /// (low-allocation light nodes) and compiled via <c>CompileFast()</c> rather than the BCL
    /// <c>System.Linq.Expressions</c> path.
    /// </para>
    /// <para>
    /// Types are resolved via <c>ikvm.runtime.Util.getInstanceTypeFromClass</c>;
    /// direct casts from <c>java.lang.Class</c> to <see cref="System.Type"/> are not
    /// supported by IKVM.
    /// </para>
    /// <para>
    /// <see cref="ExpressionType"/> values are Java enum-style singletons and must be
    /// compared by reference equality against their static properties, not cast to an
    /// inner <c>__Enum</c> type.
    /// </para>
    /// <para>
    /// A single <see cref="Linq4jExpressionTranslator"/> instance is <b>not</b> thread-safe.
    /// Create a new instance for each independent translation.
    /// </para>
    /// </remarks>
    public class Linq4jExpressionTranslator : VisitorImpl
    {

        // Variable scope: maps linq4j ParameterExpression (by identity) to the CLR ParameterExpression.
        readonly Dictionary<ParameterExpression, ParameterExpression> _scope
            = new(ReferenceEqualityComparer.Instance);

        // Label targets: maps label name to the CLR LabelTarget.
        readonly Dictionary<string, LabelTarget> _labels = [];

        // ─────────────────────────────────────────────────────────────────────
        // Public entry points
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Translates a linq4j <see cref="FunctionExpression"/> to a CLR
        /// <see cref="LambdaExpression"/>.
        /// </summary>
        public LambdaExpression TranslateLambda(FunctionExpression node)
        {
            ArgumentNullException.ThrowIfNull(node);

            var paramList = node.parameterList.AsList<ParameterExpression>();
            var parameters = new ParameterExpression[paramList.Count];
            for (int i = 0; i < paramList.Count; i++)
                parameters[i] = GetOrCreateParameter(paramList[i]);

            Expression body;
            if (node.body is not null)
            {
                body = Translate(node.body);
            }
            else if (node.function is not null)
            {
                body = Expression.Constant(node.function);
            }
            else
            {
                throw new NotSupportedException("FunctionExpression has neither a body nor a function.");
            }

            return Expression.Lambda(body, parameters);
        }

        /// <summary>
        /// Translates any linq4j <see cref="Node"/> to a CLR
        /// <see cref="Expression"/> by dispatching through <c>node.accept(this)</c>.
        /// </summary>
        public Expression Translate(Node node)
        {
            ArgumentNullException.ThrowIfNull(node);
            return (Expression)node.accept(this);
        }

        /// <summary>
        /// Seeds the translator with field declarations from a <see cref="ClassDeclaration"/>
        /// and returns the CLR variable expressions plus their initialisation assignments so
        /// the caller can wrap method bodies in an appropriately scoped block.
        /// </summary>
        public (ParameterExpression[] Variables, Expression[] Inits)
            TranslateClassDeclaration(ClassDeclaration classDeclaration)
        {
            ArgumentNullException.ThrowIfNull(classDeclaration);

            var fields = new List<ParameterExpression>();
            var inits = new List<Expression>();

            foreach (var member in classDeclaration.memberDeclarations.AsList<MemberDeclaration>())
            {
                if (member is not FieldDeclaration fd)
                    continue;

                var clrParam = GetOrCreateParameter(fd.parameter);
                fields.Add(clrParam);

                if (fd.initializer is not null)
                {
                    var initExpr = Translate(fd.initializer);
                    inits.Add(Expression.Assign(clrParam, initExpr));
                }
            }

            return (fields.ToArray(), inits.ToArray());
        }

        // ─────────────────────────────────────────────────────────────────────
        // VisitorImpl overrides
        // ─────────────────────────────────────────────────────────────────────

        /// <inheritdoc/>
        public override object visit(ConstantExpression node)
        {
            if (node.value is null)
                return Expression.Constant(null, ClrType(node.getType()));

            return Expression.Constant(node.value, ClrType(node.getType()));
        }

        /// <inheritdoc/>
        public override object visit(BinaryExpression node)
        {
            var left = Translate(node.expression0);
            var right = Translate(node.expression1);
            var nt = node.nodeType;

            if (nt == ExpressionType.Add) return Expression.Add(left, right);
            if (nt == ExpressionType.AddChecked) return Expression.AddChecked(left, right);
            if (nt == ExpressionType.Subtract) return Expression.Subtract(left, right);
            if (nt == ExpressionType.SubtractChecked) return Expression.SubtractChecked(left, right);
            if (nt == ExpressionType.Multiply) return Expression.Multiply(left, right);
            if (nt == ExpressionType.MultiplyChecked) return Expression.MultiplyChecked(left, right);
            if (nt == ExpressionType.Divide) return Expression.Divide(left, right);
            if (nt == ExpressionType.Modulo) return Expression.Modulo(left, right);
            if (nt == ExpressionType.Power) return Expression.Power(left, right);
            if (nt == ExpressionType.And) return Expression.And(left, right);
            if (nt == ExpressionType.Or) return Expression.Or(left, right);
            if (nt == ExpressionType.ExclusiveOr) return Expression.ExclusiveOr(left, right);
            if (nt == ExpressionType.LeftShift) return Expression.LeftShift(left, right);
            if (nt == ExpressionType.RightShift) return Expression.RightShift(left, right);
            if (nt == ExpressionType.AndAlso) return Expression.AndAlso(left, right);
            if (nt == ExpressionType.OrElse) return Expression.OrElse(left, right);
            if (nt == ExpressionType.Equal) return Expression.Equal(left, right);
            if (nt == ExpressionType.NotEqual) return Expression.NotEqual(left, right);
            if (nt == ExpressionType.LessThan) return Expression.LessThan(left, right);
            if (nt == ExpressionType.LessThanOrEqual) return Expression.LessThanOrEqual(left, right);
            if (nt == ExpressionType.GreaterThan) return Expression.GreaterThan(left, right);
            if (nt == ExpressionType.GreaterThanOrEqual) return Expression.GreaterThanOrEqual(left, right);
            if (nt == ExpressionType.Coalesce) return Expression.Coalesce(left, right);
            if (nt == ExpressionType.ArrayIndex) return Expression.ArrayIndex(left, right);
            if (nt == ExpressionType.Assign) return Expression.Assign(left, right);
            if (nt == ExpressionType.AddAssign) return Expression.AddAssign(left, right);
            if (nt == ExpressionType.AddAssignChecked) return Expression.AddAssignChecked(left, right);
            if (nt == ExpressionType.SubtractAssign) return Expression.SubtractAssign(left, right);
            if (nt == ExpressionType.SubtractAssignChecked) return Expression.SubtractAssignChecked(left, right);
            if (nt == ExpressionType.MultiplyAssign) return Expression.MultiplyAssign(left, right);
            if (nt == ExpressionType.MultiplyAssignChecked) return Expression.MultiplyAssignChecked(left, right);
            if (nt == ExpressionType.DivideAssign) return Expression.DivideAssign(left, right);
            if (nt == ExpressionType.ModuloAssign) return Expression.ModuloAssign(left, right);
            if (nt == ExpressionType.AndAssign) return Expression.AndAssign(left, right);
            if (nt == ExpressionType.OrAssign) return Expression.OrAssign(left, right);
            if (nt == ExpressionType.ExclusiveOrAssign) return Expression.ExclusiveOrAssign(left, right);
            if (nt == ExpressionType.LeftShiftAssign) return Expression.LeftShiftAssign(left, right);
            if (nt == ExpressionType.RightShiftAssign) return Expression.RightShiftAssign(left, right);

            throw new NotSupportedException($"Unsupported binary ExpressionType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(UnaryExpression node)
        {
            var operand = Translate(node.expression);
            var resultType = ClrType(node.getType());
            var nt = node.nodeType;

            if (nt == ExpressionType.Negate) return Expression.Negate(operand);
            if (nt == ExpressionType.NegateChecked) return Expression.NegateChecked(operand);
            if (nt == ExpressionType.Not) return Expression.Not(operand);
            if (nt == ExpressionType.OnesComplement) return Expression.OnesComplement(operand);
            if (nt == ExpressionType.UnaryPlus) return Expression.UnaryPlus(operand);
            if (nt == ExpressionType.Convert) return Expression.Convert(operand, resultType);
            if (nt == ExpressionType.ConvertChecked) return Expression.ConvertChecked(operand, resultType);
            if (nt == ExpressionType.TypeAs) return Expression.TypeAs(operand, resultType);
            if (nt == ExpressionType.ArrayLength) return Expression.ArrayLength(operand);
            if (nt == ExpressionType.Quote) return Expression.Quote(operand);
            if (nt == ExpressionType.Throw) return Expression.Throw(operand);
            if (nt == ExpressionType.Increment) return Expression.Increment(operand);
            if (nt == ExpressionType.Decrement) return Expression.Decrement(operand);
            if (nt == ExpressionType.PreIncrementAssign) return Expression.PreIncrementAssign(operand);
            if (nt == ExpressionType.PreDecrementAssign) return Expression.PreDecrementAssign(operand);
            if (nt == ExpressionType.PostIncrementAssign) return Expression.PostIncrementAssign(operand);
            if (nt == ExpressionType.PostDecrementAssign) return Expression.PostDecrementAssign(operand);
            if (nt == ExpressionType.IsTrue) return Expression.IsTrue(operand);
            if (nt == ExpressionType.IsFalse) return Expression.IsFalse(operand);

            throw new NotSupportedException($"Unsupported unary ExpressionType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(TernaryExpression node)
        {
            // Ternary maps to Expression.Condition(test, ifTrue, ifFalse).
            var test = Translate(node.expression0);
            var ifTrue = Translate(node.expression1);
            var ifFalse = Translate(node.expression2);
            return Expression.Condition(test, ifTrue, ifFalse);
        }

        /// <summary>
        /// Translates a linq4j <see cref="ConditionalExpression"/> (an if/else-if/else chain
        /// encoded as a flat list of [condition, body, condition, body, …, optional-else])
        /// into a nested CLR conditional expression.
        /// </summary>
        public override object visit(ConditionalExpression node)
        {
            // expressionList is a package-private field; access via reflection.
            var listField = typeof(ConditionalExpression).GetField(
                "expressionList", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
            var exprs = ((java.util.List)listField.GetValue(node)!).AsList<Node>();
            return BuildIfElseChain(exprs, 0);
        }

        /// <inheritdoc/>
        public override object visit(ConditionalStatement node)
        {
            var exprs = node.expressionList.AsList<Node>();
            return BuildIfElseChain(exprs, 0);
        }

        /// <inheritdoc/>
        public override object visit(BlockStatement node)
        {
            var stmts = node.statements.AsList<Node>();
            if (stmts.Count == 0)
                return Expression.Empty();

            var vars = new List<ParameterExpression>();
            var body = new List<Expression>(stmts.Count);

            foreach (var s in stmts)
            {
                // Hoist variable declarations into the block's variable list.
                if (s is DeclarationStatement ds)
                {
                    var p = GetOrCreateParameter(ds.parameter);
                    vars.Add(p);
                    if (ds.initializer is not null)
                        body.Add(Expression.Assign(p, Translate(ds.initializer)));
                }
                else
                {
                    body.Add(Translate(s));
                }
            }

            return Expression.Block(vars, body);
        }

        /// <inheritdoc/>
        public override object visit(DeclarationStatement node)
        {
            // When encountered outside a BlockStatement we just translate the initialiser
            // (the variable was already hoisted by the block visitor above).
            if (node.initializer is not null)
                return Translate(node.initializer);

            return Expression.Empty();
        }

        /// <inheritdoc/>
        public override object visit(ParameterExpression node)
        {
            return GetOrCreateParameter(node);
        }

        /// <inheritdoc/>
        public override object visit(MemberExpression node)
        {
            if (node.expression is null)
            {
                // Static member access.
                var clrType = ClrType(node.getType());
                var fieldInf = clrType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Static)
                            ?? clrType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Static);
                if (fieldInf is not null)
                    return Expression.Field(null, fieldInf);

                var propInf = clrType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Static);
                if (propInf is not null)
                    return Expression.Property(null, propInf);

                throw new NotSupportedException($"Cannot resolve static member '{node.field.getName()}' on '{clrType}'.");
            }

            var instance = Translate(node.expression);
            var instType = instance.Type;

            var iField = instType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Instance)
                      ?? instType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Instance);
            if (iField is not null)
                return Expression.Field(instance, iField);

            var iProp = instType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Instance);
            if (iProp is not null)
                return Expression.Property(instance, iProp);

            throw new NotSupportedException($"Cannot resolve instance member '{node.field.getName()}' on '{instType}'.");
        }

        /// <inheritdoc/>
        public override object visit(MethodCallExpression node)
        {
            var argList = node.expressions.AsList<Node>();
            var args = new Expression[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            if (node.targetExpression is null)
            {
                // Static call.
                var clrMethod = ResolveMethod(node.method);
                return Expression.Call(clrMethod, args);
            }

            var target = Translate(node.targetExpression);
            var method = ResolveMethod(node.method);
            return Expression.Call(target, method, args);
        }

        /// <inheritdoc/>
        public override object visit(NewExpression node)
        {
            var clrType = ClrType(node.getType());
            var argList = node.arguments.AsList<Node>();
            var args = new Expression[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            var ctor = ResolveConstructor(clrType, args);
            return Expression.New(ctor, args);
        }

        /// <inheritdoc/>
        public override object visit(NewArrayExpression node)
        {
            var elemType = ClrType(node.getType());

            if (node.bound is not null)
            {
                // new T[n]
                return Expression.NewArrayBounds(elemType, [Translate(node.bound)]);
            }

            // new T[] { e0, e1, … }
            var exprList = node.expressions.AsList<Node>();
            var inits = new Expression[exprList.Count];
            for (int i = 0; i < exprList.Count; i++)
                inits[i] = Translate(exprList[i]);

            return Expression.NewArrayInit(elemType, inits);
        }

        /// <inheritdoc/>
        public override object visit(IndexExpression node)
        {
            var array = Translate(node.array);
            var idxList = node.indexExpressions.AsList<Node>();

            if (idxList.Count == 1)
                return Expression.ArrayIndex(array, Translate(idxList[0]));

            var indices = new Expression[idxList.Count];
            for (int i = 0; i < idxList.Count; i++)
                indices[i] = Translate(idxList[i]);

            return Expression.ArrayAccess(array, indices);
        }

        /// <inheritdoc/>
        public override object visit(TypeBinaryExpression node)
        {
            var operand = Translate(node.expression);
            var nt = node.nodeType;
            var clrType = ClrType(node.getType());

            if (nt == ExpressionType.TypeIs) return Expression.TypeIs(operand, clrType);
            if (nt == ExpressionType.TypeAs) return Expression.TypeAs(operand, clrType);

            throw new NotSupportedException($"Unsupported TypeBinaryExpression nodeType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(FunctionExpression node)
        {
            return TranslateLambda(node);
        }

        /// <inheritdoc/>
        public override object visit(GotoStatement node)
        {
            var kind = node.kind;
            var labelName = node.labelTarget?.name ?? string.Empty;
            var label = GetOrCreateLabel(labelName);

            if (kind == GotoExpressionKind.Return)
            {
                return node.expression is not null
                    ? Expression.Return(label, Translate(node.expression))
                    : Expression.Return(label);
            }

            if (kind == GotoExpressionKind.Break)
                return Expression.Break(label);

            if (kind == GotoExpressionKind.Continue)
                return Expression.Continue(label);

            // GotoExpressionKind.Goto
            return node.expression is not null
                ? Expression.Goto(label, Translate(node.expression))
                : Expression.Goto(label);
        }

        /// <inheritdoc/>
        public override object visit(LabelStatement node)
        {
            // Use the node's identity hash as the label key so that matching GotoStatements
            // can resolve the same target.
            var labelKey = node.GetHashCode().ToString();
            var target = GetOrCreateLabel(labelKey);
            var defaultVal = Translate(node.defaultValue);
            return Expression.Label(target, defaultVal);
        }

        /// <inheritdoc/>
        public override object visit(ForStatement node)
        {
            // Calcite's ForStatement maps to:
            //   Block(declarations,
            //     Loop(
            //       Block(
            //         IfThen(!condition, Break),
            //         body,
            //         post)))
            var breakLabel = Expression.Label("for_break");
            var continueLabel = Expression.Label("for_continue");

            var declList = node.declarations.AsList<DeclarationStatement>();
            var declVars = new List<ParameterExpression>(declList.Count);
            var declInits = new List<Expression>(declList.Count);

            foreach (var d in declList)
            {
                var p = GetOrCreateParameter(d.parameter);
                declVars.Add(p);
                if (d.initializer is not null)
                    declInits.Add(Expression.Assign(p, Translate(d.initializer)));
            }

            var loopBodyStmts = new List<Expression>();

            if (node.condition is not null)
                loopBodyStmts.Add(Expression.IfThen(Expression.Not(Translate(node.condition)), Expression.Break(breakLabel)));

            loopBodyStmts.Add(Translate(node.body));

            if (node.post is not null)
                loopBodyStmts.Add(Translate(node.post));

            Expression loop = Expression.Loop(Expression.Block(loopBodyStmts), breakLabel, continueLabel);

            if (declVars.Count > 0)
            {
                var outerStmts = new Expression[declInits.Count + 1];
                declInits.CopyTo(outerStmts);
                outerStmts[declInits.Count] = loop;
                return Expression.Block(declVars, outerStmts);
            }

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(ForEachStatement node)
        {
            // foreach (T item in iterable) body
            // Compiles to a while loop over GetEnumerator() / MoveNext() / Current.
            var iterableExpr = Translate(node.iterable);
            var itemVar = GetOrCreateParameter(node.parameter);
            var breakLabel = Expression.Label("foreach_break");
            var continueLabel = Expression.Label("foreach_continue");

            var enumeratorType = typeof(System.Collections.IEnumerator);
            var getEnumerator = iterableExpr.Type.GetMethod("GetEnumerator")
                              ?? typeof(System.Collections.IEnumerable).GetMethod("GetEnumerator")!;
            var moveNext = enumeratorType.GetMethod("MoveNext")!;
            var currentProp = enumeratorType.GetProperty("Current")!;

            var enumeratorVar = Expression.Variable(enumeratorType, "enumerator");

            var loop = Expression.Block(
                [enumeratorVar, itemVar],
                Expression.Assign(enumeratorVar, Expression.Convert(Expression.Call(iterableExpr, getEnumerator), enumeratorType)),
                Expression.Loop(
                    Expression.Block(
                        Expression.IfThen(Expression.Not(Expression.Call(enumeratorVar, moveNext)), Expression.Break(breakLabel)),
                        Expression.Assign(itemVar, Expression.Convert(Expression.Property(enumeratorVar, currentProp), itemVar.Type)),
                        Translate(node.body)),
                    breakLabel,
                    continueLabel));

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(WhileStatement node)
        {
            var breakLabel = Expression.Label("while_break");
            var continueLabel = Expression.Label("while_continue");

            var loop = Expression.Loop(
                Expression.Block(
                    Expression.IfThen(Expression.Not(Translate(node.condition)), Expression.Break(breakLabel)),
                    Translate(node.body)),
                breakLabel,
                continueLabel);

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(TryStatement node)
        {
            var body = Translate(node.body);

            var catchBlocks = new List<CatchBlock>();
            foreach (var cb in node.catchBlocks.AsList<CatchBlock>())
            {
                var exVar = GetOrCreateParameter(cb.parameter);
                var cbBody = Translate(cb.body);
                catchBlocks.Add(Expression.Catch(exVar, cbBody));
            }

            if (node.fynally is not null)
            {
                return catchBlocks.Count > 0
                    ? Expression.TryCatchFinally(body, Translate(node.fynally), [.. catchBlocks])
                    : Expression.TryFinally(body, Translate(node.fynally));
            }

            return Expression.TryCatch(body, [.. catchBlocks]);
        }

        /// <inheritdoc/>
        public override object visit(ThrowStatement node)
        {
            return Expression.Throw(Translate(node.expression));
        }

        /// <inheritdoc/>
        public override object visit(DefaultExpression node)
        {
            return Expression.Default(ClrType(node.getType()));
        }

        // ─────────────────────────────────────────────────────────────────────
        // Helpers
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Returns the <see cref="ParameterExpression"/> for the given linq4j parameter,
        /// creating one if it has not been seen before.
        /// </summary>
        internal ParameterExpression GetOrCreateParameter(ParameterExpression node)
        {
            if (_scope.TryGetValue(node, out var existing))
                return existing;

            var clrParam = Expression.Parameter(ClrType(node.getType()), node.name);
            _scope[node] = clrParam;
            return clrParam;
        }

        /// <summary>
        /// Returns the <see cref="LabelTarget"/> for the given name,
        /// creating one if it has not been seen before.
        /// </summary>
        LabelTarget GetOrCreateLabel(string name)
        {
            if (!_labels.TryGetValue(name, out var target))
            {
                target = Expression.Label(name);
                _labels[name] = target;
            }

            return target;
        }

        /// <summary>
        /// Converts a Java <c>java.lang.reflect.Type</c> to the corresponding CLR
        /// <see cref="System.Type"/> via IKVM's runtime helper.
        /// </summary>
        static System.Type ClrType(java.lang.reflect.Type javaType)
        {
            if (javaType is java.lang.Class cls)
                return ikvm.runtime.Util.getInstanceTypeFromClass(cls);

            // Parameterised / generic types: fall back to Object.
            return typeof(object);
        }

        /// <summary>
        /// Builds a nested if/else-if/else chain from a flat list produced by
        /// <see cref="ConditionalExpression"/> or <see cref="ConditionalStatement"/>.
        /// The list is encoded as [cond0, body0, cond1, body1, …, optionalElse].
        /// </summary>
        Expression BuildIfElseChain(IList<Node> exprs, int index)
        {
            if (index >= exprs.Count)
                return Expression.Empty();

            // Odd-length tail element is the else clause.
            if (index == exprs.Count - 1)
                return Translate(exprs[index]);

            var test = Translate(exprs[index]);
            var ifTrue = Translate(exprs[index + 1]);
            var ifFalse = BuildIfElseChain(exprs, index + 2);

            return ifFalse is DefaultExpression
                ? Expression.IfThen(test, ifTrue)
                : Expression.IfThenElse(test, ifTrue, ifFalse);
        }

        /// <summary>
        /// Resolves a <c>java.lang.reflect.Method</c> to its CLR <see cref="MethodInfo"/>.
        /// </summary>
        static MethodInfo ResolveMethod(java.lang.reflect.Method javaMethod)
        {
            var declaringType = ikvm.runtime.Util.getInstanceTypeFromClass(javaMethod.getDeclaringClass());
            var name = javaMethod.getName();
            var javaParams = javaMethod.getParameterTypes();

            var paramCount = javaParams.Length;
            var paramTypes = new System.Type[paramCount];
            for (int i = 0; i < paramCount; i++)
                paramTypes[i] = ikvm.runtime.Util.getInstanceTypeFromClass(javaParams[i]);

            var method = declaringType.GetMethod(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static, null, paramTypes, null)
                      ?? declaringType.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static, null, paramTypes, null);

            return method ?? throw new MissingMethodException(declaringType.FullName, name);
        }

        /// <summary>
        /// Finds the best-matching constructor on <paramref name="clrType"/> for the supplied
        /// argument expressions.
        /// </summary>
        static ConstructorInfo ResolveConstructor(System.Type clrType, Expression[] args)
        {
            foreach (var ctor in clrType.GetConstructors())
            {
                var ps = ctor.GetParameters();
                if (ps.Length != args.Length)
                    continue;

                bool match = true;
                for (int i = 0; i < ps.Length; i++)
                {
                    if (!ps[i].ParameterType.IsAssignableFrom(args[i].Type))
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                    return ctor;
            }

            throw new MissingMethodException(clrType.FullName, ".ctor");
        }



        /// <inheritdoc/>
        public override object visit(BinaryExpression node)
        {
            var left = Translate(node.expression0);
            var right = Translate(node.expression1);
            var nt = node.nodeType;

            if (nt == ExpressionType.Add) return LinqExpr.Add(left, right);
            if (nt == ExpressionType.AddChecked) return LinqExpr.AddChecked(left, right);
            if (nt == ExpressionType.Subtract) return LinqExpr.Subtract(left, right);
            if (nt == ExpressionType.SubtractChecked) return LinqExpr.SubtractChecked(left, right);
            if (nt == ExpressionType.Multiply) return LinqExpr.Multiply(left, right);
            if (nt == ExpressionType.MultiplyChecked) return LinqExpr.MultiplyChecked(left, right);
            if (nt == ExpressionType.Divide) return LinqExpr.Divide(left, right);
            if (nt == ExpressionType.Modulo) return LinqExpr.Modulo(left, right);
            if (nt == ExpressionType.Power) return LinqExpr.Power(left, right);
            if (nt == ExpressionType.And) return LinqExpr.And(left, right);
            if (nt == ExpressionType.Or) return LinqExpr.Or(left, right);
            if (nt == ExpressionType.ExclusiveOr) return LinqExpr.ExclusiveOr(left, right);
            if (nt == ExpressionType.LeftShift) return LinqExpr.LeftShift(left, right);
            if (nt == ExpressionType.RightShift) return LinqExpr.RightShift(left, right);
            if (nt == ExpressionType.AndAlso) return LinqExpr.AndAlso(left, right);
            if (nt == ExpressionType.OrElse) return LinqExpr.OrElse(left, right);
            if (nt == ExpressionType.Equal) return LinqExpr.Equal(left, right);
            if (nt == ExpressionType.NotEqual) return LinqExpr.NotEqual(left, right);
            if (nt == ExpressionType.LessThan) return LinqExpr.LessThan(left, right);
            if (nt == ExpressionType.LessThanOrEqual) return LinqExpr.LessThanOrEqual(left, right);
            if (nt == ExpressionType.GreaterThan) return LinqExpr.GreaterThan(left, right);
            if (nt == ExpressionType.GreaterThanOrEqual) return LinqExpr.GreaterThanOrEqual(left, right);
            if (nt == ExpressionType.Coalesce) return LinqExpr.Coalesce(left, right);
            if (nt == ExpressionType.ArrayIndex) return LinqExpr.ArrayIndex(left, right);
            if (nt == ExpressionType.Assign) return LinqExpr.Assign(left, right);
            if (nt == ExpressionType.AddAssign) return LinqExpr.AddAssign(left, right);
            if (nt == ExpressionType.AddAssignChecked) return LinqExpr.AddAssignChecked(left, right);
            if (nt == ExpressionType.SubtractAssign) return LinqExpr.SubtractAssign(left, right);
            if (nt == ExpressionType.SubtractAssignChecked) return LinqExpr.SubtractAssignChecked(left, right);
            if (nt == ExpressionType.MultiplyAssign) return LinqExpr.MultiplyAssign(left, right);
            if (nt == ExpressionType.MultiplyAssignChecked) return LinqExpr.MultiplyAssignChecked(left, right);
            if (nt == ExpressionType.DivideAssign) return LinqExpr.DivideAssign(left, right);
            if (nt == ExpressionType.ModuloAssign) return LinqExpr.ModuloAssign(left, right);
            if (nt == ExpressionType.AndAssign) return LinqExpr.AndAssign(left, right);
            if (nt == ExpressionType.OrAssign) return LinqExpr.OrAssign(left, right);
            if (nt == ExpressionType.ExclusiveOrAssign) return LinqExpr.ExclusiveOrAssign(left, right);
            if (nt == ExpressionType.LeftShiftAssign) return LinqExpr.LeftShiftAssign(left, right);
            if (nt == ExpressionType.RightShiftAssign) return LinqExpr.RightShiftAssign(left, right);

            throw new NotSupportedException($"Unsupported binary ExpressionType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(UnaryExpression node)
        {
            var operand = Translate(node.expression);
            var resultType = ClrType(node.getType());
            var nt = node.nodeType;

            if (nt == ExpressionType.Negate) return LinqExpr.Negate(operand);
            if (nt == ExpressionType.NegateChecked) return LinqExpr.NegateChecked(operand);
            if (nt == ExpressionType.Not) return LinqExpr.Not(operand);
            if (nt == ExpressionType.OnesComplement) return LinqExpr.OnesComplement(operand);
            if (nt == ExpressionType.UnaryPlus) return LinqExpr.UnaryPlus(operand);
            if (nt == ExpressionType.Convert) return LinqExpr.Convert(operand, resultType);
            if (nt == ExpressionType.ConvertChecked) return LinqExpr.ConvertChecked(operand, resultType);
            if (nt == ExpressionType.TypeAs) return LinqExpr.TypeAs(operand, resultType);
            if (nt == ExpressionType.ArrayLength) return LinqExpr.ArrayLength(operand);
            if (nt == ExpressionType.Quote) return LinqExpr.Quote(operand);
            if (nt == ExpressionType.Throw) return LinqExpr.Throw(operand);
            if (nt == ExpressionType.Increment) return LinqExpr.Increment(operand);
            if (nt == ExpressionType.Decrement) return LinqExpr.Decrement(operand);
            if (nt == ExpressionType.PreIncrementAssign) return LinqExpr.PreIncrementAssign(operand);
            if (nt == ExpressionType.PreDecrementAssign) return LinqExpr.PreDecrementAssign(operand);
            if (nt == ExpressionType.PostIncrementAssign) return LinqExpr.PostIncrementAssign(operand);
            if (nt == ExpressionType.PostDecrementAssign) return LinqExpr.PostDecrementAssign(operand);
            if (nt == ExpressionType.IsTrue) return LinqExpr.IsTrue(operand);
            if (nt == ExpressionType.IsFalse) return LinqExpr.IsFalse(operand);

            throw new NotSupportedException($"Unsupported unary ExpressionType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(TernaryExpression node)
        {
            // Ternary maps to Expression.Condition(test, ifTrue, ifFalse).
            var test = Translate(node.expression0);
            var ifTrue = Translate(node.expression1);
            var ifFalse = Translate(node.expression2);
            return LinqExpr.Condition(test, ifTrue, ifFalse);
        }

            }

        }

            var body = new List<LinqExpr>(stmts.Count);

            foreach (var s in stmts)
            {
                var translated = Translate(s);

                // Hoist variable declarations into the block's variable list.
                if (s is DeclarationStatement ds)
                {
                    var p = GetOrCreateParameter(ds.parameter);
                    vars.Add(p);
                    if (ds.initializer is not null)
                        body.Add(LinqExpr.Assign(p, Translate(ds.initializer)));
                }
                else
                {
                    body.Add(translated);
                }
            }

            return LinqExpr.Block(vars, body);
        }

        /// <inheritdoc/>
        public override object visit(DeclarationStatement node)
        {
            // When encountered outside a BlockStatement we just translate the initialiser
            // (the variable was already hoisted by the block visitor above).
            if (node.initializer is not null)
                return Translate(node.initializer);

            return LinqExpr.Empty();
        }

        /// <inheritdoc/>
        public override object visit(ParameterExpression node)
        {
            return GetOrCreateParameter(node);
        }

        /// <inheritdoc/>
        public override object visit(MemberExpression node)
        {
            if (node.expression is null)
            {
                // Static member access.
                var clrType = ClrType(node.getType());
                var fieldInf = clrType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Static)
                            ?? clrType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Static);
                if (fieldInf is not null)
                    return LinqExpr.Field(null, fieldInf);

                var propInf = clrType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Static);
                if (propInf is not null)
                    return LinqExpr.Property(null, propInf);

                throw new NotSupportedException($"Cannot resolve static member '{node.field.getName()}' on '{clrType}'.");
            }

            var instance = Translate(node.expression);
            var instType = instance.Type;

            var iField = instType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Instance)
                      ?? instType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Instance);
            if (iField is not null)
                return LinqExpr.Field(instance, iField);

            var iProp = instType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Instance);
            if (iProp is not null)
                return LinqExpr.Property(instance, iProp);

            throw new NotSupportedException($"Cannot resolve instance member '{node.field.getName()}' on '{instType}'.");
        }

        /// <inheritdoc/>
        public override object visit(MethodCallExpression node)
        {
            var argList = node.expressions.AsList<Node>();
            var args = new LinqExpr[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            if (node.targetExpression is null)
            {
                // Static call.
                var clrMethod = ResolveMethod(node.method);
                return LinqExpr.Call(clrMethod, args);
            }

            var target = Translate(node.targetExpression);
            var method = ResolveMethod(node.method);
            return LinqExpr.Call(target, method, args);
        }

        /// <inheritdoc/>
        public override object visit(NewExpression node)
        {
            var clrType = ClrType(node.getType());
            var argList = node.arguments.AsList<Node>();
            var args = new LinqExpr[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            // Find best matching constructor.
            var ctor = ResolveConstructor(clrType, args);
            return LinqExpr.New(ctor, args);
        }

        /// <inheritdoc/>
        public override object visit(NewArrayExpression node)
        {
            var elemType = ClrType(node.getType());

            if (node.bound is not null)
            {
                // new T[n]
                return LinqExpr.NewArrayBounds(elemType, [Translate(node.bound)]);
            }

            // new T[] { e0, e1, … }
            var exprList = node.expressions.AsList<Node>();
            var inits = new LinqExpr[exprList.Count];
            for (int i = 0; i < exprList.Count; i++)
                inits[i] = Translate(exprList[i]);

            return LinqExpr.NewArrayInit(elemType, inits);
        }

        /// <inheritdoc/>
        public override object visit(IndexExpression node)
        {
            var array = Translate(node.array);
            var idxList = node.indexExpressions.AsList<Node>();

            if (idxList.Count == 1)
                return LinqExpr.ArrayIndex(array, Translate(idxList[0]));

            var indices = new LinqExpr[idxList.Count];
            for (int i = 0; i < idxList.Count; i++)
                indices[i] = Translate(idxList[i]);

            return LinqExpr.ArrayAccess(array, indices);
        }

        /// <inheritdoc/>
        public override object visit(TypeBinaryExpression node)
        {
            var operand = Translate(node.expression);
            var nt = node.nodeType;
            var clrType = ClrType(node.getType());

            if (nt == ExpressionType.TypeIs) return LinqExpr.TypeIs(operand, clrType);
            if (nt == ExpressionType.TypeAs) return LinqExpr.TypeAs(operand, clrType);

            throw new NotSupportedException($"Unsupported TypeBinaryExpression nodeType: {nt}");
        }

        /// <inheritdoc/>
        public override object visit(FunctionExpression node)
        {
            return TranslateLambda(node);
        }

        /// <inheritdoc/>
        public override object visit(GotoStatement node)
        {
            var kind = node.kind;
            // GotoStatement uses a LabelTarget (which holds a name) not a bare string.
            var labelName = node.labelTarget?.name ?? string.Empty;
            var label = GetOrCreateLabel(labelName);

            if (kind == GotoExpressionKind.Return)
            {
                return node.expression is not null
                    ? LinqExpr.Return(label, Translate(node.expression))
                    : LinqExpr.Return(label);
            }

            if (kind == GotoExpressionKind.Break)
                return LinqExpr.Break(label);

            if (kind == GotoExpressionKind.Continue)
                return LinqExpr.Continue(label);

            // GotoExpressionKind.Goto
            return node.expression is not null
                ? LinqExpr.Goto(label, Translate(node.expression))
                : LinqExpr.Goto(label);
        }

        /// <inheritdoc/>
        public override object visit(LabelStatement node)
        {
            // LabelStatement carries its label identity in its nodeType; use a hash of the node
            // as the label key so that matching GotoStatements can resolve the same target.
            var labelKey = node.GetHashCode().ToString();
            var target = GetOrCreateLabel(labelKey);
            var defaultVal = Translate(node.defaultValue);
            return LinqExpr.Label(target, defaultVal);
        }

        /// <inheritdoc/>
        public override object visit(ForStatement node)
        {
            // Calcite's ForStatement maps to:
            //   Block(declarations,
            //     Loop(
            //       Block(
            //         IfThen(!condition, Break),
            //         body,
            //         post)))
            var breakLabel = LinqExpr.Label("for_break");
            var continueLabel = LinqExpr.Label("for_continue");

            var declList = node.declarations.AsList<DeclarationStatement>();
            var declVars = new List<System.Linq.Expressions.ParameterExpression>(declList.Count);
            var declInits = new List<LinqExpr>(declList.Count);

            foreach (var d in declList)
            {
                var p = GetOrCreateParameter(d.parameter);
                declVars.Add(p);
                if (d.initializer is not null)
                    declInits.Add(LinqExpr.Assign(p, Translate(d.initializer)));
            }

            var loopBodyStmts = new List<LinqExpr>();

            if (node.condition is not null)
                loopBodyStmts.Add(LinqExpr.IfThen(LinqExpr.Not(Translate(node.condition)), LinqExpr.Break(breakLabel)));

            loopBodyStmts.Add(Translate(node.body));

            if (node.post is not null)
                loopBodyStmts.Add(Translate(node.post));

            LinqExpr loop = LinqExpr.Loop(LinqExpr.Block(loopBodyStmts), breakLabel, continueLabel);

            if (declVars.Count > 0)
            {
                var outerStmts = new LinqExpr[declInits.Count + 1];
                declInits.CopyTo(outerStmts);
                outerStmts[declInits.Count] = loop;
                return LinqExpr.Block(declVars, outerStmts);
            }

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(ForEachStatement node)
        {
            // foreach (T item in iterable) body
            // Compiles to a while loop over GetEnumerator() / MoveNext() / Current.
            var iterableExpr = Translate(node.iterable);
            var itemVar = GetOrCreateParameter(node.parameter);
            var breakLabel = LinqExpr.Label("foreach_break");
            var continueLabel = LinqExpr.Label("foreach_continue");

            var enumeratorType = typeof(System.Collections.IEnumerator);
            var getEnumerator = iterableExpr.Type.GetMethod("GetEnumerator")
                              ?? typeof(System.Collections.IEnumerable).GetMethod("GetEnumerator")!;
            var moveNext = enumeratorType.GetMethod("MoveNext")!;
            var currentProp = enumeratorType.GetProperty("Current")!;

            var enumeratorVar = LinqExpr.Variable(enumeratorType, "enumerator");

            var loop = LinqExpr.Block(
                [enumeratorVar, itemVar],
                LinqExpr.Assign(enumeratorVar, LinqExpr.Convert(LinqExpr.Call(iterableExpr, getEnumerator), enumeratorType)),
                LinqExpr.Loop(
                    LinqExpr.Block(
                        LinqExpr.IfThen(LinqExpr.Not(LinqExpr.Call(enumeratorVar, moveNext)), LinqExpr.Break(breakLabel)),
                        LinqExpr.Assign(itemVar, LinqExpr.Convert(LinqExpr.Property(enumeratorVar, currentProp), itemVar.Type)),
                        Translate(node.body)),
                    breakLabel,
                    continueLabel));

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(WhileStatement node)
        {
            var breakLabel = LinqExpr.Label("while_break");
            var continueLabel = LinqExpr.Label("while_continue");

            var loop = LinqExpr.Loop(
                LinqExpr.Block(
                    LinqExpr.IfThen(LinqExpr.Not(Translate(node.condition)), LinqExpr.Break(breakLabel)),
                    Translate(node.body)),
                breakLabel,
                continueLabel);

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(TryStatement node)
        {
            var body = Translate(node.body);

            var catchBlocks = new List<System.Linq.Expressions.CatchBlock>();
            foreach (var cb in node.catchBlocks.AsList<CatchBlock>())
            {
                var exVar = GetOrCreateParameter(cb.parameter);
                var cbBody = Translate(cb.body);
                catchBlocks.Add(LinqExpr.Catch(exVar, cbBody));
            }

            if (node.fynally is not null)
            {
                return catchBlocks.Count > 0
                    ? LinqExpr.TryCatchFinally(body, Translate(node.fynally), [.. catchBlocks])
                    : LinqExpr.TryFinally(body, Translate(node.fynally));
            }

            return LinqExpr.TryCatch(body, [.. catchBlocks]);
        }

        /// <inheritdoc/>
        public override object visit(ThrowStatement node)
        {
            return LinqExpr.Throw(Translate(node.expression));
        }

        /// <inheritdoc/>
        public override object visit(DefaultExpression node)
        {
            return LinqExpr.Default(ClrType(node.getType()));
        }

        // ─────────────────────────────────────────────────────────────────────
        // Helpers
        // ─────────────────────────────────────────────────────────────────────

        /// <summary>
        /// Returns the CLR <see cref="System.Linq.Expressions.ParameterExpression"/> for the
        /// given linq4j parameter, creating one if it has not been seen before.
        /// </summary>
        internal System.Linq.Expressions.ParameterExpression GetOrCreateParameter(ParameterExpression node)
        {
            if (_scope.TryGetValue(node, out var existing))
                return existing;

            var clrParam = LinqExpr.Parameter(ClrType(node.getType()), node.name);
            _scope[node] = clrParam;
            return clrParam;
        }

        /// <summary>
        /// Returns the <see cref="System.Linq.Expressions.LabelTarget"/> for the given name,
        /// creating one if it has not been seen before.
        /// </summary>
        System.Linq.Expressions.LabelTarget GetOrCreateLabel(string name)
        {
            if (!_labels.TryGetValue(name, out var target))
            {
                target = LinqExpr.Label(name);
                _labels[name] = target;
            }

            return target;
        }

        /// <summary>
        /// Converts a Java <c>java.lang.reflect.Type</c> to the corresponding CLR
        /// <see cref="System.Type"/> via IKVM's runtime helper.
        /// </summary>
        static System.Type ClrType(java.lang.reflect.Type javaType)
        {
            if (javaType is java.lang.Class cls)
                return ikvm.runtime.Util.getInstanceTypeFromClass(cls);

            // Parameterised / generic types: fall back to Object.
            return typeof(object);
        }

        /// <summary>
        /// Builds a nested if/else-if/else chain from a flat list produced by
        /// <see cref="ConditionalExpression"/> or <see cref="ConditionalStatement"/>.
        /// The list is encoded as [cond0, body0, cond1, body1, …, optionalElse].
        /// </summary>
        LinqExpr BuildIfElseChain(IList<Node> exprs, int index)
        {
            if (index >= exprs.Count)
                return LinqExpr.Empty();

            // Odd-length tail element is the else clause.
            if (index == exprs.Count - 1)
                return Translate(exprs[index]);

            var test = Translate(exprs[index]);
            var ifTrue = Translate(exprs[index + 1]);
            var ifFalse = BuildIfElseChain(exprs, index + 2);

            return ifFalse is System.Linq.Expressions.DefaultExpression
                ? LinqExpr.IfThen(test, ifTrue)
                : LinqExpr.IfThenElse(test, ifTrue, ifFalse);
        }

        /// <summary>
        /// Resolves a <c>java.lang.reflect.Method</c> to its CLR <see cref="MethodInfo"/>.
        /// </summary>
        static MethodInfo ResolveMethod(java.lang.reflect.Method javaMethod)
        {
            var declaringType = ikvm.runtime.Util.getInstanceTypeFromClass(javaMethod.getDeclaringClass());
            var name = javaMethod.getName();
            var javaParams = javaMethod.getParameterTypes();

            var paramCount = javaParams.Length;
            var paramTypes = new System.Type[paramCount];
            for (int i = 0; i < paramCount; i++)
                paramTypes[i] = ikvm.runtime.Util.getInstanceTypeFromClass(javaParams[i]);

            var method = declaringType.GetMethod(name, BindingFlags.Public | BindingFlags.Instance | BindingFlags.Static, null, paramTypes, null)
                      ?? declaringType.GetMethod(name, BindingFlags.NonPublic | BindingFlags.Instance | BindingFlags.Static, null, paramTypes, null);

            return method ?? throw new MissingMethodException(declaringType.FullName, name);
        }

        /// <summary>
        /// Finds the best-matching constructor on <paramref name="clrType"/> for the supplied
        /// argument expressions.
        /// </summary>
        static ConstructorInfo ResolveConstructor(System.Type clrType, LinqExpr[] args)
        {
            foreach (var ctor in clrType.GetConstructors())
            {
                var ps = ctor.GetParameters();
                if (ps.Length != args.Length)
                    continue;

                bool match = true;
                for (int i = 0; i < ps.Length; i++)
                {
                    if (!ps[i].ParameterType.IsAssignableFrom(args[i].Type))
                    {
                        match = false;
                        break;
                    }
                }

                if (match)
                    return ctor;
            }

            throw new MissingMethodException(clrType.FullName, ".ctor");
        }

    }

}
