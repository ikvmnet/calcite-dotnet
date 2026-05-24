using System;
using System.Collections.Generic;
using System.Reflection;
using System.Reflection.Emit;

using java.util;

using org.apache.calcite.linq4j.tree;

using LightCatchBlock = FastExpressionCompiler.LightExpression.CatchBlock;
using LightCompiler = FastExpressionCompiler.LightExpression.ExpressionCompiler;
using LightExpression = FastExpressionCompiler.LightExpression.Expression;
using LightLabelTarget = FastExpressionCompiler.LightExpression.LabelTarget;
using LightLambda = FastExpressionCompiler.LightExpression.LambdaExpression;
using LightParameter = FastExpressionCompiler.LightExpression.ParameterExpression;

namespace Apache.Calcite.Linq4j
{

    /// <summary>
    /// Translates a linq4j expression tree into an equivalent CLR <see cref="FastExpressionCompiler.LightExpression.Expression"/> tree,
    /// and optionally emits CLR types from <see cref="ClassDeclaration"/> nodes via <see cref="System.Reflection.Emit"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// A single <see cref="Linq4jExpressionTranslator"/> instance is <b>not</b> thread-safe.  Create a new instance for each independent translation.
    /// </para>
    /// </remarks>
    public class Linq4jExpressionTranslator : VisitorImpl
    {

        readonly ModuleBuilder? _moduleBuilder;

        Dictionary<ParameterExpression, LightParameter>? _scope;
        Dictionary<string, LightLabelTarget>? _labels;

        // ── Cached reflection members for well-known types ────────────────────────────────────────────────────────────
        static readonly MethodInfo s_iterableIterator = typeof(java.lang.Iterable).GetMethod(nameof(java.lang.Iterable.iterator))!;
        static readonly MethodInfo s_iteratorHasNext = typeof(java.util.Iterator).GetMethod(nameof(java.util.Iterator.hasNext))!;
        static readonly MethodInfo s_iteratorNext = typeof(java.util.Iterator).GetMethod(nameof(java.util.Iterator.next))!;

        /// <summary>
        /// Initialises a translator that can also emit CLR types from <see cref="ClassDeclaration"/> nodes.
        /// </summary>
        /// <param name="moduleBuilder">The <see cref="ModuleBuilder"/> into which emitted types are defined.</param>
        public Linq4jExpressionTranslator(ModuleBuilder moduleBuilder)
        {
            _moduleBuilder = moduleBuilder;
        }

        #region Public entry points

        /// <summary>
        /// Translates a linq4j <see cref="FunctionExpression"/> to a CLR <see cref="LightLambda"/>.
        /// Opens a fresh scope for the duration of the translation and clears it on exit.
        /// </summary>
        public LightLambda TranslateLambda(FunctionExpression node)
        {
            ArgumentNullException.ThrowIfNull(node);

            ResetScope();
            try
            {
                var paramList = node.parameterList.AsList<ParameterExpression>();
                var parameters = new LightParameter[paramList.Count];
                for (int i = 0; i < paramList.Count; i++)
                    parameters[i] = GetOrCreateParameter(paramList[i]);

                LightExpression body;
                if (node.body is not null)
                {
                    body = Translate(node.body);
                }
                else if (node.function is not null)
                {
                    body = LightExpression.Constant(node.function);
                }
                else
                {
                    throw new NotSupportedException("FunctionExpression has neither a body nor a function.");
                }

                return LightExpression.Lambda(body, parameters);
            }
            finally
            {
                ClearScope();
            }
        }

        /// <summary>
        /// Translates any linq4j <see cref="Node"/> to a CLR <see cref="LightExpression"/> by dispatching through <c>node.accept(this)</c>.
        /// </summary>
        public LightExpression Translate(Node node)
        {
            ArgumentNullException.ThrowIfNull(node);
            return (LightExpression)node.accept(this);
        }

        #endregion

        #region VisitorImpl overrides

        /// <inheritdoc/>
        public override object visit(ConstantExpression node)
        {
            if (node.value is null)
                return LightExpression.Constant(null, ClrType(node.getType()));

            return LightExpression.Constant(node.value, ClrType(node.getType()));
        }

        /// <inheritdoc/>
        public override object visit(BinaryExpression node)
        {
            var left = Translate(node.expression0);
            var right = Translate(node.expression1);
            var nt = node.nodeType;

            return (ExpressionType.__Enum)nt.ordinal() switch
            {
                ExpressionType.__Enum.Add => LightExpression.Add(left, right),
                ExpressionType.__Enum.AddChecked => LightExpression.AddChecked(left, right),
                ExpressionType.__Enum.Subtract => LightExpression.Subtract(left, right),
                ExpressionType.__Enum.SubtractChecked => LightExpression.SubtractChecked(left, right),
                ExpressionType.__Enum.Multiply => LightExpression.Multiply(left, right),
                ExpressionType.__Enum.MultiplyChecked => LightExpression.MultiplyChecked(left, right),
                ExpressionType.__Enum.Divide => LightExpression.Divide(left, right),
                ExpressionType.__Enum.Modulo => LightExpression.Modulo(left, right),
                ExpressionType.__Enum.Power => LightExpression.Power(left, right),
                ExpressionType.__Enum.And => LightExpression.And(left, right),
                ExpressionType.__Enum.Or => LightExpression.Or(left, right),
                ExpressionType.__Enum.ExclusiveOr => LightExpression.ExclusiveOr(left, right),
                ExpressionType.__Enum.LeftShift => LightExpression.LeftShift(left, right),
                ExpressionType.__Enum.RightShift => LightExpression.RightShift(left, right),
                ExpressionType.__Enum.AndAlso => LightExpression.AndAlso(left, right),
                ExpressionType.__Enum.OrElse => LightExpression.OrElse(left, right),
                ExpressionType.__Enum.Equal => LightExpression.Equal(left, right),
                ExpressionType.__Enum.NotEqual => LightExpression.NotEqual(left, right),
                ExpressionType.__Enum.LessThan => LightExpression.LessThan(left, right),
                ExpressionType.__Enum.LessThanOrEqual => LightExpression.LessThanOrEqual(left, right),
                ExpressionType.__Enum.GreaterThan => LightExpression.GreaterThan(left, right),
                ExpressionType.__Enum.GreaterThanOrEqual => LightExpression.GreaterThanOrEqual(left, right),
                ExpressionType.__Enum.Coalesce => LightExpression.Coalesce(left, right),
                ExpressionType.__Enum.ArrayIndex => LightExpression.ArrayIndex(left, right),
                ExpressionType.__Enum.Assign => LightExpression.Assign(left, right),
                ExpressionType.__Enum.AddAssign => LightExpression.AddAssign(left, right),
                ExpressionType.__Enum.AddAssignChecked => LightExpression.AddAssignChecked(left, right),
                ExpressionType.__Enum.SubtractAssign => LightExpression.SubtractAssign(left, right),
                ExpressionType.__Enum.SubtractAssignChecked => LightExpression.SubtractAssignChecked(left, right),
                ExpressionType.__Enum.MultiplyAssign => LightExpression.MultiplyAssign(left, right),
                ExpressionType.__Enum.MultiplyAssignChecked => LightExpression.MultiplyAssignChecked(left, right),
                ExpressionType.__Enum.DivideAssign => LightExpression.DivideAssign(left, right),
                ExpressionType.__Enum.ModuloAssign => LightExpression.ModuloAssign(left, right),
                ExpressionType.__Enum.AndAssign => LightExpression.AndAssign(left, right),
                ExpressionType.__Enum.OrAssign => LightExpression.OrAssign(left, right),
                ExpressionType.__Enum.ExclusiveOrAssign => LightExpression.ExclusiveOrAssign(left, right),
                ExpressionType.__Enum.LeftShiftAssign => LightExpression.LeftShiftAssign(left, right),
                ExpressionType.__Enum.RightShiftAssign => LightExpression.RightShiftAssign(left, right),
                _ => throw new NotSupportedException($"Unsupported binary ExpressionType: {nt}"),
            };
        }

        /// <inheritdoc/>
        public override object visit(UnaryExpression node)
        {
            var operand = Translate(node.expression);
            var resultType = ClrType(node.getType());
            var nt = node.nodeType;

            return (ExpressionType.__Enum)nt.ordinal() switch
            {
                ExpressionType.__Enum.Negate => LightExpression.Negate(operand),
                ExpressionType.__Enum.NegateChecked => LightExpression.NegateChecked(operand),
                ExpressionType.__Enum.Not => LightExpression.Not(operand),
                ExpressionType.__Enum.OnesComplement => LightExpression.OnesComplement(operand),
                ExpressionType.__Enum.UnaryPlus => LightExpression.UnaryPlus(operand),
                ExpressionType.__Enum.Convert => LightExpression.Convert(operand, resultType),
                ExpressionType.__Enum.ConvertChecked => LightExpression.ConvertChecked(operand, resultType),
                ExpressionType.__Enum.TypeAs => LightExpression.TypeAs(operand, resultType),
                ExpressionType.__Enum.ArrayLength => LightExpression.ArrayLength(operand),
                ExpressionType.__Enum.Quote => LightExpression.Quote(operand),
                ExpressionType.__Enum.Throw => LightExpression.Throw(operand),
                ExpressionType.__Enum.Increment => LightExpression.Increment(operand),
                ExpressionType.__Enum.Decrement => LightExpression.Decrement(operand),
                ExpressionType.__Enum.PreIncrementAssign => LightExpression.PreIncrementAssign(operand),
                ExpressionType.__Enum.PreDecrementAssign => LightExpression.PreDecrementAssign(operand),
                ExpressionType.__Enum.PostIncrementAssign => LightExpression.PostIncrementAssign(operand),
                ExpressionType.__Enum.PostDecrementAssign => LightExpression.PostDecrementAssign(operand),
                ExpressionType.__Enum.IsTrue => LightExpression.IsTrue(operand),
                ExpressionType.__Enum.IsFalse => LightExpression.IsFalse(operand),
                _ => throw new NotSupportedException($"Unsupported unary ExpressionType: {nt}"),
            };
        }

        /// <inheritdoc/>
        public override object visit(TernaryExpression node)
        {
            var test = Translate(node.expression0);
            var ifTrue = Translate(node.expression1);
            var ifFalse = Translate(node.expression2);
            return LightExpression.Condition(test, ifTrue, ifFalse);
        }

        /// <summary>
        /// Translates a linq4j <see cref="ConditionalExpression"/> (an if/else-if/else chain encoded as a flat list of [condition, body, condition, body, …, optional-else])
        /// into a nested CLR conditional expression.
        /// </summary>
        public override object visit(ConditionalExpression node)
        {
            // HACK: ConditionalExpression.expressionList is package-private, so we must reach it via reflection.
            // This should be fixed upstream — see https://issues.apache.org/jira/browse/CALCITE-7548
            // (ConditionalExpression.expressionList should be public, matching ConditionalStatement.expressionList).
            var listField = typeof(ConditionalExpression).GetField("expressionList", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
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
                return LightExpression.Empty();

            var vars = new List<LightParameter>();
            var body = new List<LightExpression>(stmts.Count);

            foreach (var s in stmts)
            {
                if (s is DeclarationStatement ds)
                {
                    var p = GetOrCreateParameter(ds.parameter);
                    vars.Add(p);
                    if (ds.initializer is not null)
                        body.Add(LightExpression.Assign(p, Translate(ds.initializer)));
                }
                else
                {
                    body.Add(Translate(s));
                }
            }

            return LightExpression.Block(vars, body);
        }

        /// <inheritdoc/>
        public override object visit(DeclarationStatement node)
        {
            if (node.initializer is not null)
                return Translate(node.initializer);

            return LightExpression.Empty();
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
                var clrType = ClrType(node.getType());
                var fieldInf = clrType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Static)
                            ?? clrType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Static);
                if (fieldInf is not null)
                    return LightExpression.Field(null, fieldInf);

                var propInf = clrType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Static);
                if (propInf is not null)
                    return LightExpression.Property(null, propInf);

                throw new NotSupportedException($"Cannot resolve static member '{node.field.getName()}' on '{clrType}'.");
            }

            var instance = Translate(node.expression);
            var instType = instance.Type;

            var iField = instType.GetField(node.field.getName(), BindingFlags.Public | BindingFlags.Instance)
                      ?? instType.GetField(node.field.getName(), BindingFlags.NonPublic | BindingFlags.Instance);
            if (iField is not null)
                return LightExpression.Field(instance, iField);

            var iProp = instType.GetProperty(node.field.getName(), BindingFlags.Public | BindingFlags.Instance);
            if (iProp is not null)
                return LightExpression.Property(instance, iProp);

            throw new NotSupportedException($"Cannot resolve instance member '{node.field.getName()}' on '{instType}'.");
        }

        /// <inheritdoc/>
        public override object visit(MethodCallExpression node)
        {
            var argList = node.expressions.AsList<Node>();
            var args = new LightExpression[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            if (node.targetExpression is null)
            {
                var clrMethod = ResolveMethod(node.method);
                return LightExpression.Call(clrMethod, args);
            }

            var target = Translate(node.targetExpression);
            var method = ResolveMethod(node.method);
            return LightExpression.Call(target, method, args);
        }

        /// <inheritdoc/>
        public override object visit(NewExpression node)
        {
            var clrType = ClrType(node.getType());
            var argList = node.arguments.AsList<Node>();
            var args = new LightExpression[argList.Count];
            for (int i = 0; i < argList.Count; i++)
                args[i] = Translate(argList[i]);

            var ctor = ResolveConstructor(clrType, args);
            return LightExpression.New(ctor, args);
        }

        /// <inheritdoc/>
        public override object visit(NewArrayExpression node)
        {
            var elemType = ClrType(node.getType());

            if (node.bound is not null)
                return LightExpression.NewArrayBounds(elemType, [Translate(node.bound)]);

            var exprList = node.expressions.AsList<Node>();
            var inits = new LightExpression[exprList.Count];
            for (int i = 0; i < exprList.Count; i++)
                inits[i] = Translate(exprList[i]);

            return LightExpression.NewArrayInit(elemType, inits);
        }

        /// <inheritdoc/>
        public override object visit(IndexExpression node)
        {
            var array = Translate(node.array);
            var idxList = node.indexExpressions.AsList<Node>();

            if (idxList.Count == 1)
                return LightExpression.ArrayIndex(array, Translate(idxList[0]));

            var indices = new LightExpression[idxList.Count];
            for (int i = 0; i < idxList.Count; i++)
                indices[i] = Translate(idxList[i]);

            return LightExpression.ArrayAccess(array, indices);
        }

        /// <inheritdoc/>
        public override object visit(TypeBinaryExpression node)
        {
            var operand = Translate(node.expression);
            var nt = node.nodeType;
            var clrType = ClrType(node.getType());

            if (nt == ExpressionType.TypeIs)
                return LightExpression.TypeIs(operand, clrType);
            if (nt == ExpressionType.TypeAs)
                return LightExpression.TypeAs(operand, clrType);

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
                    ? LightExpression.Return(label, Translate(node.expression))
                    : LightExpression.Return(label);
            }

            if (kind == GotoExpressionKind.Break)
                return LightExpression.Break(label);

            if (kind == GotoExpressionKind.Continue)
                return LightExpression.Continue(label);

            return node.expression is not null
                ? LightExpression.Goto(label, Translate(node.expression))
                : LightExpression.Goto(label);
        }

        /// <inheritdoc/>
        public override object visit(LabelStatement node)
        {
            var labelKey = node.GetHashCode().ToString();
            var target = GetOrCreateLabel(labelKey);
            var defaultVal = Translate(node.defaultValue);
            return LightExpression.Label(target, defaultVal);
        }

        /// <inheritdoc/>
        public override object visit(ForStatement node)
        {
            var breakLabel = LightExpression.Label("for_break");
            var continueLabel = LightExpression.Label("for_continue");

            var declList = node.declarations.AsList<DeclarationStatement>();
            var declVars = new List<LightParameter>(declList.Count);
            var declInits = new List<LightExpression>(declList.Count);

            foreach (var d in declList)
            {
                var p = GetOrCreateParameter(d.parameter);
                declVars.Add(p);
                if (d.initializer is not null)
                    declInits.Add(LightExpression.Assign(p, Translate(d.initializer)));
            }

            var loopBodyStmts = new List<LightExpression>();

            if (node.condition is not null)
                loopBodyStmts.Add(LightExpression.IfThen(LightExpression.Not(Translate(node.condition)), LightExpression.Break(breakLabel)));

            loopBodyStmts.Add(Translate(node.body));

            if (node.post is not null)
                loopBodyStmts.Add(Translate(node.post));

            LightExpression loop = LightExpression.Loop(LightExpression.Block(loopBodyStmts), breakLabel, continueLabel);

            if (declVars.Count > 0)
            {
                var outerStmts = new LightExpression[declInits.Count + 1];
                declInits.CopyTo(outerStmts);
                outerStmts[declInits.Count] = loop;
                return LightExpression.Block(declVars, outerStmts);
            }

            return loop;
        }

        /// <inheritdoc/>
        public override object visit(ForEachStatement node)
        {
            var iterableExpr = Translate(node.iterable);
            var itemVar = GetOrCreateParameter(node.parameter);
            var breakLabel = LightExpression.Label("foreach_break");
            var continueLabel = LightExpression.Label("foreach_continue");
            var iteratorVar = LightExpression.Variable(typeof(java.util.Iterator), "iterator");

            return LightExpression.Block(
                [iteratorVar, itemVar],
                LightExpression.Assign(iteratorVar, LightExpression.Convert(LightExpression.Call(iterableExpr, s_iterableIterator), typeof(java.util.Iterator))),
                LightExpression.Loop(
                    LightExpression.Block(
                        LightExpression.IfThen(LightExpression.Not(LightExpression.Call(iteratorVar, s_iteratorHasNext)), LightExpression.Break(breakLabel)),
                        LightExpression.Assign(itemVar, LightExpression.Convert(LightExpression.Call(iteratorVar, s_iteratorNext), itemVar.Type)),
                        Translate(node.body)),
                    breakLabel,
                    continueLabel));
        }

        /// <inheritdoc/>
        public override object visit(WhileStatement node)
        {
            var breakLabel = LightExpression.Label("while_break");
            var continueLabel = LightExpression.Label("while_continue");

            return LightExpression.Loop(
                LightExpression.Block(
                    LightExpression.IfThen(LightExpression.Not(Translate(node.condition)), LightExpression.Break(breakLabel)),
                    Translate(node.body)),
                breakLabel,
                continueLabel);
        }

        /// <inheritdoc/>
        public override object visit(TryStatement node)
        {
            var body = Translate(node.body);

            var catchBlocks = new List<LightCatchBlock>();
            foreach (var cb in node.catchBlocks.AsList<CatchBlock>())
            {
                var exVar = GetOrCreateParameter(cb.parameter);
                var cbBody = Translate(cb.body);
                catchBlocks.Add(LightExpression.Catch(exVar, cbBody));
            }

            if (node.fynally is not null)
            {
                return catchBlocks.Count > 0
                    ? LightExpression.TryCatchFinally(body, Translate(node.fynally), [.. catchBlocks])
                    : LightExpression.TryFinally(body, Translate(node.fynally));
            }

            return LightExpression.TryCatch(body, [.. catchBlocks]);
        }

        /// <inheritdoc/>
        public override object visit(ThrowStatement node)
        {
            return LightExpression.Throw(Translate(node.expression));
        }

        /// <inheritdoc/>
        public override object visit(DefaultExpression node)
        {
            return LightExpression.Default(ClrType(node.getType()));
        }

        #endregion

        #region Class emission

        /// <summary>
        /// Emits a CLR type from a linq4j <see cref="ClassDeclaration"/> into the <see cref="ModuleBuilder"/> that was supplied at construction time.
        /// </summary>
        /// <returns>The <see cref="TypeBuilder"/> for the newly defined type (not yet baked).</returns>
        /// <exception cref="InvalidOperationException">Thrown when no <see cref="ModuleBuilder"/> was provided to this instance.</exception>
        public override object visit(ClassDeclaration classDeclaration)
        {
            if (_moduleBuilder is null)
                throw new InvalidOperationException($"A {nameof(ModuleBuilder)} is required to emit a ClassDeclaration. Use the constructor overload that accepts one.");

            return EmitClass(classDeclaration, _moduleBuilder);
        }

        /// <summary>
        /// Emits a new CLR type into <paramref name="moduleBuilder"/> from the supplied <see cref="ClassDeclaration"/> and returns the finished <see cref="TypeBuilder"/>.
        /// </summary>
        /// <param name="classDeclaration">The linq4j class to emit.</param>
        /// <param name="moduleBuilder">The module into which the type is defined.</param>
        protected virtual TypeBuilder EmitClass(ClassDeclaration classDeclaration, ModuleBuilder moduleBuilder)
        {
            var members = classDeclaration.memberDeclarations.AsList<MemberDeclaration>();

            // ── Base type and interfaces ──────────────────────────────────────
            var baseType = typeof(object);
            if (classDeclaration.extended is java.lang.Class extClass)
                baseType = ClrType(extClass);

            Type[] interfaceTypes;
            if (classDeclaration.implemented is java.util.List ifaces)
            {
                var ifaceList = ifaces.AsList<java.lang.reflect.Type>();
                interfaceTypes = new Type[ifaceList.Count];
                for (int i = 0; i < ifaceList.Count; i++)
                    interfaceTypes[i] = ClrType(ifaceList[i]);
            }
            else
            {
                interfaceTypes = [];
            }

            var typeAttrs = JavaModifierToTypeAttributes(classDeclaration.modifier);
            var typeBuilder = moduleBuilder.DefineType(classDeclaration.name, typeAttrs, baseType, interfaceTypes);

            // ── Fields ───────────────────────────────────────────────────────
            var fieldInitializers = new List<(FieldBuilder Field, FieldDeclaration Decl)>();

            foreach (var member in members)
            {
                if (member is not FieldDeclaration fd)
                    continue;

                var fieldType = ClrType(fd.parameter.getType());
                var fieldAttrs = JavaModifierToFieldAttributes(fd.modifier);
                var fb = typeBuilder.DefineField(fd.parameter.name, fieldType, fieldAttrs);

                if (fd.initializer is not null)
                    fieldInitializers.Add((fb, fd));
            }

            // ── Methods ──────────────────────────────────────────────────────
            foreach (var member in members)
            {
                if (member is not MethodDeclaration md)
                    continue;

                EmitMethod(typeBuilder, md, interfaceTypes);
            }

            // ── Constructor ──────────────────────────────────────────────────
            EmitConstructor(typeBuilder, baseType, fieldInitializers);

            return typeBuilder;
        }

        /// <summary>
        /// Defines and compiles a single method from <paramref name="md"/> into <paramref name="typeBuilder"/>.
        /// </summary>
        /// <remarks>
        /// <see cref="ResetScope"/> is called before translation so that parameter scope is isolated to this method.  The compiled body is emitted directly into the
        /// <see cref="MethodBuilder"/>'s <see cref="ILGenerator"/> via <c>ExpressionCompiler.CompileFastToIL</c>.  If the method body is <see langword="null"/>
        /// a <c>default(T)</c> expression is used as a placeholder.
        /// </remarks>
        /// <param name="typeBuilder">The type being constructed.</param>
        /// <param name="md">The linq4j method declaration to emit.</param>
        /// <param name="interfaceTypes">Interfaces implemented by the type; used to wire up explicit overrides.</param>
        protected virtual void EmitMethod(TypeBuilder typeBuilder, MethodDeclaration md, Type[] interfaceTypes)
        {
            var returnType = ClrType(md.resultType);
            var paramList = md.parameters.AsList<ParameterExpression>();
            bool isStatic = (md.modifier & java.lang.reflect.Modifier.STATIC) != 0;

            ResetScope();
            try
            {
                var paramExprs = new LightParameter[paramList.Count];
                for (int i = 0; i < paramList.Count; i++)
                    paramExprs[i] = GetOrCreateParameter(paramList[i]);

                var paramTypes = new Type[paramList.Count];
                for (int i = 0; i < paramList.Count; i++)
                    paramTypes[i] = paramExprs[i].Type;

                var bodyExpr = md.body is not null
                    ? Translate(md.body)
                    : LightExpression.Default(returnType);

                // CompileFastToIL treats the first lambda parameter as 'this' for instance methods.
                LightParameter[] lambdaParams = isStatic
                    ? paramExprs
                    : [LightExpression.Parameter(typeBuilder, "this"), .. paramExprs];

                var lambda = LightExpression.Lambda(bodyExpr, lambdaParams);

                var methodAttrs = JavaModifierToMethodAttributes(md.modifier);
                var methodBuilder = typeBuilder.DefineMethod(md.name, methodAttrs, returnType, paramTypes);

                LightCompiler.CompileFastToIL(lambda, methodBuilder.GetILGenerator(), flags: default);

                foreach (var iface in interfaceTypes)
                {
                    var ifaceMethod = iface.GetMethod(md.name, paramTypes);
                    if (ifaceMethod is not null)
                        typeBuilder.DefineMethodOverride(methodBuilder, ifaceMethod);
                }
            }
            finally
            {
                ClearScope();
            }
        }

        /// <summary>
        /// Emits a public no-argument constructor into <paramref name="typeBuilder"/> that chains to the base-type constructor
        /// and initialises every field that carries an initializer expression.
        /// </summary>
        /// <remarks>
        /// Each field initializer is translated after a <see cref="ResetScope"/> call and compiled into a private static helper method, which the constructor calls
        /// before storing the result with <c>stfld</c>.  This avoids embedding arbitrary expression trees inside a single <see cref="ILGenerator"/> sequence.
        /// </remarks>
        /// <param name="typeBuilder">The type being constructed.</param>
        /// <param name="baseType">The CLR base type whose no-argument constructor is chained.</param>
        /// <param name="fieldInitializers">Fields that have initializer expressions, in declaration order.</param>
        protected virtual void EmitConstructor(TypeBuilder typeBuilder, Type baseType, List<(FieldBuilder Field, FieldDeclaration Decl)> fieldInitializers)
        {
            var noArgCtor = baseType.GetConstructor([]) ?? typeof(object).GetConstructor([])!;
            var ctorBuilder = typeBuilder.DefineConstructor(MethodAttributes.Public, CallingConventions.Standard, []);
            var ctorIl = ctorBuilder.GetILGenerator();
            ctorIl.Emit(OpCodes.Ldarg_0);
            ctorIl.Emit(OpCodes.Call, noArgCtor);

            foreach (var (fb, fd) in fieldInitializers)
            {
                ResetScope();
                try
                {
                    var thisParam = LightExpression.Parameter(typeBuilder, "this");
                    GetOrCreateParameter(fd.parameter);   // register field param in scope

                    var initExpr = Translate(fd.initializer!);
                    var initLambda = LightExpression.Lambda(initExpr, thisParam);

                    var helperAttrs = MethodAttributes.Private | MethodAttributes.Static | MethodAttributes.HideBySig;
                    var helperBuilder = typeBuilder.DefineMethod($"<>_init_{fb.Name}", helperAttrs, fb.FieldType, [typeBuilder]);

                    LightCompiler.CompileFastToIL(initLambda, helperBuilder.GetILGenerator(), flags: default);

                    ctorIl.Emit(OpCodes.Ldarg_0);           // target for Stfld
                    ctorIl.Emit(OpCodes.Ldarg_0);           // argument to helper (this)
                    ctorIl.Emit(OpCodes.Call, helperBuilder);
                    ctorIl.Emit(OpCodes.Stfld, fb);
                }
                finally
                {
                    ClearScope();
                }
            }

            ctorIl.Emit(OpCodes.Ret);
        }

        /// <summary>
        /// Maps a Java <c>java.lang.reflect.Modifier</c> bitmask to <see cref="TypeAttributes"/> for <see cref="ModuleBuilder.DefineType"/>.
        /// </summary>
        protected virtual TypeAttributes JavaModifierToTypeAttributes(int modifier)
        {
            var attrs = TypeAttributes.Class | TypeAttributes.BeforeFieldInit;

            if ((modifier & java.lang.reflect.Modifier.PUBLIC) != 0)
                attrs |= TypeAttributes.Public;
            else
                attrs |= TypeAttributes.NotPublic;

            if ((modifier & java.lang.reflect.Modifier.ABSTRACT) != 0)
                attrs |= TypeAttributes.Abstract;

            if ((modifier & java.lang.reflect.Modifier.FINAL) != 0)
                attrs |= TypeAttributes.Sealed;

            return attrs;
        }

        /// <summary>Maps a Java <c>java.lang.reflect.Modifier</c> bitmask to <see cref="FieldAttributes"/> for <see cref="TypeBuilder.DefineField"/>.</summary>
        protected virtual FieldAttributes JavaModifierToFieldAttributes(int modifier)
        {
            var attrs = (modifier & java.lang.reflect.Modifier.PUBLIC) != 0
                ? FieldAttributes.Public
                : (modifier & java.lang.reflect.Modifier.PROTECTED) != 0
                    ? FieldAttributes.Family
                    : FieldAttributes.Private;

            if ((modifier & java.lang.reflect.Modifier.STATIC) != 0)
                attrs |= FieldAttributes.Static;
            if ((modifier & java.lang.reflect.Modifier.FINAL) != 0)
                attrs |= FieldAttributes.InitOnly;

            return attrs;
        }

        /// <summary>Maps a Java <c>java.lang.reflect.Modifier</c> bitmask to <see cref="MethodAttributes"/> for <see cref="TypeBuilder.DefineMethod"/>.</summary>
        protected virtual MethodAttributes JavaModifierToMethodAttributes(int modifier)
        {
            var attrs = MethodAttributes.HideBySig;

            if ((modifier & java.lang.reflect.Modifier.PUBLIC) != 0)
                attrs |= MethodAttributes.Public;
            else if ((modifier & java.lang.reflect.Modifier.PROTECTED) != 0)
                attrs |= MethodAttributes.Family;
            else
                attrs |= MethodAttributes.Private;

            if ((modifier & java.lang.reflect.Modifier.STATIC) != 0)
                attrs |= MethodAttributes.Static;
            if ((modifier & java.lang.reflect.Modifier.ABSTRACT) != 0)
                attrs |= MethodAttributes.Abstract | MethodAttributes.Virtual;
            if ((modifier & java.lang.reflect.Modifier.FINAL) != 0)
                attrs |= MethodAttributes.Final | MethodAttributes.Virtual;

            return attrs;
        }

        #endregion

        #region Helpers

        /// <summary>
        /// Opens a fresh variable scope and label table, making the translator ready to translate a new method body or field initializer.
        /// Called automatically by <see cref="EmitMethod"/> and <see cref="EmitConstructor"/> before each member body;
        /// can also be called manually before an independent <see cref="Translate"/> call.
        /// </summary>
        public void ResetScope()
        {
            _scope = new(ReferenceEqualityComparer.Instance);
            _labels = [];
        }

        /// <summary>
        /// Closes the current variable scope and label table by setting them back to <see langword="null"/>.
        /// Called automatically by <see cref="EmitMethod"/> and <see cref="EmitConstructor"/> after each member body is compiled.
        /// </summary>
        public void ClearScope()
        {
            _scope = null;
            _labels = null;
        }

        /// <summary>
        /// Returns the CLR <see cref="LightParameter"/> for the given linq4j parameter, creating one if it has not been seen before.
        /// </summary>
        internal LightParameter GetOrCreateParameter(ParameterExpression node)
        {
            if (_scope is null)
                throw new InvalidOperationException("No active scope. Call ResetScope() before translating a method body or field initializer.");

            if (_scope.TryGetValue(node, out var existing))
                return existing;

            var clrParam = LightExpression.Parameter(ClrType(node.getType()), node.name);
            _scope[node] = clrParam;
            return clrParam;
        }

        /// <summary>
        /// Returns the <see cref="LightLabelTarget"/> for the given name, creating one if it has not been seen before.
        /// </summary>
        LightLabelTarget GetOrCreateLabel(string name)
        {
            if (_labels is null)
                throw new InvalidOperationException("No active scope. Call ResetScope() before translating a method body or field initializer.");

            if (!_labels.TryGetValue(name, out var target))
            {
                target = LightExpression.Label(name);
                _labels[name] = target;
            }

            return target;
        }

        /// <summary>
        /// Converts a Java <c>java.lang.reflect.Type</c> to the corresponding CLR <see cref="System.Type"/> via IKVM's runtime helper.
        /// </summary>
        internal static System.Type ClrType(java.lang.reflect.Type javaType)
        {
            if (javaType is java.lang.Class cls)
                return ikvm.runtime.Util.getInstanceTypeFromClass(cls);

            return typeof(object);
        }

        /// <summary>
        /// Builds a nested if/else-if/else chain from a flat list produced by <see cref="ConditionalExpression"/> or <see cref="ConditionalStatement"/>.
        /// The list is encoded as [cond0, body0, cond1, body1, …, optionalElse].
        /// </summary>
        LightExpression BuildIfElseChain(IList<Node> exprs, int index)
        {
            if (index >= exprs.Count)
                return LightExpression.Empty();

            if (index == exprs.Count - 1)
                return Translate(exprs[index]);

            var test = Translate(exprs[index]);
            var ifTrue = Translate(exprs[index + 1]);
            var ifFalse = BuildIfElseChain(exprs, index + 2);

            return ifFalse is FastExpressionCompiler.LightExpression.DefaultExpression
                ? LightExpression.IfThen(test, ifTrue)
                : LightExpression.IfThenElse(test, ifTrue, ifFalse);
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
        static ConstructorInfo ResolveConstructor(System.Type clrType, LightExpression[] args)
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

        #endregion

    }

}

