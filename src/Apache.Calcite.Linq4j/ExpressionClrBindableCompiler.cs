using System;
using System.Collections.Generic;
using System.Reflection;

using FastExpressionCompiler.LightExpression;

using java.util;

using org.apache.calcite.linq4j;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Linq4j
{

    /// <summary>
    /// A <see cref="ClrBindableCompiler"/> that compiles a Calcite <see cref="ClassDeclaration"/>
    /// into a FastExpression light-expression lambda and wraps it in a <see cref="ClrArrayBindable"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's enumerable adapter generates one <c>ClassDeclaration</c> per query. The class
    /// always contains a <c>bind(DataContext)</c> method whose body is a linq4j
    /// <see cref="BlockStatement"/>. This compiler:
    /// <list type="number">
    ///   <item>Seeds a fresh <see cref="Linq4jExpressionTranslator"/> with the class's field
    ///         declarations so that field references inside method bodies resolve correctly.</item>
    ///   <item>Locates the <c>bind</c> method declaration and compiles its body into a CLR
    ///         lambda whose single parameter is a <see cref="org.apache.calcite.DataContext"/>.</item>
    ///   <item>Wraps the compiled delegate in a <see cref="ClrArrayBindable"/> and returns it
    ///         as the query <see cref="Bindable"/>.</item>
    /// </list>
    /// </para>
    /// <para>
    /// Trees are compiled via <c>CompileFast()</c> rather than the BCL <c>Compile()</c> path.
    /// Any methods beyond <c>bind</c> are ignored; Calcite helper methods called from within
    /// <c>bind</c> are already inlined in the linq4j tree.
    /// </para>
    /// </remarks>
    public class ExpressionClrBindableCompiler : ClrBindableCompiler
    {

        const string BindMethodName = "bind";

        /// <inheritdoc/>
        public override Bindable Compile(ClassDeclaration classDeclaration, string classBody, int fieldCount)
        {
            ArgumentNullException.ThrowIfNull(classDeclaration);

            var translator = new Linq4jExpressionTranslator();

            // Seed translator scope with all field declarations.
            var (fieldVars, fieldInits) = translator.TranslateClassDeclaration(classDeclaration);

            // Find the bind(DataContext) method.
            MethodDeclaration? bindMethod = null;
            foreach (var member in classDeclaration.memberDeclarations.AsList<MemberDeclaration>())
            {
                if (member is MethodDeclaration md && md.name == BindMethodName)
                {
                    bindMethod = md;
                    break;
                }
            }

            if (bindMethod is null)
                throw new InvalidOperationException(
                    $"Generated class '{classDeclaration.name}' does not contain a '{BindMethodName}' method.");

            // Build the CLR DataContext parameter and register it in the translator scope.
            var dataContextParam = Expression.Parameter(
                typeof(org.apache.calcite.DataContext), "dataContext");

            var bindParams = bindMethod.parameters.AsList<ParameterExpression>();
            if (bindParams.Count > 0)
                RegisterParameter(translator, bindParams[0], dataContextParam);

            var bindBodyExpr = translator.Translate(bindMethod.body);

            // Wrap field initialisations around the bind body.
            Expression fullBody;
            if (fieldVars.Length > 0)
            {
                var stmts = new Expression[fieldInits.Length + 1];
                Array.Copy(fieldInits, stmts, fieldInits.Length);
                stmts[fieldInits.Length] = bindBodyExpr;
                fullBody = Expression.Block(fieldVars, stmts);
            }
            else
            {
                fullBody = bindBodyExpr;
            }

            var lambda = Expression.Lambda<Func<org.apache.calcite.DataContext, Enumerable>>(
                fullBody, dataContextParam);

            return new ClrArrayBindable(lambda.CompileFast());
        }

        /// <summary>
        /// Registers a linq4j <see cref="ParameterExpression"/> against an already-created
        /// <see cref="ParameterExpression"/> in the translator's scope.
        /// </summary>
        static void RegisterParameter(
            Linq4jExpressionTranslator translator,
            ParameterExpression javaParam,
            ParameterExpression clrParam)
        {
            var scopeField = typeof(Linq4jExpressionTranslator).GetField(
                "_scope", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var scope = (Dictionary<ParameterExpression, ParameterExpression>)
                scopeField.GetValue(translator)!;
            scope[javaParam] = clrParam;
        }

    }

}

    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite's enumerable adapter generates one <c>ClassDeclaration</c> per query. The class
    /// always contains a <c>bind(DataContext)</c> method whose body is a linq4j
    /// <see cref="BlockStatement"/>. This compiler:
    /// <list type="number">
    ///   <item>Seeds a fresh <see cref="Linq4jExpressionTranslator"/> with the class's field
    ///         declarations so that field references inside method bodies resolve correctly.</item>
    ///   <item>Locates the <c>bind</c> method declaration and compiles its body into a CLR
    ///         <see cref="System.Linq.Expressions.LambdaExpression"/> whose single parameter is
    ///         a <see cref="org.apache.calcite.DataContext"/>.</item>
    ///   <item>Wraps the compiled delegate in a <see cref="ClrArrayBindable"/> and returns it
    ///         as the query <see cref="Bindable"/>.</item>
    /// </list>
    /// </para>
    /// <para>
    /// Any methods beyond <c>bind</c> are ignored; Calcite helper methods called from within
    /// <c>bind</c> are already inlined in the linq4j tree.
    /// </para>
    /// </remarks>
    public class ExpressionClrBindableCompiler : ClrBindableCompiler
    {

        const string BindMethodName = "bind";

        /// <inheritdoc/>
        public override Bindable Compile(ClassDeclaration classDeclaration, string classBody, int fieldCount)
        {
            ArgumentNullException.ThrowIfNull(classDeclaration);

            var translator = new Linq4jExpressionTranslator();

            // Seed translator scope with all field declarations.
            var (fieldVars, fieldInits) = translator.TranslateClassDeclaration(classDeclaration);

            // Find the bind(DataContext) method.
            MethodDeclaration? bindMethod = null;
            foreach (var member in classDeclaration.memberDeclarations.AsList<MemberDeclaration>())
            {
                if (member is MethodDeclaration md && md.name == BindMethodName)
                {
                    bindMethod = md;
                    break;
                }
            }

            if (bindMethod is null)
                throw new InvalidOperationException(
                    $"Generated class '{classDeclaration.name}' does not contain a '{BindMethodName}' method.");

            // Build the CLR DataContext parameter and register it in the translator scope.
            var dataContextParam = System.Linq.Expressions.Expression.Parameter(
                typeof(org.apache.calcite.DataContext), "dataContext");

            var bindParams = bindMethod.parameters.AsList<ParameterExpression>();
            if (bindParams.Count > 0)
                RegisterParameter(translator, bindParams[0], dataContextParam);

            var bindBodyExpr = translator.Translate(bindMethod.body);

            // Wrap field initialisations around the bind body.
            LinqExpr fullBody;
            if (fieldVars.Length > 0)
            {
                var stmts = new LinqExpr[fieldInits.Length + 1];
                Array.Copy(fieldInits, stmts, fieldInits.Length);
                stmts[fieldInits.Length] = bindBodyExpr;
                fullBody = System.Linq.Expressions.Expression.Block(fieldVars, stmts);
            }
            else
            {
                fullBody = bindBodyExpr;
            }

            var lambda = System.Linq.Expressions.Expression.Lambda<Func<org.apache.calcite.DataContext, Enumerable>>(
                fullBody, dataContextParam);

            return new ClrArrayBindable(lambda.Compile());
        }

        /// <summary>
        /// Registers a linq4j <see cref="ParameterExpression"/> against an already-created CLR
        /// <see cref="System.Linq.Expressions.ParameterExpression"/> in the translator's private
        /// scope via reflection.
        /// </summary>
        static void RegisterParameter(
            Linq4jExpressionTranslator translator,
            ParameterExpression javaParam,
            System.Linq.Expressions.ParameterExpression clrParam)
        {
            var scopeField = typeof(Linq4jExpressionTranslator).GetField(
                "_scope", BindingFlags.NonPublic | BindingFlags.Instance)!;
            var scope = (Dictionary<ParameterExpression, System.Linq.Expressions.ParameterExpression>)
                scopeField.GetValue(translator)!;
            scope[javaParam] = clrParam;
        }

    }

}
