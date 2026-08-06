using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq.Expressions;

using Apache.Calcite.Linq.Runtime;
using Apache.Calcite.Linq.Tree;

using org.apache.calcite;
using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Linq
{

    /// <summary>
    /// Turns a tree of <see cref="ClrEnumerableRel"/> into the expression that runs it.
    /// </summary>
    /// <remarks>
    /// The counterpart of Calcite's <c>EnumerableRelImplementor</c>, and used the same way: one instance
    /// implements one plan. A node reaches its inputs through <see cref="VisitChild"/> and returns a
    /// <see cref="ClrEnumerableResult"/>; <see cref="ImplementRoot"/> does the whole plan at once.
    ///
    /// <para>This is how a plan of this convention is run: cast the planned root to
    /// <see cref="ClrEnumerableRel"/>, pass it to <see cref="ImplementRoot"/>, and compile the lambda that
    /// comes back into a <c>Func&lt;DataContext, IEnumerable&gt;</c>.</para>
    /// </remarks>
    public class ClrEnumerableRelImplementor
    {

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, RexToLixTranslator.InputGetter> corrVars = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder">The builder for row expressions, from the plan's cluster.</param>
        /// <param name="internalParameters">The map values are stashed into, which must be the one the
        /// <see cref="DataContext"/> will serve at run time.</param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters)
        {
            this.rexBuilder = rexBuilder ?? throw new ArgumentNullException(nameof(rexBuilder));
            this.map = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));

            Root = Expression.Parameter(typeof(DataContext), "root");
            Translator = new ExpressionTranslator(map);
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
        /// Gets the linq4j expression standing for the <see cref="DataContext"/>, to hand to a generator of
        /// Calcite's that needs one.
        /// </summary>
        /// <remarks>
        /// Bound to <see cref="Root"/>, so a translated tree reads the argument the plan was called with.
        /// </remarks>
        public J.ParameterExpression RootExpression => DataContext.ROOT;

        /// <summary>
        /// Gets the translator that turns a linq4j expression into a CLR one. One serves the whole plan, so a
        /// variable means the same thing wherever a node mentions it.
        /// </summary>
        internal ExpressionTranslator Translator { get; }

        /// <summary>
        /// Translates a linq4j expression into a CLR one.
        /// </summary>
        /// <param name="node">The expression to translate.</param>
        /// <returns>The same expression, as a <see cref="System.Linq.Expressions"/> tree.</returns>
        /// <remarks>
        /// A node of this convention builds expressions directly and has no need of this. It is here for the
        /// node that cannot: one whose expression comes from a generator of Calcite's, which produces linq4j
        /// and nothing else. Translate such an expression where it is produced rather than composing it into
        /// a larger tree first.
        /// </remarks>
        /// <exception cref="ArgumentNullException"></exception>
        public Expression Translate(J.Node node)
        {
            ArgumentNullException.ThrowIfNull(node);

            return Translator.Translate(node);
        }

        /// <summary>
        /// Translates a linq4j block, and the declarations it carries, into one CLR expression.
        /// </summary>
        /// <param name="body">The block to translate, whose last statement is its value.</param>
        /// <param name="returnType">The type the block yields.</param>
        /// <returns>The block, as a <see cref="System.Linq.Expressions"/> tree.</returns>
        /// <exception cref="ArgumentNullException"></exception>
        public Expression TranslateBody(J.BlockStatement body, Type returnType)
        {
            ArgumentNullException.ThrowIfNull(body);
            ArgumentNullException.ThrowIfNull(returnType);

            return Translator.TranslateBody(body, returnType);
        }

        /// <summary>
        /// Gets the internal parameters, which reach the query through the <see cref="DataContext"/> it is
        /// bound with rather than through the plan.
        /// </summary>
        public java.util.Map Map => map;

        /// <summary>
        /// Gets the lookup from a correlation variable's name to its getter, to hand to a generator of
        /// Calcite's that translates row expressions.
        /// </summary>
        public Function1 AllCorrelateVariables { get; }

        /// <summary>
        /// Implements one input of a node.
        /// </summary>
        /// <param name="parent">The node being implemented, or <see langword="null"/> for a root.</param>
        /// <param name="ordinal">Which input of <paramref name="parent"/> this is.</param>
        /// <param name="child">The input to implement.</param>
        /// <param name="prefer">How the parent wants the input's rows represented.</param>
        /// <returns>The input's plan, physical type and row format.</returns>
        public ClrEnumerableResult VisitChild(ClrEnumerableRel? parent, int ordinal, ClrEnumerableRel child, ClrEnumerablePrefer prefer)
        {
            return child.Implement(this, prefer);
        }

        /// <summary>
        /// Implements a whole plan as a function of the <see cref="DataContext"/> it will be bound with.
        /// </summary>
        /// <param name="rootRel">The root of the plan, which must be of this convention.</param>
        /// <param name="prefer">How the caller wants rows represented.</param>
        /// <returns>
        /// A lambda of one <see cref="DataContext"/> parameter whose value is the rows.
        /// <see cref="System.Linq.Expressions.LambdaExpression.Compile()"/> gives a
        /// <c>Func&lt;DataContext, IEnumerable&gt;</c>.
        /// </returns>
        /// <exception cref="java.lang.IllegalStateException">
        /// A node of the plan could not be implemented. The message names the plan; the failure itself is the
        /// inner exception.
        /// </exception>
        public LambdaExpression ImplementRoot(ClrEnumerableRel rootRel, ClrEnumerablePrefer prefer)
        {
            ClrEnumerableResult result;

            try
            {
                result = rootRel.Implement(this, prefer);
            }
            catch (Exception e)
            {
                throw new java.lang.IllegalStateException(
                    $"Unable to implement {org.apache.calcite.plan.RelOptUtil.toString(rootRel, org.apache.calcite.sql.SqlExplainLevel.ALL_ATTRIBUTES)}", e);
            }

            // a one column result is the value, not a one element row, which is what every caller of a query
            // expects and what EnumerableRelImplementor arranges the same way
            if (prefer == ClrEnumerablePrefer.Array
                && result.Format == JavaRowFormat.ARRAY
                && rootRel.getRowType().getFieldCount() == 1)
                result = new ClrEnumerableResult(
                    Expression.Call(null, ClrBuiltInMethod.Slice0, result.Expression),
                    result.PhysType,
                    JavaRowFormat.SCALAR);

            return Expression.Lambda<Func<DataContext, IEnumerable>>(
                Expression.Convert(BoxScalars(result.Expression), typeof(IEnumerable)),
                Root);
        }

        /// <summary>
        /// Boxes a sequence of primitives the way Java would.
        /// </summary>
        /// <param name="sequence"></param>
        /// <returns></returns>
        /// <remarks>
        /// A one column result is the value, and where that value is a primitive the sequence is of a
        /// primitive. Handing it out untyped boxes it, and the CLR would box it as its own, which is not the
        /// java.lang.Integer every reader of a Calcite result expects. The type factory decides what a value
        /// is, and it says Integer, so this is the same boxing every other conversion here does.
        ///
        /// <para>Not <c>EnumerableInterpretable.box</c>, which wraps each row in a one element
        /// <c>Object[]</c> for the interpreter and has no counterpart here. This has no counterpart there
        /// either: generated Java produces an <c>Enumerable</c> of objects and the question cannot arise.
        /// It belongs with <see cref="ImplementRoot"/> because that is where a plan becomes the sequence a
        /// caller reads.</para>
        /// </remarks>
        static Expression BoxScalars(Expression sequence)
        {
            if (sequence.Type.IsGenericType == false)
                return sequence;

            var elementType = sequence.Type.GetGenericArguments()[0];
            if (elementType.IsValueType == false)
                return sequence;

            var row = Expression.Parameter(elementType, "row");

            return Expression.Call(null,
                ClrBuiltInMethod.Select.MakeGenericMethod(elementType, typeof(object)),
                sequence,
                Expression.Lambda(JavaCast.To(row, typeof(object)), row));
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
            return Expression.Constant(input, TypeResolver.FromClass(clazz));
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
        /// Reads one field of the outer row a correlated sub-query was entered with.
        /// </summary>
        /// <param name="name">The correlation variable's name.</param>
        /// <param name="ordinal">The field's position in the outer row.</param>
        /// <param name="storageType">The type to read the field as, or <see langword="null"/> for the
        /// field's own.</param>
        /// <returns>The field's value.</returns>
        /// <exception cref="java.lang.IllegalStateException">No such variable is in scope.</exception>
        /// <remarks>
        /// <see cref="GetCorrelVariableGetter"/> answers with Calcite's <c>InputGetter</c>, which reads a
        /// field as linq4j and takes a linq4j block to declare into. This is that, translated, so a node
        /// outside this assembly can read a correlated field without holding a linq4j tree of its own.
        /// </remarks>
        public Expression CorrelVariableField(string name, int ordinal, java.lang.reflect.Type? storageType = null)
        {
            ArgumentNullException.ThrowIfNull(name);

            // the getter declares into the block it was created with, not one passed to it, so there is
            // nothing here for a caller's block to receive
            return Translate(GetCorrelVariableGetter(name).field(null, ordinal, storageType));
        }

        /// <summary>
        /// Creates the result a node's <c>Implement</c> returns.
        /// </summary>
        /// <param name="physType">How the rows are represented.</param>
        /// <param name="expression">The plan, whose value is the rows.</param>
        /// <returns></returns>
        public ClrEnumerableResult Result(PhysType physType, Expression expression)
        {
            // PhysTypeImpl keeps its format package-private, and getFormat is the same value in public
            return new ClrEnumerableResult(expression, physType, physType.getFormat());
        }

        /// <summary>
        /// Gets the SQL conformance the query is being planned under.
        /// </summary>
        public SqlConformance Conformance => (SqlConformance)map.getOrDefault("_conformance", SqlConformanceEnum.DEFAULT);

        /// <summary>
        /// Reads a field of the outer row a correlated sub-query was entered with.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="pe"></param>
        /// <param name="corrBlock"></param>
        /// <param name="physType"></param>
        sealed class CorrelInputGetter(string name, J.ParameterExpression pe, J.BlockBuilder corrBlock, PhysType physType) : RexToLixTranslator.InputGetter
        {

            /// <inheritdoc />
            public J.Expression field(J.BlockBuilder list, int index, java.lang.reflect.Type storageType)
            {
                return corrBlock.append(name + "_" + index, physType.fieldReference(pe, index, storageType));
            }

        }

    }

}
