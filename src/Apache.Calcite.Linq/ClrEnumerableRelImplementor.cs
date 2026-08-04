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
    /// The counterpart of <c>EnumerableRelImplementor</c>. What it composes differs, but what it holds does
    /// not: the <c>RexBuilder</c> a node translates its row expressions with, the correlation variables in
    /// scope, and the root the <c>DataContext</c> arrives by.
    ///
    /// <para>One <see cref="ExpressionTranslator"/> serves the whole plan, so a linq4j variable means the same
    /// CLR variable wherever it is mentioned. A correlated sub-query depends on exactly that: the outer row's
    /// parameter is registered by one node and read by another.</para>
    /// </remarks>
    public class ClrEnumerableRelImplementor
    {

        readonly RexBuilder rexBuilder;
        readonly java.util.Map map;
        readonly Dictionary<string, RexToLixTranslator.InputGetter> corrVars = [];

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rexBuilder"></param>
        /// <param name="internalParameters"></param>
        public ClrEnumerableRelImplementor(RexBuilder rexBuilder, java.util.Map internalParameters)
        {
            this.rexBuilder = rexBuilder ?? throw new ArgumentNullException(nameof(rexBuilder));
            this.map = internalParameters ?? throw new ArgumentNullException(nameof(internalParameters));

            Root = Expression.Parameter(typeof(DataContext), "root");
            Translator = new ExpressionTranslator();
            Translator.Bind(DataContext.ROOT, Root);

            AllCorrelateVariables = new DelegateFunction1<string, RexToLixTranslator.InputGetter>(GetCorrelVariableGetter);
        }

        /// <summary>
        /// Gets the builder a node translates its row expressions with.
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
        /// Gets the expression used to access the <see cref="DataContext"/> in a linq4j tree.
        /// </summary>
        /// <remarks>
        /// This is what is handed to <c>RexToLixTranslator</c>, which reaches it for a dynamic parameter, for
        /// <c>CURRENT_TIMESTAMP</c> and for <c>USER</c>. It is bound to <see cref="Root"/>, so what the
        /// translated tree reads is the argument this plan was called with.
        /// </remarks>
        public J.ParameterExpression RootExpression => DataContext.ROOT;

        /// <summary>
        /// Gets the translator carrying the scope every node's expressions are translated in.
        /// </summary>
        public ExpressionTranslator Translator { get; }

        /// <summary>
        /// Gets the values passed to the executor rather than written into the plan.
        /// </summary>
        public java.util.Map Map => map;

        /// <summary>
        /// Gets a getter for every correlation variable in scope.
        /// </summary>
        public Function1 AllCorrelateVariables { get; }

        /// <summary>
        /// Implements a child of a relational expression.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="ordinal"></param>
        /// <param name="child"></param>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public ClrEnumerableResult VisitChild(ClrEnumerableRel? parent, int ordinal, ClrEnumerableRel child, ClrEnumerablePrefer prefer)
        {
            return child.Implement(this, prefer);
        }

        /// <summary>
        /// Implements the root of a plan, as a function of the <see cref="DataContext"/> it is bound with.
        /// </summary>
        /// <param name="rootRel"></param>
        /// <param name="prefer"></param>
        /// <returns></returns>
        public LambdaExpression ImplementRoot(ClrEnumerableRel rootRel, ClrEnumerablePrefer prefer)
        {
            var result = rootRel.Implement(this, prefer);

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
        /// Stashes a value for the executor.
        /// </summary>
        /// <param name="input"></param>
        /// <param name="clazz"></param>
        /// <returns></returns>
        /// <remarks>
        /// Calcite has to put the value on the <see cref="DataContext"/> and read it back, because Janino
        /// compiles source text and source text cannot mention an object. An expression tree holds the object
        /// itself, so this is a constant. The method stays because every call site of it does.
        /// </remarks>
        public Expression Stash(object? input, java.lang.Class clazz)
        {
            return Expression.Constant(input, TypeResolver.FromClass(clazz));
        }

        /// <summary>
        /// Registers the variable a correlated sub-query reads its outer row by.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="pe"></param>
        /// <param name="corrBlock"></param>
        /// <param name="physType"></param>
        public void RegisterCorrelVariable(string name, J.ParameterExpression pe, J.BlockBuilder corrBlock, PhysType physType)
        {
            corrVars[name] = new CorrelInputGetter(name, pe, corrBlock, physType);
        }

        /// <summary>
        /// Forgets a correlation variable once its scope has ended.
        /// </summary>
        /// <param name="name"></param>
        public void ClearCorrelVariable(string name)
        {
            if (corrVars.Remove(name) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");
        }

        /// <summary>
        /// Returns the getter for a correlation variable.
        /// </summary>
        /// <param name="name"></param>
        /// <returns></returns>
        public RexToLixTranslator.InputGetter GetCorrelVariableGetter(string name)
        {
            if (corrVars.TryGetValue(name, out var getter) == false)
                throw new java.lang.IllegalStateException($"Correlation variable {name} should be defined");

            return getter;
        }

        /// <summary>
        /// Creates the result of implementing a node.
        /// </summary>
        /// <param name="physType"></param>
        /// <param name="expression"></param>
        /// <returns></returns>
        public ClrEnumerableResult Result(PhysType physType, Expression expression)
        {
            // PhysTypeImpl keeps its format package-private, and getFormat is the same value in public
            return new ClrEnumerableResult(expression, physType, physType.getFormat());
        }

        /// <summary>
        /// Gets the desired SQL conformance.
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
