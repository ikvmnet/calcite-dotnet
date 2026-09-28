using System;

using Apache.Calcite.FullText.Sql;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// One <c>CLR_FT_*</c> operator at one arity, declared as a schema function.
    /// </summary>
    /// <remarks>
    /// <para>Calcite builds its own <c>SqlUserDefinedFunction</c> from this declaration's name and parameter
    /// list, so a call in a plan carries that operator rather than <see cref="Operator"/>; recognise it with
    /// <see cref="FullTextOperatorTable.Matches"/>.</para>
    ///
    /// <para>There is no implementation. <see cref="getImplementor"/> throws with a message explaining that
    /// the call has to be evaluated by the store, which is the error a caller sees when a plan that still
    /// holds the call is compiled.</para>
    /// </remarks>
    public sealed class FullTextSchemaFunction : ScalarFunction, ImplementableFunction
    {

        readonly SqlFunction op;
        readonly int arity;
        readonly FullTextOperand[] operands;
        readonly java.util.List parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="op">The operator to declare; one of the fields of <see cref="FullTextOperatorTable"/>.</param>
        /// <param name="arity">How many operands this declaration takes. Every parameter is required.</param>
        /// <exception cref="ArgumentNullException"><paramref name="op"/> is <c>null</c>.</exception>
        /// <exception cref="ArgumentException">
        /// <paramref name="op"/> does not use a <see cref="FullTextOperandTypeChecker"/>.
        /// </exception>
        public FullTextSchemaFunction(SqlFunction op, int arity)
        {
            ArgumentNullException.ThrowIfNull(op);

            this.op = op;
            this.arity = arity;

            var checker = op.getOperandTypeChecker() as FullTextOperandTypeChecker ??
                throw new ArgumentException($"'{op.getName()}' is not a full text operator.", nameof(op));

            // Each parameter is required, since Calcite pads optional ones with DEFAULT, and typed as the
            // operator's own checker types that position, so both routes validate the same calls.
            operands = new FullTextOperand[arity];
            parameters = new java.util.ArrayList(arity);

            for (var i = 0; i < arity; i++)
            {
                operands[i] = checker.At(i);
                parameters.add(new FullTextSchemaFunctionParameter(i, operands[i]));
            }
        }

        /// <summary>
        /// Gets the operator this declares.
        /// </summary>
        public SqlFunction Operator => op;

        /// <summary>
        /// Gets how many operands this declaration takes.
        /// </summary>
        public int Arity => arity;

        /// <summary>
        /// Returns the parameters, one <see cref="FullTextSchemaFunctionParameter"/> per operand.
        /// </summary>
        /// <returns>A list of <see cref="Arity"/> required parameters.</returns>
        public java.util.List getParameters()
        {
            return parameters;
        }

        /// <summary>
        /// Returns the type of a call to this function.
        /// </summary>
        /// <remarks>
        /// Asks the operator's own return type inference, over the declared parameter types, so that a call
        /// resolved through a schema is typed as one resolved through the operator table: a nullable
        /// <c>BOOLEAN</c> for a predicate, a nullable <c>DOUBLE</c> for a score, and <c>ANY</c> for a term
        /// constructor.
        /// </remarks>
        /// <param name="typeFactory">The type factory.</param>
        /// <returns>The return type.</returns>
        public RelDataType getReturnType(RelDataTypeFactory typeFactory)
        {
            var types = new java.util.ArrayList(arity);

            foreach (var operand in operands)
                types.add(FullTextOperandTypeChecker.TypeOf(operand, typeFactory));

            return op.inferReturnType(new ExplicitOperatorBinding(typeFactory, op, types));
        }

        /// <summary>
        /// Always throws: a full text call has no in-process implementation.
        /// </summary>
        /// <remarks>
        /// Calcite asks for the implementor while generating code, so this runs only for a call that no rule
        /// pushed down to a store. <c>ImplementableFunction</c> is implemented, rather than left off, so that the
        /// failure carries the message from <see cref="Refusal"/> instead of Calcite's report that the function
        /// does not implement the interface.
        /// </remarks>
        /// <returns>Never returns.</returns>
        /// <exception cref="java.lang.UnsupportedOperationException">Always, with the message from <see cref="Refusal"/>.</exception>
        public CallImplementor getImplementor()
        {
            throw new java.lang.UnsupportedOperationException(Refusal(op.getName()));
        }

        /// <summary>
        /// Returns the message explaining why a full text call cannot be evaluated in process.
        /// </summary>
        /// <remarks>
        /// <c>CLR_FT_SCORE</c> and <c>CLR_FT_RRF</c> get a message about relevance scores, naming the usual
        /// causes (an ordering that could not be pushed down whole, or a projected score the store will not
        /// return); every other name gets a message about the store's analyzer.
        /// </remarks>
        /// <param name="name">The function's name.</param>
        /// <returns>The message.</returns>
        public static string Refusal(string name)
        {
            return name is "CLR_FT_SCORE" or "CLR_FT_RRF"
                ? name + " has no value this package can produce. A relevance score is computed by the store "
                    + "while it searches, from an index and an analyzer that exist only there. This plan asks "
                    + "for the value somewhere the call could not be pushed down — commonly an ordering the "
                    + "adapter could not push down whole, or a projected score in a store that will not "
                    + "return one."
                : name + " is evaluated by the store and has no in-process body. Which documents match is "
                    + "decided by the store's analyzer, and approximating one would answer differently from "
                    + "the store for the same query. This plan asks for the value somewhere the call could "
                    + "not be pushed down.";
        }

    }

}
