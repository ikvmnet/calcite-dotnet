using System;

using Apache.Calcite.FullText.Sql;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.sql;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// One <c>CLR_FT_*</c> operator at one arity, in the form a schema declares it.
    /// </summary>
    /// <remarks>
    /// <para>A schema function is a shape rather than an operator: Calcite reads the parameter list and builds
    /// a <c>SqlUserDefinedFunction</c> of its own around it, so what reaches a plan carries this one's name and
    /// arity rather than being this one. That is why <c>FullTextOperatorTable.Matches</c> asks a call for its
    /// name — an adapter exists to render one of these into a statement, and the name is the whole of what
    /// rendering needs.</para>
    ///
    /// <para><b>No body, and <c>ImplementableFunction</c> implemented anyway.</b> Binding a CLR method here
    /// would let a call that no rule pushed down plan regardless and then answer with something the store never
    /// computed — a wrong answer rather than a failure. With no body the refusal happens before any row exists.
    /// But declining the interface leaves Calcite to report it as <c>User defined function CLR_FT_SCORE must
    /// implement ImplementableFunction</c>, which names an interface rather than a reason and reads as a defect
    /// in the adapter. Implementing it and throwing gives the same refusal at the same moment — Calcite asks for
    /// a body while generating code — with a sentence saying why.</para>
    ///
    /// <para><b>And unlike Cosmos's type tests, none of these will ever acquire a body.</b> There the question
    /// "would an in-process body answer what the service answers?" had an answer, and a differential test
    /// against a live account said yes for eight of them. Here it cannot: a full text match is decided by the
    /// store's analyzer — tokenising, case folding, stemming, stopwords, per language — and an approximation
    /// answers differently for the same query. So this is a property of the family rather than a decision per
    /// function.</para>
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
        /// <param name="op">The operator to declare.</param>
        /// <param name="arity">How many operands this declaration takes.</param>
        public FullTextSchemaFunction(SqlFunction op, int arity)
        {
            ArgumentNullException.ThrowIfNull(op);

            this.op = op;
            this.arity = arity;

            var checker = op.getOperandTypeChecker() as FullTextOperandTypeChecker ??
                throw new ArgumentException($"'{op.getName()}' is not a full text operator.", nameof(op));

            // Every one required, and every one typed as the operator's own checker types that position. An
            // optional parameter is padded with DEFAULT at the call site and no store renders one; a
            // differently typed one would make the two routes disagree about what validates.
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

        /// <inheritdoc />
        public java.util.List getParameters()
        {
            return parameters;
        }

        /// <summary>
        /// Returns what a call to this function is typed as.
        /// </summary>
        /// <remarks>
        /// Asked of the operator rather than restated here. Every one of these infers its return type from
        /// nothing else — a nullable boolean or a nullable double — so the operands are a formality; asking
        /// anyway is what keeps a plan built through a connection and a plan built through a chained operator
        /// table the same plan.
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
        /// Refuses, and says why.
        /// </summary>
        /// <remarks>
        /// Calcite asks for this while generating code for a plan, so a call that survived to here is one no
        /// rule pushed down. Throwing is the same outcome as not implementing the interface, at the same
        /// moment; what it adds is the reason.
        /// </remarks>
        /// <returns>Never.</returns>
        /// <exception cref="java.lang.UnsupportedOperationException">Always.</exception>
        public CallImplementor getImplementor()
        {
            throw new java.lang.UnsupportedOperationException(Refusal(op.getName()));
        }

        /// <summary>
        /// Says why a full text call cannot be evaluated where the plan put it.
        /// </summary>
        /// <remarks>
        /// The scoring functions get their own sentence because a caller reaching that one has usually done
        /// something different: ordering by a score in a plan whose ordering could not be pushed down whole,
        /// rather than writing a predicate the adapter declined.
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
