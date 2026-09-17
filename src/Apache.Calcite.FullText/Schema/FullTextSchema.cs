using System;

using Apache.Calcite.FullText.Sql;

using com.google.common.collect;

using org.apache.calcite.schema;
using org.apache.calcite.sql;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// The same operators <see cref="FullTextOperatorTable"/> carries, in the form a schema declares them, so
    /// that a plain connection can name one.
    /// </summary>
    /// <remarks>
    /// <para><b>There are two routes to a name and they are not interchangeable.</b> A validator resolves a
    /// function against the operator table it was built with, chained with the catalog reader — and the
    /// catalog reader resolves a schema's own functions. So an operator table is something a <em>host</em>
    /// hands a planner it assembled, and a schema function is something a <em>connection</em> finds by
    /// itself. A stock <c>jdbc:calcite:</c> connection chains nothing, so without these the whole package
    /// would be reachable only by embedders.</para>
    ///
    /// <code>
    /// FullTextSchema.AddTo(rootSchema);
    /// </code>
    ///
    /// <para><b>Declare at every level the connection might be rooted at.</b> An unqualified name resolves
    /// against the connection's default schema and the root, and nowhere else — never a subschema. An adapter
    /// with a schema per database under a schema per account declares at both, and nothing resolves twice
    /// because no arrangement searches both for one unqualified name.</para>
    ///
    /// <para><b>Chaining the operator table as well is not a duplicate, and is not a second registration
    /// either.</b> Overload resolution takes the first candidate whose arity fits and a chained table comes
    /// before the catalog reader, so the operator answers and the declaration is simply not reached. What it
    /// buys is the arity past <see cref="VariadicOperandLimit"/>.</para>
    ///
    /// <para><b>The same order is why the names are worth a tripwire.</b> A connection chains the table its
    /// <c>fun</c> property names ahead of the catalog reader, so the day Calcite gives some library a function
    /// called <c>CLR_FT_SCORE</c>, that operator would answer and these declarations would stop being reached —
    /// silently, and only for hosts that set <c>fun</c>. The failure would be a wrong statement rather than an
    /// error. This package owns that test once rather than having each adapter remember to write it.</para>
    /// </remarks>
    public static class FullTextSchema
    {

        /// <summary>
        /// How many operands a function with no declared upper arity is offered through a schema.
        /// </summary>
        /// <remarks>
        /// Calcite builds a function's operand count range out of its parameter list, so a schema function
        /// accepts as many operands as it declares parameters and no more. The operators themselves are
        /// unbounded — <c>CLR_FT_CONTAINS_ALL</c> takes as many keywords as a caller has — and this is the arity
        /// at which that stops being true through a connection. A query needing more still resolves against
        /// <c>FullTextOperatorTable.Instance()</c>, whose checker is genuinely variadic, which is what
        /// chaining it is still for.
        /// </remarks>
        public const int VariadicOperandLimit = 16;

        /// <summary>
        /// Registers every <c>CLR_FT_*</c> operator on the given schema.
        /// </summary>
        /// <param name="schema">The schema to register on.</param>
        /// <returns>The schema, for chaining.</returns>
        public static SchemaPlus AddTo(SchemaPlus schema)
        {
            ArgumentNullException.ThrowIfNull(schema);

            var functions = Functions();
            var entries = functions.entries().iterator();

            while (entries.hasNext())
            {
                var entry = (java.util.Map.Entry)entries.next();
                schema.add((string)entry.getKey(), (Function)entry.getValue());
            }

            return schema;
        }

        /// <summary>
        /// Gets the functions a schema declares, keyed by name.
        /// </summary>
        /// <remarks>
        /// For an adapter that implements <c>Schema.getFunctions</c> itself and wants these to arrive with its
        /// tables. <see cref="AddTo"/> is the same thing for a caller holding a <c>SchemaPlus</c>.
        /// </remarks>
        /// <returns>The declarations.</returns>
        public static Multimap Functions()
        {
            return instance;
        }

        static readonly Multimap instance = Build();

        /// <summary>
        /// Declares each operator once per arity it accepts.
        /// </summary>
        /// <remarks>
        /// <para><b>One declaration per arity rather than one with optional parameters</b>, and that is a
        /// measurement rather than a preference. <c>SqlCallBinding.operands</c> pads a call out to the whole
        /// parameter list with <c>DEFAULT</c> where three things hold at once — room under the count range's
        /// maximum, the position is optional, and the checker's parameters are fixed — so a single variadic
        /// declaration produced <c>CLR_FT_CONTAINS_ALL(BODY, 'steel', DEFAULT(), …)</c> out to the limit, and no
        /// store has a rendering for <c>DEFAULT</c>. Declared one arity at a time, every parameter is
        /// required, the second condition is false, and nothing is padded.</para>
        ///
        /// <para>A name therefore carries several declarations, which is what a multimap is for and what
        /// Calcite's overload resolution expects: it keeps the candidates whose operand count range accepts
        /// the call, and exactly one of these does.</para>
        ///
        /// <para>Derived from the operator table rather than listed beside it. The two would otherwise be one
        /// list written twice, and an operator added to one and forgotten in the other would resolve through a
        /// planner a host built and not through a connection — which is the gap these close.</para>
        /// </remarks>
        static Multimap Build()
        {
            var builder = ImmutableMultimap.builder();

            var operators = FullTextOperatorTable.Instance().getOperatorList();

            for (var i = 0; i < operators.size(); i++)
            {
                if (operators.get(i) is not SqlFunction function)
                    continue;

                var range = function.getOperandCountRange();
                var minimum = range.getMin();

                // A count range with no maximum reports -1.
                var maximum = range.getMax() < 0 ? Math.Max(VariadicOperandLimit, minimum) : range.getMax();

                for (var arity = minimum; arity <= maximum; arity++)
                    builder.put(function.getName(), new FullTextSchemaFunction(function, arity));
            }

            return builder.build();
        }

    }

}
