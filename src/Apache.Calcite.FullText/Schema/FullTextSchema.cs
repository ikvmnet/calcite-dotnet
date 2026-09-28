using System;

using Apache.Calcite.FullText.Sql;

using com.google.common.collect;

using org.apache.calcite.schema;
using org.apache.calcite.sql;

namespace Apache.Calcite.FullText.Schema
{

    /// <summary>
    /// Declares the <c>CLR_FT_*</c> operators as schema functions, so that a connection which cannot chain
    /// <see cref="FullTextOperatorTable"/>, such as a plain <c>jdbc:calcite:</c> connection, can resolve them.
    /// </summary>
    /// <remarks>
    /// <para>Calcite's catalog reader resolves a schema's functions for every statement a connection prepares,
    /// so registering them is enough:</para>
    ///
    /// <code>
    /// FullTextSchema.AddTo(rootSchema);
    /// </code>
    ///
    /// <para>An unqualified function name resolves against the connection's default schema and the root
    /// schema only, never against another subschema, so declare the functions at each level a connection may
    /// use as its default.</para>
    ///
    /// <para>Use this or the operator table, not both. With both registered, each call has two candidates,
    /// and Calcite's type-precedence pass then throws <c>IllegalArgumentException</c> for a call whose
    /// searched operand is an <c>ARRAY</c> column.</para>
    ///
    /// <para>A schema function takes a fixed number of operands, so each variadic operator is declared once
    /// per arity up to <see cref="VariadicOperandLimit"/>. A call with more operands resolves only through the
    /// operator table.</para>
    /// </remarks>
    public static class FullTextSchema
    {

        /// <summary>
        /// The largest number of operands, the searched operand included, for which a variadic operator is
        /// declared on a schema.
        /// </summary>
        /// <remarks>
        /// Calcite derives a schema function's operand count from its parameter list, so each variadic
        /// operator (<c>CLR_FT_CONTAINS_ALL</c>, <c>CLR_FT_CONTAINS_ANY</c>, <c>CLR_FT_SCORE</c>,
        /// <c>CLR_FT_RRF</c>) is declared once for every arity from two up to this limit. The operators in
        /// <see cref="FullTextOperatorTable"/> have no upper limit.
        /// </remarks>
        public const int VariadicOperandLimit = 16;

        /// <summary>
        /// Adds every declaration in <see cref="Functions"/> to the given schema.
        /// </summary>
        /// <param name="schema">The schema to register on.</param>
        /// <returns>The schema, for chaining.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="schema"/> is <c>null</c>.</exception>
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
        /// Gets the schema function declarations, keyed by name, with one <see cref="FullTextSchemaFunction"/>
        /// per operator and arity.
        /// </summary>
        /// <remarks>
        /// For an adapter that implements <c>Schema.getFunctions</c> itself and returns these alongside its own
        /// functions. The same immutable multimap is returned on every call.
        /// </remarks>
        /// <returns>The declarations.</returns>
        public static Multimap Functions()
        {
            return instance;
        }

        static readonly Multimap instance = Build();

        /// <summary>
        /// Declares each operator of <see cref="FullTextOperatorTable"/> once per arity it accepts.
        /// </summary>
        /// <remarks>
        /// <para>One declaration per arity with every parameter required, rather than one declaration with
        /// optional parameters, because <c>SqlCallBinding.operands</c> pads a call to a function with optional
        /// parameters out to the full parameter list with <c>DEFAULT</c>, which no store can render. Overload
        /// resolution keeps the one declaration whose operand count matches the call.</para>
        ///
        /// <para>The list is read from the operator table so that the two routes offer the same operators.</para>
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
