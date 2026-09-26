using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// What the shared Rex helpers need of an implementor, whichever convention it belongs to.
    /// </summary>
    /// <remarks>
    /// Four members, and they are the four <see cref="ClrEnumUtils"/> actually reads — measured, not
    /// guessed. Every one of them is about a <em>row</em>: the type factory that decides what a value is, the
    /// conformance a condition is translated under, the translator that turns a linq4j tree into a CLR one,
    /// and the correlation variables a sub-query reads its outer row by. None of them is about a sequence,
    /// which is why the row helpers can take this rather than the cursor convention's implementor, and
    /// stay in the namespace that mirrors Calcite's <c>adapter.enumerable</c> where <c>EnumUtils</c> is.
    /// </remarks>
    interface IClrRelImplementor
    {

        /// <summary>
        /// Gets the type factory, which decides what every field value is.
        /// </summary>
        JavaTypeFactory TypeFactory { get; }

        /// <summary>
        /// Gets the SQL conformance the query is being planned under.
        /// </summary>
        SqlConformance Conformance { get; }

        /// <summary>
        /// Gets the table of implementors a translated call is written with.
        /// </summary>
        /// <remarks>
        /// <c>EnumerableRelImplementor.getRexImplementorTable</c>. Everything that hands a Rex expression to
        /// Calcite's translator passes it, so a caller which registered implementors of its own is asked
        /// about them wherever a call is written; the overloads that leave it out resolve from
        /// <c>RexImpTable.INSTANCE</c> and are deprecated upstream.
        /// </remarks>
        RexImplementorTable RexImplementorTable { get; }

        /// <summary>
        /// Gets the translator that turns a linq4j expression into a CLR one.
        /// </summary>
        LixToClrTranslator Translator { get; }

        /// <summary>
        /// Gets the lookup from a correlation variable's name to its getter.
        /// </summary>
        Function1 AllCorrelateVariables { get; }

    }

}
