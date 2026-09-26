using System;
using System.Collections;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Runs a compiled plan of one of the CLR conventions for a caller on the Java side.
    /// </summary>
    /// <remarks>
    /// What a converter <em>out of</em> one of these conventions leaves behind. Calcite compiles its side
    /// with Janino from generated source, and generated source cannot mention an object, so the converter
    /// compiles the sub-plan to a delegate, stashes it on the <see cref="DataContext"/>, and emits a call to
    /// the static below. At run time the generated Java calls it with the stashed delegate and the context,
    /// and gets back a linq4j <c>Enumerable</c>.
    ///
    /// <para>There is one such converter per convention that can be read from Calcite's side, and each
    /// arrives here synchronously whatever the caller asked for, because there is only one boundary where
    /// Janino is on the other side: a sub-plan handed <em>to</em> Calcite has to become a linq4j
    /// <c>Enumerator</c>, whose <c>moveNext</c> returns a <c>boolean</c> with nowhere to await, and the
    /// generated source it would be called from cannot await either. <see cref="Bind"/> is the sequence
    /// convention's and <see cref="BindCursor"/> the cursor convention's, whose open is synchronous for
    /// the same reason.</para>
    ///
    /// <para>That is not a claim that the asynchronous convention has no converter out.
    /// reading an awaiting node's rows synchronously is one, and it blocks a thread per row -- but both
    /// its sides are <see cref="System.Linq.Expressions"/>, so it splices rather than compiling separately
    /// and leaves nothing here to call back into.</para>
    ///
    /// <para>Separate from <see cref="JavaSequences"/>, which carries a sequence between the two runtimes
    /// and nothing else. This takes a <em>plan</em> — a function of a <see cref="DataContext"/> — and
    /// invoking one is a different job from adapting what it returns. It lived there because that is where
    /// the adapter it finishes with lives, which is not a reason.</para>
    ///
    /// <para><b>Public, and the plan arrives as an <see cref="object"/>, and both are what the generated
    /// source can reach.</b> IKVM exposes an <c>internal</c> class to Java as package-private, and Janino
    /// silently drops a candidate whose declaring class the generated class cannot access — measured: the
    /// same signature on a public class compiles, on this class as it was it did not, and the error names
    /// the method it refused as its one candidate. And the plan cannot be declared by its own type, because
    /// linq4j writes every <c>$</c> in a class name as <c>.</c> — <c>Types.className</c>, so that a nested
    /// class reads <c>Outer.Inner</c> — while IKVM mangles a generic instantiation with <c>$$</c>, so
    /// <c>ClrPlan&lt;IEnumerable&gt;</c> arrives as <c>ClrPlan..00601_...</c> and Janino stops at the
    /// second dot. So the converter stashes the plan as an <c>Object</c> and the cast is made here, on the
    /// side where a cast is a cast. Neither had ever been exercised: no test ran a node of a Clr convention
    /// under a node of Calcite's until the cursor convention's suite did.</para>
    /// </remarks>
    public static class JavaPlans
    {

        /// <summary>
        /// Runs a compiled plan of the <c>ClrEnumerableConvention</c> calling convention and reads its rows
        /// as a linq4j sequence.
        /// </summary>
        /// <param name="plan">The sub-plan, stashed by the converter that emitted this call. It compiles
        /// itself the first time it is run, rather than during planning. A <c>ClrPlan&lt;IEnumerable&gt;</c>,
        /// arriving untyped for the reason the class remarks give.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns></returns>
        public static org.apache.calcite.linq4j.Enumerable Bind(object plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);

            return JavaSequences.ToJava(System.Linq.Enumerable.Cast<object>(((ClrPlan<IEnumerable>)plan).Invoke(root)));
        }

        /// <summary>
        /// Runs a compiled plan of the <c>ClrDataCursorConvention</c> calling convention and reads its rows
        /// as a linq4j sequence.
        /// </summary>
        /// <param name="plan">The sub-plan, stashed by the converter that emitted this call. It compiles
        /// itself the first time it is run, rather than during planning. A <c>ClrPlan&lt;ClrDataCursor&gt;</c>,
        /// arriving untyped for the reason the class remarks give.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns></returns>
        /// <remarks>
        /// The sequence's <c>enumerator()</c> is the plan's synchronous open, for the reason the class
        /// remarks give: nothing on Calcite's side of the boundary can await.
        /// </remarks>
        public static org.apache.calcite.linq4j.Enumerable BindCursor(object plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);

            return JavaCursors.ToJava((ClrPlan<ClrDataCursor>)plan, root);
        }

    }

}
