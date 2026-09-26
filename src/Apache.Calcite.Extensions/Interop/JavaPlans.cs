using System;
using System.Collections;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Runs a compiled plan of the <c>ClrCursorConvention</c> calling convention for a caller on the Java
    /// side.
    /// </summary>
    /// <remarks>
    /// What a converter <em>out of</em> that convention leaves behind. Calcite compiles its side
    /// with Janino from generated source, and generated source cannot mention an object, so the converter
    /// compiles the sub-plan to a delegate, stashes it on the <see cref="DataContext"/>, and emits a call to
    /// the static below. At run time the generated Java calls it with the stashed delegate and the context,
    /// and gets back a linq4j <c>Enumerable</c>.
    ///
    /// <para>The sub-plan arrives here synchronously whatever the caller asked for, because Janino is on
    /// the other side: a sub-plan handed <em>to</em> Calcite has to become a linq4j <c>Enumerator</c>,
    /// whose <c>moveNext</c> returns a <c>boolean</c> with nowhere to await, and the generated source it
    /// would be called from cannot await either. So <see cref="BindCursor"/> opens the cursor
    /// synchronously and reads it with <c>Read</c>.</para>
    ///
    /// <para>Separate from <see cref="JavaCursors"/>, which carries rows between the two runtimes and
    /// nothing else. This takes a <em>plan</em> — a function of a <see cref="DataContext"/> — and invoking
    /// one is a different job from adapting what it returns.</para>
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
    /// side where a cast is a cast.</para>
    /// </remarks>
    public static class JavaPlans
    {

        /// <summary>
        /// Runs a compiled plan of the <c>ClrCursorConvention</c> calling convention and reads its rows
        /// as a linq4j sequence.
        /// </summary>
        /// <param name="plan">The sub-plan, stashed by the converter that emitted this call. It compiles
        /// itself the first time it is run, rather than during planning. A <c>ClrPlan&lt;IClrCursor&gt;</c>,
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

            return JavaCursors.ToJava((ClrPlan<IClrCursor>)plan, root);
        }

        /// <summary>
        /// <see cref="BindCursor"/>, with the outer rows of the correlation variables the plan reads.
        /// </summary>
        /// <param name="plan">The compiled plan, a <c>ClrPlan&lt;IClrCursor&gt;</c>.</param>
        /// <param name="root">The context the plan is bound with.</param>
        /// <param name="names">The correlation variables' names.</param>
        /// <param name="rows">The outer rows, one per name, each an <c>Object[]</c> of the row's fields.</param>
        /// <returns></returns>
        /// <remarks>
        /// What a converter out of the cursor convention calls when its sub-plan sits under a correlate of
        /// Calcite's. The outer row is a parameter of the Java lambda that correlate generates, which the
        /// sub-plan cannot see, so the converter reads its fields where it can — through the getter Calcite
        /// registered — and hands them in through a <see cref="ClrCorrelationDataContext"/>.
        /// </remarks>
        public static org.apache.calcite.linq4j.Enumerable BindCursorCorrelated(object plan, DataContext root, string[] names, object[] rows)
        {
            return BindCursor(plan, new ClrCorrelationDataContext(root, names, rows));
        }

    }

}
