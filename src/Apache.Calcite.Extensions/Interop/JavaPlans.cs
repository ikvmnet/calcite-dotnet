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
    /// <para>There is one such converter and there is only going to be one.
    /// <c>ClrAsyncEnumerableConvention</c> reads a Calcite sub-plan but never feeds one: a sequence going
    /// that way would have to become a linq4j <c>Enumerator</c>, whose <c>moveNext</c> returns a
    /// <c>boolean</c> with nowhere to await, so it would block once per row. Nothing requires it — Calcite
    /// cannot read an <c>IClrAsyncScannableTable</c> by any route, so no plan can put a Calcite node above
    /// an asynchronous one.</para>
    ///
    /// <para>Separate from <see cref="JavaSequences"/>, which carries a sequence between the two runtimes
    /// and nothing else. This takes a <em>plan</em> — a function of a <see cref="DataContext"/> — and
    /// invoking one is a different job from adapting what it returns. It lived there because that is where
    /// the adapter it finishes with lives, which is not a reason.</para>
    /// </remarks>
    static class JavaPlans
    {

        /// <summary>
        /// Runs a compiled plan of the <c>ClrEnumerableConvention</c> calling convention and reads its rows
        /// as a linq4j sequence.
        /// </summary>
        /// <param name="plan">The sub-plan, stashed by the converter that emitted this call. It compiles
        /// itself the first time it is run, rather than during planning.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns></returns>
        public static org.apache.calcite.linq4j.Enumerable Bind(ClrPlan<IEnumerable> plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);

            return JavaSequences.ToJava(System.Linq.Enumerable.Cast<object>(plan.Invoke(root)));
        }

    }

}
