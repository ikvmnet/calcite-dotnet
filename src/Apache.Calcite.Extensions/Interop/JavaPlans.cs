using System;
using System.Collections;

using Apache.Calcite.Extensions.Runtime;

using org.apache.calcite;

namespace Apache.Calcite.Extensions.Interop
{

    /// <summary>
    /// Entry points that Calcite's generated Java code calls to run a sub-plan of the
    /// <c>ClrCursorConvention</c> calling convention. Not intended to be called directly.
    /// </summary>
    /// <remarks>
    /// A converter from the cursor convention to <c>EnumerableConvention</c> stashes the sub-plan in the
    /// generated code and emits a call to one of these methods, which opens the sub-plan and returns its rows
    /// as a linq4j <c>Enumerable</c>. The cursor is opened and read synchronously, since neither a linq4j
    /// <c>Enumerator</c> nor the generated code can await.
    ///
    /// <para>The class is public because IKVM exposes an <c>internal</c> class to Java as package private,
    /// and Janino does not consider a method of a class the generated code cannot access. The plan is
    /// passed as <see cref="object"/> because linq4j writes a class name with <c>$</c> replaced by
    /// <c>.</c>, which breaks the name IKVM gives a generic instantiation.</para>
    /// </remarks>
    public static class JavaPlans
    {

        /// <summary>
        /// Returns a linq4j sequence over the rows of a sub-plan of the <c>ClrCursorConvention</c> calling
        /// convention.
        /// </summary>
        /// <param name="plan">The sub-plan the converter stashed, a <c>ClrPlan&lt;IClrCursor&gt;</c>.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <returns>A sequence whose <c>enumerator()</c> opens the sub-plan synchronously.</returns>
        /// <exception cref="ArgumentNullException"><paramref name="plan"/> is <see langword="null"/>.</exception>
        public static org.apache.calcite.linq4j.Enumerable BindCursor(object plan, DataContext root)
        {
            ArgumentNullException.ThrowIfNull(plan);

            return JavaCursors.ToJava((ClrPlan<IClrCursor>)plan, root);
        }

        /// <summary>
        /// <see cref="BindCursor"/> for a sub-plan that reads correlation variables.
        /// </summary>
        /// <param name="plan">The sub-plan the converter stashed, a <c>ClrPlan&lt;IClrCursor&gt;</c>.</param>
        /// <param name="root">The context the query is being run against.</param>
        /// <param name="names">The correlation variables' names.</param>
        /// <param name="rows">The outer rows, one per name, each an <c>Object[]</c> of the row's fields.</param>
        /// <returns>A sequence whose <c>enumerator()</c> opens the sub-plan synchronously.</returns>
        /// <remarks>
        /// Called when the sub-plan runs under Calcite's <c>EnumerableCorrelate</c>. The generated code reads
        /// the outer row's fields and passes them here, and the sub-plan reads them back through a
        /// <see cref="ClrCorrelationDataContext"/>.
        /// </remarks>
        public static org.apache.calcite.linq4j.Enumerable BindCursorCorrelated(object plan, DataContext root, string[] names, object[] rows)
        {
            return BindCursor(plan, new ClrCorrelationDataContext(root, names, rows));
        }

    }

}
