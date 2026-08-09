using System;
using System.Collections.Generic;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// Registering things on a <see cref="SchemaPlus"/> from .NET.
    /// </summary>
    public static class SchemaPlusExtensions
    {

        /// <summary>
        /// Adds a view over <paramref name="viewSql"/> to <paramref name="schema"/>, and returns the macro
        /// that was registered.
        /// </summary>
        /// <remarks>
        /// <b>A view registered this way is described under the connection's configuration.</b> One
        /// registered by <c>ViewTable.viewMacro</c> is not: the macro that call produces analyzes the
        /// definition through <c>MaterializedViewTable.MATERIALIZATION_CONNECTION</c>, a process-wide
        /// <c>DriverManager.getConnection("jdbc:calcite:")</c> whose configuration is Calcite's default, so
        /// a function the connection asked for fails inside a view. <see cref="ClrViewTableMacro"/> is that
        /// macro with the connection left null, which is what makes Calcite read the real one. What that
        /// reaches is the catalog reader and the validator — <c>fun</c>, <c>conformance</c>,
        /// <c>caseSensitive</c> and the like — and not the parser, which is Calcite's default for a view
        /// definition either way; its remarks have the detail.
        ///
        /// <para>So <c>ViewTable.viewMacro</c> cannot be called here, and the obstacle is one line: all
        /// three of its overloads end in <c>new ViewTableMacro(...)</c>, naming the class rather than
        /// taking one. Everything else it does is <c>CalciteSchema.from</c> and passing the arguments
        /// through, which is what this method does instead.</para>
        ///
        /// <para><b>The view path is computed rather than asked for.</b> It is the argument a caller has no
        /// reason to think about and every reason to get wrong: Calcite compares it against the contexts on
        /// <c>CalcitePrepare.Dummy</c>'s stack to raise <c>CyclicDefinitionException</c>, so a view defined
        /// in terms of itself is caught rather than recursing until the stack runs out — and passing the
        /// wrong one, or none, quietly gives that up. <c>CalciteSchema.path(name)</c> is the schema's own
        /// path with the name on the end, which is what <c>ModelHandler</c> and <c>ServerDdlExecutor</c>
        /// both build by hand.</para>
        /// </remarks>
        /// <param name="schema">The schema to declare the view in, and register it on.</param>
        /// <param name="name">The name the view is reachable by.</param>
        /// <param name="viewSql">The view's defining query.</param>
        /// <param name="schemaPath">
        /// The path the definition is resolved against. <see langword="null"/> — the usual case — leaves the
        /// macro to use the schema's own, which is what a view over tables beside it wants. Give one where
        /// the definition names tables elsewhere by an unqualified name.
        /// </param>
        /// <param name="modifiable">
        /// <see langword="true"/> requires a modifiable view and fails the analysis if the definition
        /// cannot be one; <see langword="false"/> refuses one; <see langword="null"/> takes one where the
        /// definition allows it.
        /// </param>
        public static TableMacro AddView(
            this SchemaPlus schema,
            string name,
            string viewSql,
            IEnumerable<string>? schemaPath = null,
            bool? modifiable = null)
        {
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(name);
            ArgumentNullException.ThrowIfNull(viewSql);

            var calciteSchema = CalciteSchema.from(schema);

            var macro = new ClrViewTableMacro(
                calciteSchema,
                viewSql,
                ToJavaList(schemaPath),
                calciteSchema.path(name),
                modifiable is null ? null : java.lang.Boolean.valueOf(modifiable.Value));

            schema.add(name, macro);

            return macro;
        }

        static java.util.List? ToJavaList(IEnumerable<string>? values)
        {
            if (values is null)
                return null;

            var list = new java.util.ArrayList();
            foreach (var value in values)
                list.add(value);

            return list;
        }

    }

}
