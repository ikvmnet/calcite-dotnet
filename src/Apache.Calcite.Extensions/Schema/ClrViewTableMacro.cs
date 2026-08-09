using System;
using System.Collections.Generic;

using org.apache.calcite.jdbc;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Extensions.Schema
{

    /// <summary>
    /// A view macro that analyzes its definition against <b>this</b> connection's configuration.
    /// </summary>
    /// <remarks>
    /// <c>ViewTableMacro.apply</c> calls
    /// <c>Schemas.analyzeView(MaterializedViewTable.MATERIALIZATION_CONNECTION, ...)</c>, and
    /// <c>Schemas.makeContext</c> takes the configuration, the type factory and the data context from
    /// whatever connection it is handed. That one is a <c>DriverManager.getConnection("jdbc:calcite:")</c>
    /// held in a <c>static final</c>, so a view is described under Calcite's <i>default</i> configuration —
    /// <c>fun</c>, <c>lex</c>, <c>conformance</c> and the rest of the connection string are invisible to it.
    /// A function the connection asked for therefore works in a query and fails inside a view, which is a
    /// defect rather than a limitation: the same view expands correctly once
    /// <c>ClrPreparingStmt.expandView</c> gets it, so only the <i>description</i> is wrong.
    ///
    /// <para><b>The fix is a branch Calcite already has.</b> <c>makeContext</c> reads
    /// <c>CalcitePrepare.Dummy.peek()</c> when the connection is null, and that is this provider's own
    /// pushed context, carrying the real configuration. So this class is <c>apply</c> again with
    /// <see langword="null"/> in place of the connection, and nothing else: <c>viewTable</c> and
    /// <c>modifiableViewTable</c> are the base class's documented extension points, so
    /// <see cref="ViewTable"/> and <see cref="ModifiableViewTable"/> are reused rather than copied.</para>
    ///
    /// <para><c>Dummy.peek()</c> is <c>castNonNull(stack.peek())</c> — a cast the runtime does not check —
    /// so on an empty stack it yields <see langword="null"/> rather than throwing. A macro can be applied
    /// with no context on the stack, and this falls back to the connection Calcite would have used rather
    /// than failing: worse configuration, never worse than upstream.</para>
    ///
    /// <para>Only views registered through <see cref="ClrViewTable"/> get this. A model's
    /// <c>"type":"view"</c> and a <c>CREATE VIEW</c> both build Calcite's macro, from code with no seam to
    /// pass a different one, so they keep upstream's behaviour — see
    /// <c>CalciteViewTests.View_is_analyzed_under_the_default_config_not_the_connections</c>.</para>
    /// </remarks>
    public class ClrViewTableMacro : ViewTableMacro
    {

        /// <summary>
        /// Whether the view was asked to be modifiable.
        /// </summary>
        /// <remarks>
        /// The base class keeps its own copy and makes it <c>private</c>, so this is not a duplicate by
        /// choice. <see langword="null"/> means "modifiable if the analysis says it can be", which is not
        /// the same as <see langword="false"/>.
        /// </remarks>
        readonly java.lang.Boolean? _modifiable;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="schema">The schema the view is declared in.</param>
        /// <param name="viewSql">The view's defining query.</param>
        /// <param name="schemaPath">The path the definition is resolved against, or <see langword="null"/> for the schema's own.</param>
        /// <param name="viewPath">The view's own path, used to detect a view defined in terms of itself.</param>
        /// <param name="modifiable">Whether the view is required to be modifiable, or <see langword="null"/> to allow it.</param>
        public ClrViewTableMacro(CalciteSchema schema, string viewSql, java.util.List? schemaPath, java.util.List? viewPath, java.lang.Boolean? modifiable) :
            base(schema, viewSql, schemaPath, viewPath, modifiable)
        {
            _modifiable = modifiable;
        }

        /// <inheritdoc />
        /// <remarks>
        /// <c>ViewTableMacro.apply</c>, with the connection chosen rather than fixed. Every other step is
        /// upstream's, including the two conditions under which a modifiable view is returned.
        /// </remarks>
        public override TranslatableTable apply(java.util.List arguments)
        {
            // null routes Schemas.makeContext through CalcitePrepare.Dummy.peek(), which is our context;
            // where there is none to peek at, the connection Calcite would have used is still better than
            // failing
            var connection = CalcitePrepare.Dummy.peek() is null
                ? MaterializedViewTable.MATERIALIZATION_CONNECTION
                : null;

            var wantsModifiable = _modifiable is not null && _modifiable.booleanValue();

            var parsed = Schemas.analyzeView(connection, schema, schemaPath, viewSql, viewPath, wantsModifiable);
            var schemaPath1 = schemaPath ?? schema.path(null);

            if ((_modifiable is null || _modifiable.booleanValue()) && parsed.modifiable && parsed.table is not null)
                return modifiableViewTable(parsed, viewSql, schemaPath1, viewPath, schema);

            return viewTable(parsed, viewSql, schemaPath1, viewPath);
        }

    }

    /// <summary>
    /// Creates the view macros this provider registers.
    /// </summary>
    /// <remarks>
    /// <c>ViewTable.viewMacro</c>, answering with a <see cref="ClrViewTableMacro"/>. Registering the result
    /// on a schema is what makes a view: <c>schema.add(name, macro)</c> puts it in the function map, where
    /// a nullary <c>TableMacro</c> is what the catalog reader resolves a table name to when the table map
    /// has nothing.
    /// </remarks>
    public static class ClrViewTable
    {

        /// <summary>
        /// Creates a macro for a view over <paramref name="viewSql"/>.
        /// </summary>
        /// <param name="schema">The schema the view is declared in, and the one it is registered on.</param>
        /// <param name="viewSql">The view's defining query.</param>
        /// <param name="schemaPath">
        /// The path the definition is resolved against. <see langword="null"/> uses the schema's own path,
        /// which is what a view over tables beside it wants; give a path explicitly when the definition
        /// names tables somewhere else by an unqualified name.
        /// </param>
        /// <param name="viewPath">
        /// The view's own path. Calcite compares it against the contexts already on the stack and raises
        /// <c>CyclicDefinitionException</c> on a match, so a view defined in terms of itself is caught
        /// rather than recursing. Passing <see langword="null"/> gives that up.
        /// </param>
        /// <param name="modifiable">
        /// <see langword="true"/> requires a modifiable view and fails the analysis if the definition
        /// cannot be one; <see langword="false"/> refuses one; <see langword="null"/> takes one where the
        /// definition allows it.
        /// </param>
        public static TableMacro ViewMacro(
            SchemaPlus schema,
            string viewSql,
            IEnumerable<string>? schemaPath = null,
            IEnumerable<string>? viewPath = null,
            bool? modifiable = null)
        {
            ArgumentNullException.ThrowIfNull(schema);
            ArgumentNullException.ThrowIfNull(viewSql);

            return new ClrViewTableMacro(
                CalciteSchema.from(schema),
                viewSql,
                ToJavaList(schemaPath),
                ToJavaList(viewPath),
                modifiable is null ? null : java.lang.Boolean.valueOf(modifiable.Value));
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
