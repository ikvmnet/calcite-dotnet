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
    /// held in a <c>static final</c>, so a view is described under Calcite's <i>default</i> configuration.
    /// A function the connection asked for therefore works in a query and fails inside a view, which is a
    /// defect rather than a limitation: the same view expands correctly once
    /// <c>ClrPreparingStmt.expandView</c> gets it, so only the <i>description</i> is wrong.
    ///
    /// <para><b>What the configuration reaches, and what it does not.</b>
    /// <c>CalcitePrepareImpl.parse_</c> builds the catalog reader and the validator from
    /// <c>context.config()</c>, so <c>fun</c>, <c>conformance</c>, <c>caseSensitive</c>,
    /// <c>lenientOperatorLookup</c> and <c>defaultNullCollation</c> are what this fixes. It does
    /// <i>not</i> reach the parser: <c>parse_</c> calls <c>createParser(sql)</c> with the default
    /// configuration, exactly as <c>ClrPreparingStmt.ParserConfig</c> does when the view is later
    /// expanded. So the quoting and the casing a <c>lex</c> implies are Calcite's own inside a view
    /// definition whichever macro holds it, and nothing here changes that.</para>
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
    /// <para>Two things register this. A model's <c>"type":"view"</c> gets it from
    /// <c>ClrModelHandler</c>, and <see cref="Create"/> is how a caller registers one itself. A
    /// <c>CREATE VIEW</c> does not: <c>ServerDdlExecutor</c> builds Calcite's macro with no seam to pass a
    /// different one, and it is <c>calcite-server</c>, which nothing shipped here depends on.</para>
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

}
