using System.Collections.Generic;

using Apache.Calcite.Extensions.Schema;

using org.apache.calcite.jdbc;
using org.apache.calcite.model;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// <see cref="ModelHandler"/> with every <c>"type":"view"</c> registered as a
    /// <see cref="ClrViewTableMacro"/>.
    /// </summary>
    /// <remarks>
    /// A model view is described under Calcite's default configuration rather than the connection's,
    /// because <c>ViewTableMacro.apply</c> analyzes it through
    /// <c>MaterializedViewTable.MATERIALIZATION_CONNECTION</c> — see <see cref="ClrViewTableMacro"/>.
    /// <c>ModelHandler.visit(JsonView)</c> constructs that macro directly, so the only way to change which
    /// one a model produces is to replace what the handler registered.
    ///
    /// <para><b>Why the work happens in the constructor body.</b> <c>ModelHandler</c> does everything in
    /// <i>its</i> constructor: it reads the model and visits it, so by the time this class's constructor
    /// body runs, every schema exists and every view is registered. The visit methods below therefore only
    /// <i>record</i> what they saw — they cannot register anything themselves, because reaching the schema
    /// a view belongs to means <c>currentMutableSchema</c>, which is private, and the root schema is a
    /// private field. Recording during the visit and resolving afterwards, when the root is a parameter in
    /// scope, needs neither. C# runs a derived class's field initialisers before the base constructor, so
    /// the list is there when the first visit arrives.</para>
    ///
    /// <para>The base's <c>visit</c> is still called for each view, so anything else in the model that
    /// depends on the view existing during the traversal — a materialization naming it — sees what it
    /// expects; the macro is swapped afterwards. Registering a macro does not apply it, so neither the
    /// original nor the replacement analyzes anything here.</para>
    /// </remarks>
    sealed class ClrModelHandler : ModelHandler
    {

        readonly List<(string Schema, JsonView View)> _views = [];

        string? _currentSchemaName;

        /// <summary>
        /// Reads <paramref name="uri"/> into <paramref name="rootSchema"/>, then re-registers its views.
        /// </summary>
        /// <param name="rootSchema">The schema the model is applied to.</param>
        /// <param name="uri">The model, as <c>inline:</c> text or a path.</param>
        public ClrModelHandler(SchemaPlus rootSchema, string uri) :
            base(rootSchema, uri)
        {
            foreach (var (schemaName, view) in _views)
                Replace(rootSchema, schemaName, view);
        }

        /// <inheritdoc />
        public override void visit(JsonMapSchema jsonSchema)
        {
            _currentSchemaName = jsonSchema.name;
            base.visit(jsonSchema);
        }

        /// <inheritdoc />
        public override void visit(JsonCustomSchema jsonSchema)
        {
            _currentSchemaName = jsonSchema.name;
            base.visit(jsonSchema);
        }

        /// <inheritdoc />
        public override void visit(JsonView jsonView)
        {
            base.visit(jsonView);

            if (_currentSchemaName is not null)
                _views.Add((_currentSchemaName, jsonView));
        }

        /// <summary>
        /// Swaps the macro <c>ModelHandler</c> registered for one of ours.
        /// </summary>
        /// <remarks>
        /// The two defaults are <c>ModelHandler.visit(JsonView)</c>'s own: a view's path is its schema's
        /// unless the model gave one, and its view path is that with its name on the end — which is what
        /// <c>CalcitePrepare.Dummy.push</c> compares to catch a view defined in terms of itself.
        /// </remarks>
        static void Replace(SchemaPlus rootSchema, string schemaName, JsonView jsonView)
        {
            var schemaPlus = rootSchema.getSubSchema(schemaName);
            if (schemaPlus is null)
                return;

            var path = jsonView.path ?? Singleton(schemaName);

            var viewPath = new java.util.ArrayList();
            viewPath.addAll(path);
            viewPath.add(jsonView.name);

            var schema = CalciteSchema.from(schemaPlus);

            // removeFunction takes every function of the name, which is what ServerDdlExecutor's DROP VIEW
            // does too; the macro going back in its place is the one just removed, re-made
            schema.removeFunction(jsonView.name);
            schemaPlus.add(jsonView.name, new ClrViewTableMacro(schema, jsonView.getSql(), path, viewPath, jsonView.modifiable));
        }

        static java.util.List Singleton(string value)
        {
            var list = new java.util.ArrayList();
            list.add(value);

            return list;
        }

    }

}
