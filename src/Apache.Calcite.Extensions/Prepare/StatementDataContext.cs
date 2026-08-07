using System.Collections.Generic;

using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Extensions.Prepare
{

    /// <summary>
    /// A native <see cref="DataContext"/> used at execution time. Provides the schema, type factory, per-statement variables (current
    /// timestamp, cancel flag, timeout), positional parameter values addressed by Calcite as <c>?0</c>, <c>?1</c>, ..., and any
    /// stashed compile-time values recorded by <c>EnumerableRelImplementor.stash()</c> during planning.
    /// </summary>
    internal sealed class StatementDataContext : DataContext
    {

        readonly SchemaPlus _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        readonly IReadOnlyDictionary<string, object?> _vars;
        readonly IReadOnlyList<object?> _parameters;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="rootSchema"></param>
        /// <param name="typeFactory"></param>
        /// <param name="cancelFlag"></param>
        /// <param name="queryTimeoutMillis"></param>
        /// <param name="parameters">Bound positional query parameters (<c>?0</c>, <c>?1</c>, …).</param>
        /// <param name="internalParameters">
        /// The <c>signature.internalParameters</c> map produced by planning. Contains values stashed by
        /// <c>EnumerableRelImplementor.stash()</c> (e.g. <c>"v0stashed"</c>, <c>"_conformance"</c>) that
        /// the generated <c>bind(root)</c> method retrieves via <c>root.get(name)</c> at execution time.
        /// Pass <see langword="null"/> when constructing a throwaway planning-only context.
        /// </param>
        public StatementDataContext(SchemaPlus rootSchema, JavaTypeFactory typeFactory, AtomicBoolean cancelFlag, long queryTimeoutMillis, IReadOnlyList<object?> parameters, java.util.Map? internalParameters = null)
        {
            _rootSchema = rootSchema;
            _typeFactory = typeFactory;
            _parameters = parameters ?? [];

            var nowUtc = java.lang.System.currentTimeMillis();

            var vars = new Dictionary<string, object?>
            {
                { DataContext.Variable.UTC_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.CURRENT_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.LOCAL_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.SYS_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag },
                { DataContext.Variable.TIMEOUT.camelName, java.lang.Long.valueOf(queryTimeoutMillis) },
            };

            // Merge stashed compile-time values from planning, mirroring
            // CalciteConnectionImpl.enumerable()'s map.putAll(signature.internalParameters).
            if (internalParameters != null && !internalParameters.isEmpty())
            {
                var it = internalParameters.entrySet().iterator();
                while (it.hasNext())
                {
                    var entry = (java.util.Map.Entry)it.next();
                    vars[(string)entry.getKey()] = entry.getValue();
                }
            }

            _vars = vars;
        }

        /// <summary>
        /// Returns the root schema of the current session.
        /// </summary>
        public SchemaPlus getRootSchema() => _rootSchema;

        /// <summary>
        /// Returns the <see cref="JavaTypeFactory"/> used to create Java type representations.
        /// </summary>
        public JavaTypeFactory getTypeFactory() => _typeFactory;

        /// <summary>
        /// Returns <see cref="org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER"/>, which implements
        /// <c>executeQuery(queryable)</c> by delegating directly to <c>queryable.enumerator()</c>.
        /// This mirrors what the Calcite JDBC driver does: <c>CalciteConnectionImpl</c> itself
        /// implements <c>QueryProvider</c> via the same delegation, and <c>org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER</c>
        /// is the non-JDBC equivalent.
        /// </summary>
        public QueryProvider getQueryProvider() => org.apache.calcite.linq4j.Linq4j.DEFAULT_PROVIDER;

        /// <summary>
        /// Returns the value of the named variable, or <see langword="null"/> if not found.
        /// <para>
        /// Well-known per-statement variables (<c>utcTimestamp</c>, <c>currentTimestamp</c>,
        /// <c>localTimestamp</c>, <c>sysTimestamp</c>, <c>cancelFlag</c>, <c>queryTimeout</c>)
        /// are resolved first. Positional dynamic parameters supplied by the caller are then
        /// resolved by the Calcite naming convention <c>?0</c>, <c>?1</c>, …
        /// </para>
        /// </summary>
        /// <param name="name">The variable name as supplied by the Calcite engine.</param>
        /// <returns>The variable value, or <see langword="null"/> if the name is not recognised.</returns>
        public object? get(string name)
        {
            if (_vars.TryGetValue(name, out var v))
                return v;

            // Calcite addresses positional dynamic parameters as "?0", "?1", ...
            if (name is { Length: > 1 } && name[0] == '?')
            {
                if (int.TryParse(name.Substring(1), out var idx) && idx >= 0 && idx < _parameters.Count)
                    return _parameters[idx];
            }

            return null;
        }

    }

}
