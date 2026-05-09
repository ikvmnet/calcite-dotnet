using System.Collections.Generic;

using java.util.concurrent.atomic;

using org.apache.calcite;
using org.apache.calcite.adapter.java;
using org.apache.calcite.linq4j;
using org.apache.calcite.schema;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// A native <see cref="DataContext"/> used at execution time. Provides the schema, type factory,
    /// per-statement variables (current timestamp, cancel flag, timeout), and positional parameter
    /// values addressed by Calcite as <c>?0</c>, <c>?1</c>, ... that Calcite expects.
    /// </summary>
    internal sealed class StatementDataContext : DataContext
    {

        readonly SchemaPlus _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        readonly Dictionary<string, object?> _vars;
        readonly IReadOnlyList<object?> _parameters;

        public StatementDataContext(
            SchemaPlus rootSchema,
            JavaTypeFactory typeFactory,
            AtomicBoolean cancelFlag,
            long queryTimeoutMillis,
            IReadOnlyList<object?> parameters)
        {
            _rootSchema = rootSchema;
            _typeFactory = typeFactory;
            _parameters = parameters ?? System.Array.Empty<object?>();

            var nowUtc = java.lang.System.currentTimeMillis();
            _vars = new Dictionary<string, object?>
            {
                { DataContext.Variable.UTC_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.CURRENT_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.LOCAL_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.SYS_TIMESTAMP.camelName, java.lang.Long.valueOf(nowUtc) },
                { DataContext.Variable.CANCEL_FLAG.camelName, cancelFlag },
                { DataContext.Variable.TIMEOUT.camelName, java.lang.Long.valueOf(queryTimeoutMillis) },
            };
        }

        public SchemaPlus getRootSchema() => _rootSchema;

        public JavaTypeFactory getTypeFactory() => _typeFactory;

        public QueryProvider getQueryProvider() => throw new java.lang.UnsupportedOperationException();

        public object? get(string name)
        {
            if (_vars.TryGetValue(name, out var v))
                return v;

            // Calcite addresses positional dynamic parameters as "?0", "?1", ...
            if (name is { Length: > 1 } && name[0] == '?')
            {
                if (int.TryParse(name.AsSpan(1), out var idx) && idx >= 0 && idx < _parameters.Count)
                    return _parameters[idx];
            }

            return null;
        }

    }

}
