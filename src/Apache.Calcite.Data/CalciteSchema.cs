using System;

using java.util;
using java.util.function;

using org.apache.calcite.schema;
using org.apache.calcite.util;
using org.apache.calcite.schema.lookup;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Schema.
    /// </summary>
    public abstract class CalciteSchema
    {

        readonly CalciteSchema? _parent;
        readonly Schema _schema;
        readonly string _name;

        readonly NameMap _tableMap;
        readonly LazyReference _tables = new LazyReference();
        readonly NameMultimap _functionMap;
        readonly NameMap _typeMap;
        readonly NameMap _latticeMap;
        readonly NameSet _functionNames;
        readonly NameMap _nullaryFunctionMap;
        readonly NameMap _subSchemaMap;
        readonly LazyReference _subSchemas = new LazyReference();
        readonly List? _path;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parent"></param>
        /// <param name="schema"></param>
        /// <param name="name"></param>
        /// <param name="subSchemaMap"></param>
        /// <param name="tableMap"></param>
        /// <param name="latticeMap"></param>
        /// <param name="typeMap"></param>
        /// <param name="functionMap"></param>
        /// <param name="functionNames"></param>
        /// <param name="nullaryFunctionMap"></param>
        /// <param name="path"></param>
        protected CalciteSchema(CalciteSchema? parent, Schema schema, string name, NameMap? subSchemaMap, NameMap? tableMap, NameMap? latticeMap, NameMap? typeMap, NameMultimap? functionMap, NameSet? functionNames, NameMap? nullaryFunctionMap, List? path)
        {
            _parent = parent;
            _schema = schema ?? throw new ArgumentNullException(nameof(schema));
            _name = name ?? throw new ArgumentNullException(nameof(name));
            _subSchemaMap = subSchemaMap ?? new NameMap();
            _tableMap = tableMap ?? new NameMap();
            _latticeMap = latticeMap ?? new NameMap();
            _typeMap = typeMap ?? new NameMap();
            _path = path;

            if (functionMap is null)
            {
                _functionMap = new NameMultimap();
                _functionNames = new NameSet();
                _nullaryFunctionMap = new NameMap();
            }
            else
            {
                _functionMap = functionMap;
                _functionNames = functionNames ?? throw new ArgumentNullException(nameof(functionNames));
                _nullaryFunctionMap = nullaryFunctionMap ?? throw new ArgumentNullException(nameof(nullaryFunctionMap));
            }
        }

        public Lookup Tables => _tables
            .getOrCompute(new DelegateSupplier<object>(() =>
                Lookup.concat(Lookup.of(_tableMap), _schema.tables().map()))

    }

}
