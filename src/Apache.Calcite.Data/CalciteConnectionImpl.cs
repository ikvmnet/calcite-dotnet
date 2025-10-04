using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

using com.google.common.collect;

using java.lang;
using java.util;

using org.apache.calcite.adapter.java;
using org.apache.calcite.avatica;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.materialize;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Backend implementation of <see cref="CalciteConnection"/>.
    /// </summary>
    class CalciteConnectionImpl
    {


        /// <summary>
        /// Local implementation.
        /// </summary>
        class _DelegatingTypeSystem : DelegatingTypeSystem
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="typeSystem"></param>
            protected internal _DelegatingTypeSystem(RelDataTypeSystem typeSystem) :
                base(typeSystem)
            {

            }

            /// <inheritdoc />
            public override bool shouldConvertRaggedUnionTypesToVarying()
            {
                return true;
            }

        }

        readonly CalciteConnection _owner;
        readonly Properties _properties;
        readonly CalciteSchema _rootSchema;
        readonly JavaTypeFactory _typeFactory;
        internal CalciteTransaction? _transaction;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="owner"></param>
        /// <param name="properties"></param>
        /// <param name="rootSchema"></param>
        /// <param name="typeFactory"></param>
        public CalciteConnectionImpl(CalciteConnection owner, IReadOnlyDictionary<object, object> properties, CalciteSchema rootSchema, JavaTypeFactory typeFactory)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));

            var info = new Properties();
            foreach (var kvp in properties)
                info.put(kvp.Key, kvp.Value);

            var cfg = new CalciteConnectionConfigImpl(info);

            if (typeFactory is not null)
            {
                _typeFactory = typeFactory;
            }
            else
            {
                var typeSystem = (RelDataTypeSystem)cfg.typeSystem(typeof(RelDataTypeSystem), RelDataTypeSystem.DEFAULT);
                if (cfg.conformance().shouldConvertRaggedUnionTypesToVarying())
                    typeSystem = new _DelegatingTypeSystem(typeSystem);

                _typeFactory = new JavaTypeFactoryImpl(typeSystem);
            }

            _rootSchema = rootSchema != null ? rootSchema : CalciteSchema.createRootSchema(true);

            if (cfg.conformance().isSupportedDualTable())
            {
                var schemaPlus = _rootSchema.plus();
                schemaPlus.add("DUAL", ViewTable.viewMacro(schemaPlus, "VALUES ('X')", ImmutableList.of(), null, java.lang.Boolean.FALSE));
            }

            if (_rootSchema.isRoot() == false)
                throw new ArgumentException("Must be root schema.", nameof(rootSchema));

            // setup connection properties
            _properties = new Properties();
            _properties.put(InternalProperty.CASE_SENSITIVE, cfg.caseSensitive());
            _properties.put(InternalProperty.UNQUOTED_CASING, cfg.unquotedCasing());
            _properties.put(InternalProperty.QUOTED_CASING, cfg.quotedCasing());
            _properties.put(InternalProperty.QUOTING, cfg.quoting());
        }

        /// <summary>
        /// Called after the constructor has completed and the model has been loaded.
        /// </summary>
        void Init()
        {
            var service = MaterializationService.instance();
            foreach (var e in Schemas.getLatticeEntries(_rootSchema).AsEnumerable<CalciteSchema.LatticeEntry>())
            {
                var lattice = e.getLattice();
                foreach (var tile in lattice.computeTiles().AsEnumerable<Lattice.Tile>())
                    service.defineTile(lattice, tile.bitSet(), tile.measures, e.schema, true, true);
            }
        }

    }

}
