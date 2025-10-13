using System;
using System.Linq;

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
        readonly CalciteConnectionConfigImpl _config;
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
        public CalciteConnectionImpl(CalciteConnection owner, Properties properties, CalciteSchema? rootSchema, JavaTypeFactory? typeFactory)
        {
            _owner = owner ?? throw new ArgumentNullException(nameof(owner));
            _properties = properties ?? throw new ArgumentNullException(nameof(properties));
            _config = new CalciteConnectionConfigImpl(properties);

            if (typeFactory is not null)
            {
                _typeFactory = typeFactory;
            }
            else
            {
                var typeSystem = (RelDataTypeSystem)_config.typeSystem(typeof(RelDataTypeSystem), RelDataTypeSystem.DEFAULT);
                if (_config.conformance().shouldConvertRaggedUnionTypesToVarying())
                    typeSystem = new _DelegatingTypeSystem(typeSystem);

                _typeFactory = new JavaTypeFactoryImpl(typeSystem);
            }

            _rootSchema = rootSchema != null ? rootSchema : CalciteSchema.createRootSchema(true);

            if (_config.conformance().isSupportedDualTable())
            {
                var schemaPlus = _rootSchema.plus();
                schemaPlus.add("DUAL", ViewTable.viewMacro(schemaPlus, "VALUES ('X')", ImmutableList.of(), null, java.lang.Boolean.FALSE));
            }

            if (_rootSchema.Root == false)
                throw new ArgumentException("Must be root schema.", nameof(rootSchema));
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

        /// <summary>
        /// Gets the current configuration.
        /// </summary>
        public CalciteConnectionConfig Config => _config;

    }

}
