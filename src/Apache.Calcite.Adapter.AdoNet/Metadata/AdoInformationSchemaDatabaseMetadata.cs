using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for a driver whose <c>Tables</c> and <c>Columns</c>
    /// schema collections are the SQL <c>INFORMATION_SCHEMA</c> views.
    /// </summary>
    /// <remarks>
    /// The shape is the driver's choice and not every driver's is this one, so a driver whose collections
    /// are shaped otherwise derives from <see cref="AdoDatabaseMetadata"/> instead:
    /// <see cref="OdbcDatabaseMetadata"/> reads the ODBC catalog, whose columns are <c>TABLE_CAT</c>,
    /// <c>TABLE_SCHEM</c> and a numeric <c>DATA_TYPE</c>, and <see cref="OleDbDatabaseMetadata"/> reads the
    /// OLE DB schema rowsets, which share these names and not their types.
    /// </remarks>
    abstract class AdoInformationSchemaDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        public AdoInformationSchemaDatabaseMetadata(DbDataSource dataSource)
        {
            _dbDataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        }

        /// <inheritdoc />
        public DbDataSource DbDataSource => _dbDataSource;

        /// <inheritdoc />
        public override string? GetDefaultDatabase()
        {
            using var cnn = _dbDataSource.OpenConnection();

            // return database we connected to
            return cnn.Database;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            using var cnn = _dbDataSource.OpenConnection();

            // establish target database
            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);
            else
                databaseName = cnn.Database;

            using var result = cnn.GetSchema("Tables");
            var set = new HashSet<AdoSchemaMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName)
                    set.Add(new AdoSchemaMetadata((string)row["TABLE_SCHEMA"]));

            return set;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName)
        {
            using var cnn = _dbDataSource.OpenConnection();

            // establish target database
            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);
            else
                databaseName = cnn.Database;

            // establish target schema
            if (schemaName is null)
                schemaName = GetDefaultSchema();

            using var result = cnn.GetSchema("Tables");
            var set = new HashSet<AdoTableMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName && (string)row["TABLE_SCHEMA"] == schemaName)
                    set.Add(new AdoTableMetadata((string)row["TABLE_CATALOG"], (string)row["TABLE_SCHEMA"], (string)row["TABLE_NAME"]));

            return set;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName)
        {
            ArgumentNullException.ThrowIfNull(tableName);

            using var cnn = _dbDataSource.OpenConnection();

            // establish target database
            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);
            else
                databaseName = cnn.Database;

            // establish target schema
            if (schemaName is null)
                schemaName = GetDefaultSchema();

            // retrieve the Columns schema object to return as list of fields.
            using var result = cnn.GetSchema("Columns");
            var list = new HashSet<AdoFieldMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName && (string)row["TABLE_SCHEMA"] == schemaName && (string)row["TABLE_NAME"] == tableName)
                    list.Add(new AdoFieldMetadata(
                        SchemaRow.String(row, "COLUMN_NAME") ?? throw new InvalidOperationException(),
                        ParseDbType(SchemaRow.String(row, "DATA_TYPE") ?? throw new InvalidOperationException()),
                        SchemaRow.Int32(row, "CHARACTER_MAXIMUM_LENGTH"),
                        SchemaRow.Int32(row, "NUMERIC_PRECISION"),
                        SchemaRow.Int32(row, "NUMERIC_SCALE"),
                        SchemaRow.Boolean(row, "IS_NULLABLE") ?? true
                    ));

            return list;
        }

        /// <summary>
        /// Returns a <see cref="DbType"/> based on the given type name.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        protected abstract DbType ParseDbType(string typeName);

    }

}
