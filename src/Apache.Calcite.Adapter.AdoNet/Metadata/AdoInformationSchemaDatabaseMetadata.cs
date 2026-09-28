using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Base class of the metadata for a driver whose <c>Tables</c> and <c>Columns</c> schema collections have the
    /// shape of the SQL <c>INFORMATION_SCHEMA</c> views, with the type named in <c>DATA_TYPE</c>.
    /// </summary>
    /// <remarks>
    /// Every member opens a connection and reads a schema collection, switching to the named database first with
    /// <see cref="DbConnection.ChangeDatabase"/>. A null database is the connection's own, and a null schema is
    /// <see cref="AdoDatabaseMetadata.GetDefaultSchema"/>. A derived class maps the type names.
    /// </remarks>
    abstract class AdoInformationSchemaDatabaseMetadata : AdoDatabaseMetadata
    {

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource">The data source to read metadata from.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dataSource"/> is <see langword="null"/>.</exception>
        public AdoInformationSchemaDatabaseMetadata(DbDataSource dataSource)
        {
            _dbDataSource = dataSource ?? throw new ArgumentNullException(nameof(dataSource));
        }

        /// <summary>
        /// Gets the data source this metadata describes.
        /// </summary>
        public DbDataSource DbDataSource => _dbDataSource;

        /// <summary>
        /// Returns the database a new connection is in.
        /// </summary>
        /// <returns>The database name.</returns>
        public override string? GetDefaultDatabase()
        {
            using var cnn = _dbDataSource.OpenConnection();

            return cnn.Database;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            using var cnn = _dbDataSource.OpenConnection();

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

            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);
            else
                databaseName = cnn.Database;

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

            if (databaseName is not null)
                cnn.ChangeDatabase(databaseName);
            else
                databaseName = cnn.Database;

            if (schemaName is null)
                schemaName = GetDefaultSchema();

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
        /// Returns the <see cref="DbType"/> for a type name from <c>DATA_TYPE</c>.
        /// </summary>
        /// <param name="typeName">The type name.</param>
        /// <returns>The type.</returns>
        protected abstract DbType ParseDbType(string typeName);

    }

}
