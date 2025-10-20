using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoDatabaseMetadata"/> for a basic database that supports the GetSchema collections.
    /// </summary>
    abstract class AdoInformationSchemaDatabaseMetadata<TConnection> : AdoDatabaseMetadata
        where TConnection : DbConnection
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
            using var cnn = (TConnection?)_dbDataSource.OpenConnection();
            if (cnn is null)
                throw new NullReferenceException();

            // return database we connected to
            return cnn.Database;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            using var cnn = (TConnection?)_dbDataSource.OpenConnection();
            if (cnn is null)
                throw new NullReferenceException();

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
            using var cnn = (TConnection?)_dbDataSource.OpenConnection();
            if (cnn is null)
                throw new NullReferenceException();

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

            using var cnn = (TConnection?)_dbDataSource.OpenConnection();
            if (cnn is null)
                throw new NullReferenceException();

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
                        row.Field<string>("COLUMN_NAME") ?? throw new InvalidOperationException(),
                        ParseDbType(row.Field<string>("DATA_TYPE") ?? throw new InvalidOperationException()),
                        row.Field<int?>("CHARACTER_MAXIMUM_LENGTH"),
                        row.Field<int?>("NUMERIC_PRECISION"),
                        row.Field<int?>("NUMERIC_SCALE"),
                        row.Field<string>("IS_NULLABLE") == "YES"
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
