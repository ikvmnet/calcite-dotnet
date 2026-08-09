using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Globalization;

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
                        GetInt32(row, "CHARACTER_MAXIMUM_LENGTH"),
                        GetInt32(row, "NUMERIC_PRECISION"),
                        GetInt32(row, "NUMERIC_SCALE"),
                        row.Field<string>("IS_NULLABLE") == "YES"
                    ));

            return list;
        }

        /// <summary>
        /// Reads a count out of the information schema without insisting on the width the server declared it
        /// in.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <remarks>
        /// The information schema does not type these columns uniformly, and a driver carries the declared
        /// type onto the <see cref="DataTable"/> it builds: SQL Server's <c>NUMERIC_PRECISION</c> is a
        /// <c>tinyint</c> and arrives as a <see cref="byte"/>, its <c>DATETIME_PRECISION</c> and
        /// <c>NUMERIC_PRECISION_RADIX</c> as <see cref="short"/>, while <c>NUMERIC_SCALE</c> and
        /// <c>CHARACTER_MAXIMUM_LENGTH</c> are <see cref="int"/>. <see cref="DataRowExtensions.Field{T}(DataRow, string)"/>
        /// unboxes rather than converts, so it throws on any of them that is not exactly the width asked
        /// for — which is every table with a numeric column, and so every query. Each of these is a small
        /// count whatever it was declared as, so widening it is exact.
        /// </remarks>
        static int? GetInt32(DataRow row, string columnName)
        {
            var value = row[columnName];
            if (value is null || value == DBNull.Value)
                return null;

            return Convert.ToInt32(value, CultureInfo.InvariantCulture);
        }

        /// <summary>
        /// Returns a <see cref="DbType"/> based on the given type name.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        protected abstract DbType ParseDbType(string typeName);

    }

}
