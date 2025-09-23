namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Implements the <see cref="AdoMetadataProvider"/> for the Microsoft SQL Server product.
    /// </summary>
    class SqlServerMetadataProvider : AdoMetadataProvider
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dataSource"></param>
        public SqlServerMetadataProvider(DbDataSource dataSource) :
            base(dataSource)
        {

        }

        /// <inheritdoc />
        public override IReadOnlyList<AdoSchemaMetadata> GetSchemas(string databaseName)
        {
            ArgumentNullException.ThrowIfNull(databaseName);

            using var cnn = DataSource.OpenConnection() as SqlConnection;
            if (cnn is null)
                throw new NotSupportedException("SqlMetadataProvider only supports SqlConnection.");

            // switch to target database
            cnn.ChangeDatabase(databaseName);

            using var result = cnn.GetSchema("Tables");
            var list = new List<AdoSchemaMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName)
                    list.Add(new AdoSchemaMetadata((string)row["TABLE_NAME"]));

            return list;
        }

        /// <inheritdoc />
        public override string GetDefaultDatabase()
        {
            "";
        }

        /// <inheritdoc />
        public override string GetDefaultSchema()
        {
            return "dbo";
        }

        /// <inheritdoc />
        public override IReadOnlyList<AdoTableMetadata> GetTables(string databaseName, string schemaName)
        {
            ArgumentNullException.ThrowIfNull(databaseName);
            ArgumentNullException.ThrowIfNull(schemaName);

            using var cnn = DataSource.OpenConnection() as SqlConnection;
            if (cnn is null)
                throw new NotSupportedException("SqlMetadataProvider only supports SqlConnection.");

            // switch to target database
            cnn.ChangeDatabase(databaseName);

            using var result = cnn.GetSchema("Tables");
            var list = new List<AdoTableMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName && (string)row["TABLE_SCHEMA"] == schemaName)
                    list.Add(new AdoTableMetadata((string)row["TABLE_NAME"]));

            return list;
        }

        /// <inheritdoc />
        public override IReadOnlyList<AdoFieldMetadata> GetFields(string databaseName, string schemaName, string tableName)
        {
            ArgumentNullException.ThrowIfNull(databaseName);
            ArgumentNullException.ThrowIfNull(schemaName);
            ArgumentNullException.ThrowIfNull(tableName);

            using var cnn = DataSource.OpenConnection() as SqlConnection;
            if (cnn is null)
                throw new NotSupportedException("SqlMetadataProvider only supports SqlConnection.");

            // switch to target database
            cnn.ChangeDatabase(databaseName);

            // retrieve the Columns schema object to return as list of fields.
            using var result = cnn.GetSchema("Columns");
            var list = new List<AdoFieldMetadata>();
            foreach (DataRow row in result.Rows)
                if ((string)row["TABLE_CATALOG"] == databaseName && (string)row["TABLE_SCHEMA"] == schemaName && (string)row["TABLE_NAME"] == tableName)
                    list.Add(new AdoFieldMetadata(
                        (string)row["COLUMN_NAME"],
                        ToDbType((string)row["DATA_TYPE"]),
                        (int?)row["CHARACTER_MAXIMUM_LENGTH"],
                        (int?)row["NUMERIC_PRECISION"],
                        (int?)row["NUMERIC_SCALE"],
                        (string)row["IS_NULLABLE"] == "YES"
                    ));

            return list;
        }

        /// <summary>
        /// Returns a <see cref="DbType"/> based on the given type name.
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        DbType ToDbType(string typeName)
        {
            return typeName switch
            {
                "int" => DbType.Int32,
                "char" => DbType.AnsiStringFixedLength,
                "varchar" => DbType.AnsiString,
                "nchar" => DbType.StringFixedLength,
                "nvarchar" => DbType.String,
                _ => throw new NotImplementedException($"Unsupported field type: {typeName}"),
            };
        }

    }

}
