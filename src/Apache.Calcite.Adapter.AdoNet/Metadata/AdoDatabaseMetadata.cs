using System.Collections.Generic;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Abstract class for providing metadata regarding ADO.NET data sources.
    /// </summary>
    public abstract class AdoDatabaseMetadata
    {

        /// <summary>
        /// Gets the current database name.
        /// </summary>
        /// <returns></returns>
        public abstract string? GetDefaultDatabase();

        /// <summary>
        /// Gets the current schema name.
        /// </summary>
        /// <returns></returns>
        public abstract string? GetDefaultSchema();

        /// <summary>
        /// Gets the <see cref="SqlDialect"/> for the data source. This replaces <see cref="SqlDialectFactory"/>, which requires JDBC Metadata.
        /// </summary>
        /// <returns></returns>
        public abstract SqlDialect GetDialect();

        /// <summary>
        /// Gets the set of schemas available within the database.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <returns></returns>
        public abstract IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName);

        /// <summary>
        /// Gets the set of tables available within the schema.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <returns></returns>
        public abstract IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName);

        /// <summary>
        /// Gets the set of fields available within the table.
        /// </summary>
        /// <param name="databaseName"></param>
        /// <param name="schemaName"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public abstract IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName);

        /// <summary>
        /// Gets the parameter name to use for the parameter at index i.
        /// </summary>
        /// <param name="index"></param>
        /// <returns></returns> 
        public abstract string GetParameterName(int index);

    }

}
