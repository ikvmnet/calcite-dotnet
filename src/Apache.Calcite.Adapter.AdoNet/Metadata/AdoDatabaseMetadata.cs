using System.Collections.Generic;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes the schema structure of an ADO.NET data source to the Calcite adapter.
    /// </summary>
    /// <remarks>
    /// Implement this class and provide an instance to your <see cref="AdoDataSource"/> so that
    /// the Calcite adapter can discover databases, schemas, tables, and columns at query-planning
    /// time. The adapter also uses <see cref="GetDialect"/> to generate SQL that is syntactically
    /// correct for the target database.
    /// </remarks>
    public abstract class AdoDatabaseMetadata
    {

        /// <summary>
        /// Returns the name of the default database for this data source, or <see langword="null"/> if the
        /// concept does not apply.
        /// </summary>
        public abstract string? GetDefaultDatabase();

        /// <summary>
        /// Returns the name of the default schema within the default database, or <see langword="null"/> if
        /// the concept does not apply.
        /// </summary>
        public abstract string? GetDefaultSchema();

        /// <summary>
        /// Returns the <see cref="SqlDialect"/> that the adapter uses to generate SQL for this data source.
        /// </summary>
        public abstract SqlDialect GetDialect();

        /// <summary>
        /// Returns all schemas available in the specified database.
        /// </summary>
        /// <param name="databaseName">The database to enumerate schemas for, or <see langword="null"/> for the default database.</param>
        /// <returns>A set of <see cref="AdoSchemaMetadata"/> values describing each schema.</returns>
        public abstract IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName);

        /// <summary>
        /// Returns all tables available in the specified schema.
        /// </summary>
        /// <param name="databaseName">The database that contains the schema, or <see langword="null"/> for the default database.</param>
        /// <param name="schemaName">The schema to enumerate tables for, or <see langword="null"/> for the default schema.</param>
        /// <returns>A set of <see cref="AdoTableMetadata"/> values describing each table.</returns>
        public abstract IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName);

        /// <summary>
        /// Returns all columns in the specified table.
        /// </summary>
        /// <param name="databaseName">The database that contains the table, or <see langword="null"/> for the default database.</param>
        /// <param name="schemaName">The schema that contains the table, or <see langword="null"/> for the default schema.</param>
        /// <param name="tableName">The name of the table whose columns to enumerate.</param>
        /// <returns>A set of <see cref="AdoFieldMetadata"/> values describing each column.</returns>
        public abstract IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName);

        /// <summary>
        /// Returns the SQL parameter placeholder name to use for the parameter at the specified zero-based index.
        /// </summary>
        /// <param name="index">The zero-based ordinal of the parameter.</param>
        /// <returns>A parameter name string suitable for inclusion in a SQL statement.</returns>
        public abstract string GetParameterName(int index);

    }

}
