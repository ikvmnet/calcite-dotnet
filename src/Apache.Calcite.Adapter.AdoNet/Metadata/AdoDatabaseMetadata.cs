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
    /// time. The adapter uses <see cref="Syntax"/> to generate SQL that is syntactically correct for the
    /// target database.
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
        /// Gets the <see cref="SqlDialect"/> used to generate SQL for this target.
        /// </summary>
        public abstract SqlDialect Dialect { get; }

        /// <summary>
        /// Gets how this driver names a query parameter.
        /// </summary>
        /// <remarks>
        /// Defaults to <c>@P0</c>, which SqlClient, MySql and several others accept. A driver that spells it
        /// differently overrides this.
        /// </remarks>
        public virtual IAdoSqlSyntax Syntax => DefaultSyntax;

        /// <summary>
        /// The naming every driver that does not say otherwise gets.
        /// </summary>
        static readonly IAdoSqlSyntax DefaultSyntax = new AtPrefixed();

        /// <summary>
        /// Takes the interface's own default and adds nothing.
        /// </summary>
        sealed class AtPrefixed : IAdoSqlSyntax
        {

        }

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


    }

}
