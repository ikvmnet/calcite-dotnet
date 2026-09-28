using System.Collections.Generic;

using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Describes a database reached through ADO.NET: its schemas, tables and columns, the SQL dialect it speaks,
    /// and how its driver names parameters.
    /// </summary>
    /// <remarks>
    /// The adapter provides implementations for SQL Server, SQLite, ODBC and OLE DB, chosen by
    /// <see cref="AdoDatabaseMetadataFactoryImpl"/>. Derive from this class to support another provider, and pass
    /// the instance to an <see cref="AdoSchema"/> <c>Create</c> overload or name its type in the
    /// <c>adoDatabaseMetadata</c> model operand. The adapter calls these members while planning, and may call
    /// them more than once.
    /// </remarks>
    public abstract class AdoDatabaseMetadata
    {

        /// <summary>
        /// Returns the database a schema uses when none is named, or <see langword="null"/> where the provider has
        /// no such notion.
        /// </summary>
        /// <returns>The database name, or <see langword="null"/>.</returns>
        public abstract string? GetDefaultDatabase();

        /// <summary>
        /// Returns the schema a schema uses when none is named, or <see langword="null"/> where the provider has
        /// no such notion.
        /// </summary>
        /// <returns>The schema name, or <see langword="null"/>.</returns>
        public abstract string? GetDefaultSchema();

        /// <summary>
        /// Gets the dialect SQL is generated in for this database.
        /// </summary>
        /// <remarks>
        /// Read when the schema is created and whenever a rule asks, so an implementation that has to query the
        /// server for it should compute it once.
        /// </remarks>
        public abstract SqlDialect Dialect { get; }

        /// <summary>
        /// Gets how the driver names a query parameter, and any rewrite the generated statement needs.
        /// </summary>
        /// <remarks>
        /// Defaults to parameters named <c>@P0</c>, <c>@P1</c> and so on, with no rewrite. Override where the driver
        /// expects something else.
        /// </remarks>
        public virtual IAdoSqlSyntax Syntax => DefaultSyntax;

        /// <summary>
        /// The syntax <see cref="Syntax"/> returns unless overridden: parameters named <c>@P0</c>, <c>@P1</c> and so
        /// on, and the statement left as Calcite generated it.
        /// </summary>
        static readonly IAdoSqlSyntax DefaultSyntax = new AtPrefixed();

        /// <summary>
        /// The syntax with every member left at the interface's default.
        /// </summary>
        sealed class AtPrefixed : IAdoSqlSyntax
        {

        }

        /// <summary>
        /// Returns the schemas of a database.
        /// </summary>
        /// <param name="databaseName">The database, or <see langword="null"/> for the connection's
        /// default.</param>
        /// <returns>The schemas.</returns>
        public abstract IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName);

        /// <summary>
        /// Returns the tables of a schema.
        /// </summary>
        /// <param name="databaseName">The database, or <see langword="null"/>. What <see langword="null"/> means is
        /// the implementation's: the connection's default, or for ODBC and OLE DB every database.</param>
        /// <param name="schemaName">The schema, or <see langword="null"/>. What <see langword="null"/> means is the
        /// implementation's: the default schema, or for ODBC and OLE DB every schema.</param>
        /// <returns>The tables.</returns>
        public abstract IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName);

        /// <summary>
        /// Returns the columns of a table.
        /// </summary>
        /// <param name="databaseName">The table's database, or <see langword="null"/>.</param>
        /// <param name="schemaName">The table's schema, or <see langword="null"/>.</param>
        /// <param name="tableName">The table's name.</param>
        /// <returns>The columns. The adapter builds the row type in the order the set enumerates them, so it should
        /// enumerate them in the table's column order.</returns>
        public abstract IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName);


    }

}
