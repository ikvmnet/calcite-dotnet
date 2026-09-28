using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text.RegularExpressions;

using org.apache.calcite.sql;
using org.apache.calcite.sql.dialect;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// The metadata for SQLite through <c>Microsoft.Data.Sqlite</c>.
    /// </summary>
    /// <remarks>
    /// SQLite has one database and no schemas, so every member refuses a database or schema name. Tables are the
    /// <c>sqlite_master</c> rows of type <c>table</c>, and columns come from <c>PRAGMA table_xinfo</c>, typed by the
    /// declared type name's affinity.
    /// </remarks>
    partial class SqliteDatabaseMetadata : AdoDatabaseMetadata
    {

        [GeneratedRegex(".*(INT|BOOL).*", RegexOptions.Compiled)]
        private static partial Regex GetIntegerRegex();

        [GeneratedRegex(".*(CHAR|CLOB|TEXT|BLOB).*", RegexOptions.Compiled)]
        private static partial Regex GetVarcharRegex();

        [GeneratedRegex(".*(REAL|FLOA|DOUB|DEC|NUM).*", RegexOptions.Compiled)]
        private static partial Regex GetFloatRegex();

        /// <summary>
        /// Builds the column metadata from a row of <c>PRAGMA table_xinfo</c>.
        /// </summary>
        /// <param name="name">The column's name.</param>
        /// <param name="dataType">The declared type, such as <c>VARCHAR(20)</c>.</param>
        /// <param name="notNull"><c>0</c> where the column is nullable.</param>
        /// <returns>The column metadata.</returns>
        /// <remarks>
        /// The type follows SQLite's affinity rules loosely: a name containing <c>INT</c> or <c>BOOL</c> is
        /// <see cref="DbType.Int64"/>; <c>CHAR</c>, <c>CLOB</c>, <c>TEXT</c> or <c>BLOB</c> is
        /// <see cref="DbType.String"/>; <c>REAL</c>, <c>FLOA</c>, <c>DOUB</c>, <c>DEC</c> or <c>NUM</c> is
        /// <see cref="DbType.Single"/>; anything else is <see cref="DbType.String"/>. A parenthesised size, and a
        /// scale after a comma, are parsed into the size and precision.
        /// </remarks>
        static AdoFieldMetadata ParseField(string name, string dataType, string notNull)
        {
            int nullable = 2;
            if (notNull != null)
                nullable = notNull.Equals("0") ? 1 : 0;

            int size = 2000000000;
            int prec = 10;

            // loosely https://www.sqlite.org/datatype3.html, "Determination Of Column Affinity"
            dataType = dataType == null ? "TEXT" : dataType.ToUpperInvariant();

            DbType dbType;

            // affinity rule 1, with BOOL added
            if (GetIntegerRegex().IsMatch(dataType))
            {
                dbType = DbType.Int64;
                prec = 0;
            }
            else if (GetVarcharRegex().IsMatch(dataType))
            {
                dbType = DbType.String;
                prec = 0;
            }
            else if (GetFloatRegex().IsMatch(dataType))
            {
                dbType = DbType.Single;
            }
            else
            {
                dbType = DbType.String;
            }


            // an optional "(n)" or "(n, m)" after the type name
            int sod = dataType.IndexOf('(');
            if (sod > 0)
            {
                int eod = dataType.IndexOf(')', sod);
                if (eod > 0)
                {
                    string? intPart, decPart;

                    int sep = dataType.IndexOf(',', sod);
                    if (sep > 0)
                    {
                        intPart = dataType[(sod + 1)..sep];
                        decPart = dataType[(sep + 1)..eod];
                    }
                    else
                    {
                        intPart = dataType[(sod + 1)..eod];
                        decPart = null;
                    }

                    try
                    {
                        int integer = int.Parse(intPart.Trim());

                        if (decPart != null)
                        {
                            prec = int.Parse(decPart.Trim());
                            size = integer + prec;
                        }
                        else
                        {
                            prec = 0;
                            size = integer;
                        }
                    }
                    catch (FormatException)
                    {
                        // an unparseable size keeps the defaults
                    }
                }

                dataType = dataType[..sod].Trim();
            }

            return new AdoFieldMetadata(name, dbType, size, prec, null, nullable == 1);
        }

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource">The data source to read metadata from.</param>
        /// <exception cref="ArgumentNullException"><paramref name="dbDataSource"/> is <see langword="null"/>.</exception>
        public SqliteDatabaseMetadata(DbDataSource dbDataSource)
        {
            _dbDataSource = dbDataSource ?? throw new ArgumentNullException(nameof(dbDataSource));
        }

        /// <summary>
        /// Returns <see langword="null"/>: SQLite has one database.
        /// </summary>
        /// <returns><see langword="null"/>.</returns>
        public override string? GetDefaultDatabase()
        {
            return null;
        }

        /// <summary>
        /// Returns <see langword="null"/>: SQLite has no schemas.
        /// </summary>
        /// <returns><see langword="null"/>.</returns>
        public override string? GetDefaultSchema()
        {
            return null;
        }

        /// <inheritdoc />
        public override SqlDialect Dialect => SqliteSqlDialect.DEFAULT;

        /// <inheritdoc />
        /// <remarks>
        /// Parameters are named <c>$P0</c>, <c>$P1</c> and so on.
        /// </remarks>
        public override IAdoSqlSyntax Syntax { get; } = new SqliteSqlSyntax();

        /// <summary>
        /// Names parameters in the <c>$name</c> form Microsoft.Data.Sqlite binds.
        /// </summary>
        sealed class SqliteSqlSyntax : IAdoSqlSyntax
        {

            /// <inheritdoc />
            public string GetParameterName(int index) => $"$P{index}";

        }

        /// <summary>
        /// Returns an empty set: SQLite has no schemas.
        /// </summary>
        /// <param name="databaseName">Must be <see langword="null"/>.</param>
        /// <returns>An empty set.</returns>
        /// <exception cref="ArgumentException"><paramref name="databaseName"/> is not <see langword="null"/>.</exception>
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            if (databaseName is not null)
                throw new ArgumentException("Sqlite does not support multiple databases.", nameof(databaseName));

            return new HashSet<AdoSchemaMetadata>();
        }

        /// <summary>
        /// Returns the tables listed in <c>sqlite_master</c>.
        /// </summary>
        /// <param name="databaseName">Must be <see langword="null"/>.</param>
        /// <param name="schemaName">Must be <see langword="null"/>.</param>
        /// <returns>The tables, with no database or schema.</returns>
        /// <exception cref="ArgumentException"><paramref name="databaseName"/> or <paramref name="schemaName"/> is not
        /// <see langword="null"/>.</exception>
        public override IReadOnlySet<AdoTableMetadata> GetTables(string? databaseName, string? schemaName)
        {
            if (databaseName is not null)
                throw new ArgumentException("Sqlite does not support multiple databases.", nameof(databaseName));
            if (schemaName is not null)
                throw new ArgumentException("Sqlite does not support schemas.", nameof(schemaName));

            using var cnn = _dbDataSource.CreateConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = @"SELECT name FROM sqlite_master WHERE type = 'table';";

            using var rdr = cmd.ExecuteReader();

            var list = new HashSet<AdoTableMetadata>();
            while (rdr.Read())
                list.Add(new AdoTableMetadata(null, null, rdr.GetString(0)));

            return list;
        }

        /// <summary>
        /// Returns the table's columns from <c>PRAGMA table_xinfo</c>.
        /// </summary>
        /// <param name="databaseName">Must be <see langword="null"/>.</param>
        /// <param name="schemaName">Must be <see langword="null"/>.</param>
        /// <param name="tableName">The table's name, which is written into the pragma between single quotes
        /// without escaping.</param>
        /// <returns>The columns.</returns>
        /// <exception cref="ArgumentException"><paramref name="databaseName"/> or <paramref name="schemaName"/> is not
        /// <see langword="null"/>.</exception>
        public override IReadOnlySet<AdoFieldMetadata> GetFields(string? databaseName, string? schemaName, string tableName)
        {
            if (databaseName is not null)
                throw new ArgumentException("Sqlite does not support multiple databases.", nameof(databaseName));
            if (schemaName is not null)
                throw new ArgumentException("Sqlite does not support schemas.", nameof(schemaName));

            using var cnn = _dbDataSource.CreateConnection();
            cnn.Open();

            using var cmd = cnn.CreateCommand();
            cmd.CommandText = $"PRAGMA table_xinfo('{tableName}')";

            using var rdr = cmd.ExecuteReader();
            var list = new HashSet<AdoFieldMetadata>();
            while (rdr.Read())
                list.Add(ParseField(rdr.GetString("name"), rdr.GetString("type"), rdr.GetString("notnull")));

            return list;
        }


    }

}
