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
    /// Implements the <see cref="AdoDatabaseMetadata"/> for the Microsoft Sqlite driver.
    /// </summary>
    partial class SqliteDatabaseMetadata : AdoDatabaseMetadata
    {

        [GeneratedRegex(".*(INT|BOOL).*", RegexOptions.Compiled)]
        private static partial Regex GetIntegerRegex();

        [GeneratedRegex(".*(CHAR|CLOB|TEXT|BLOB).*", RegexOptions.Compiled)]
        private static partial Regex GetVarcharRegex();

        [GeneratedRegex(".*(REAL|FLOA|DOUB|DEC|NUM).*", RegexOptions.Compiled)]
        private static partial Regex GetFloatRegex();

        /// <summary>
        /// Parses the 'table_xinfo' output for a field into an <see cref="AdoFieldMetadata"/>.
        /// </summary>
        /// <param name="name"></param>
        /// <param name="dataType"></param>
        /// <param name="notNull"></param>
        /// <returns></returns>
        static AdoFieldMetadata ParseField(string name, string dataType, string notNull)
        {
            int nullable = 2;
            if (notNull != null)
                nullable = notNull.Equals("0") ? 1 : 0;

            int size = 2000000000;
            int prec = 10;

            /*
             * improved column types
             * ref https://www.sqlite.org/datatype3.html - 2.1 Determination Of Column Affinity
             * plus some degree of artistic-license applied
             */
            dataType = dataType == null ? "TEXT" : dataType.ToUpperInvariant();

            DbType dbType;

            // rule #1 + boolean
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


            // try to find an (optional) length/dimension of the column
            int sod = dataType.IndexOf('(');
            if (sod > 0)
            {
                // find end of dimension
                int eod = dataType.IndexOf(')', sod);
                if (eod > 0)
                {
                    string? intPart, decPart;

                    // check for two values (integer part, fraction) divided by comma
                    int sep = dataType.IndexOf(',', sod);
                    if (sep > 0)
                    {
                        intPart = dataType[(sod + 1)..sep];
                        decPart = dataType[(sep + 1)..eod];
                    }
                    // only a single dimension
                    else
                    {
                        intPart = dataType[(sod + 1)..eod];
                        decPart = null;
                    }

                    // try to parse the values
                    try
                    {
                        int integer = int.Parse(intPart.Trim());

                        // parse decimals?
                        if (decPart != null)
                        {
                            prec = int.Parse(decPart.Trim());
                            size = integer + prec; // columns size equals sum of integer and decimal part of dimension
                        }
                        else
                        {
                            prec = 0; // no decimals
                            size = integer; // columns size equals dimension
                        }
                    }
                    catch (FormatException)
                    {
                        // just ignore invalid dimension formats here
                    }
                }

                // "TYPE_NAME" (colType) is without the length/ dimension
                dataType = dataType[..sod].Trim();
            }

            return new AdoFieldMetadata(name, dbType, size, prec, null, nullable == 1);
        }

        readonly DbDataSource _dbDataSource;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dbDataSource"></param>
        public SqliteDatabaseMetadata(DbDataSource dbDataSource)
        {
            _dbDataSource = dbDataSource ?? throw new ArgumentNullException(nameof(dbDataSource));
        }

        /// <inheritdoc />
        public override string? GetDefaultDatabase()
        {
            return null;
        }

        /// <inheritdoc />
        public override string? GetDefaultSchema()
        {
            return null;
        }

        /// <inheritdoc />
        public override SqlDialect GetDialect()
        {
            return SqliteSqlDialect.DEFAULT;
        }

        /// <inheritdoc />
        public override IReadOnlySet<AdoSchemaMetadata> GetSchemas(string? databaseName)
        {
            if (databaseName is not null)
                throw new ArgumentException("Sqlite does not support multiple databases.", nameof(databaseName));

            return new HashSet<AdoSchemaMetadata>();
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
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
