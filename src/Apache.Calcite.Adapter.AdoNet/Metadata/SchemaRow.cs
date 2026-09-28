using System;
using System.Data;
using System.Globalization;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Reads values from a row of a <see cref="System.Data.Common.DbConnection.GetSchema(string)"/> collection,
    /// converting rather than unboxing.
    /// </summary>
    /// <remarks>
    /// Drivers carry the same column in different CLR types: SQL Server's <c>NUMERIC_PRECISION</c> is a
    /// <see cref="byte"/> and its <c>NUMERIC_SCALE</c> an <see cref="int"/>; OLE DB's
    /// <c>CHARACTER_MAXIMUM_LENGTH</c> is a <see cref="decimal"/> and its <c>NUMERIC_SCALE</c> a <see cref="short"/>;
    /// ODBC's <c>DECIMAL_DIGITS</c> is a <see cref="short"/>. <see cref="DataRowExtensions.Field{T}(DataRow, string)"/>
    /// unboxes and throws on any width but the one asked for. The values are counts and codes, so converting them
    /// is exact. A column the collection does not have reads as <see langword="null"/>.
    /// </remarks>
    static class SchemaRow
    {

        /// <summary>
        /// Reads a count or a type code, whatever integral type the driver used.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="columnName">The column.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static int? Int32(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToInt32(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a flags word, whatever integral type the driver used.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="columnName">The column.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static long? Int64(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToInt64(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a name.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="columnName">The column.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static string? String(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a flag stated as a <see cref="bool"/> or as <c>YES</c>/<c>NO</c> text. Any text but <c>YES</c> reads
        /// as <see langword="false"/>.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="columnName">The column.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        public static bool? Boolean(DataRow row, string columnName)
        {
            return Value(row, columnName) switch
            {
                null => null,
                bool b => b,
                string s => s.Equals("YES", StringComparison.OrdinalIgnoreCase),
                object value => Convert.ToBoolean(value, CultureInfo.InvariantCulture),
            };
        }

        /// <summary>
        /// Returns the value of a column, or <see langword="null"/> where the column is absent from the
        /// collection or holds no value.
        /// </summary>
        /// <param name="row">The row.</param>
        /// <param name="columnName">The column.</param>
        /// <returns>The value, or <see langword="null"/>.</returns>
        /// <remarks>
        /// A missing column does not throw, because which columns a collection has is up to the driver: ODBC's
        /// <c>Columns</c> collection has no <c>NUMERIC_PRECISION</c>.
        /// </remarks>
        static object? Value(DataRow row, string columnName)
        {
            if (row.Table.Columns.Contains(columnName) == false)
                return null;

            var value = row[columnName];
            return value is null || value == DBNull.Value ? null : value;
        }

    }

}
