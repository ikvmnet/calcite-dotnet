using System;
using System.Data;
using System.Globalization;

namespace Apache.Calcite.Adapter.AdoNet.Metadata
{

    /// <summary>
    /// Reads a row of a provider's <see cref="System.Data.Common.DbConnection.GetSchema(string)"/> collection
    /// without insisting on the CLR type the driver chose to carry a value in.
    /// </summary>
    /// <remarks>
    /// A driver builds its schema <see cref="DataTable"/> from whatever its catalog gave it, and the widths
    /// are neither uniform across drivers nor across the columns of one collection: SQL Server's
    /// <c>NUMERIC_PRECISION</c> arrives as a <see cref="byte"/> and its <c>NUMERIC_SCALE</c> as an
    /// <see cref="int"/>; OLE DB's <c>CHARACTER_MAXIMUM_LENGTH</c> is a <see cref="decimal"/> and its
    /// <c>NUMERIC_SCALE</c> a <see cref="short"/>; ODBC's <c>DECIMAL_DIGITS</c> is a <see cref="short"/>.
    /// <see cref="DataRowExtensions.Field{T}(DataRow, string)"/> unboxes rather than converts and throws on
    /// every one of those that is not the width asked for — which is how every query against SQL Server came
    /// to fail. These are counts and codes, so widening one is exact.
    /// </remarks>
    static class SchemaRow
    {

        /// <summary>
        /// Reads a count or a type code, whatever integral width the driver declared it in.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static int? Int32(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToInt32(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a flags word, whatever integral width the driver declared it in.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static long? Int64(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToInt64(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a name.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        public static string? String(DataRow row, string columnName)
        {
            return Value(row, columnName) is object value ? Convert.ToString(value, CultureInfo.InvariantCulture) : null;
        }

        /// <summary>
        /// Reads a flag, whether the driver stated it as a <see cref="bool"/> or as the <c>YES</c> / <c>NO</c>
        /// text the information schema and ODBC both use.
        /// </summary>
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
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
        /// <param name="row"></param>
        /// <param name="columnName"></param>
        /// <returns></returns>
        /// <remarks>
        /// Absent rather than throwing, because which columns a schema collection carries is the driver's
        /// choice: ODBC's <c>Columns</c> collection has no <c>NUMERIC_PRECISION</c> at all, and one driver's
        /// extension columns are another's absence.
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
