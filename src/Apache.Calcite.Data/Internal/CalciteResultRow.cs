using System;

using org.apache.calcite.avatica;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Provides a wrapper over a single row in a Calcite result set. Prevents a copy of the row layout into a separate column-oriented
    /// array.
    /// </summary>
    internal readonly struct CalciteResultRow
    {

        readonly CalciteResultColumns _columns;
        readonly Meta.CursorFactory _cursorFactory;
        readonly object? _row;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="result"></param>
        /// <param name="cursorFactory"></param>
        /// <param name="row"></param>
        public CalciteResultRow(CalciteResultColumns result, Meta.CursorFactory cursorFactory, object? row)
        {
            _columns = result;
            _cursorFactory = cursorFactory;
            _row = row;
        }

        /// <summary>
        /// Gets the raw value of the specified column index in the row.
        /// </summary>
        /// <param name="ordinal"></param>
        /// <returns></returns>
        /// <exception cref="IndexOutOfRangeException"></exception>
        /// <exception cref="NullReferenceException"></exception>
        /// <exception cref="NotSupportedException"></exception>
        public CalciteResultValue GetValue(int ordinal)
        {
            var style = _cursorFactory.style;

            if (style == Meta.Style.OBJECT)
            {
                if (ordinal < 0 || ordinal > 1)
                    throw new IndexOutOfRangeException();

                return new CalciteResultValue(_columns.GetSqlType(ordinal), _row);
            }

            if (style == Meta.Style.ARRAY)
            {
                if (_row is null)
                    throw new NullReferenceException();

                return new CalciteResultValue(_columns.GetSqlType(ordinal), ((object[])_row)[ordinal]);
            }

            if (style == Meta.Style.LIST)
            {
                if (_row is null)
                    throw new NullReferenceException();

                return new CalciteResultValue(_columns.GetSqlType(ordinal), ((java.util.List)_row).get(ordinal));
            }

            throw new NotSupportedException($"Cursor style '{style}' is not yet supported.");
        }

    }

}
