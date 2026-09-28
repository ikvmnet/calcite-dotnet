using System;

using org.apache.calcite.avatica;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// The current row of a result, read in place in the shape the plan produced it.
    /// </summary>
    /// <remarks>
    /// The row's shape is the signature's <c>Meta.CursorFactory</c> style: <c>OBJECT</c> for a one-column
    /// result, whose row is the value itself; <c>ARRAY</c> for an <c>object[]</c>; <c>LIST</c> for a
    /// <c>java.util.List</c>.
    /// </remarks>
    internal readonly struct CalciteResultRow
    {

        readonly CalciteResultColumns _columns;
        readonly Meta.CursorFactory _cursorFactory;
        readonly object? _row;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="result">The result's columns.</param>
        /// <param name="cursorFactory">The signature's cursor factory, whose style gives the row's shape.</param>
        /// <param name="row">The row as the plan produced it.</param>
        public CalciteResultRow(CalciteResultColumns result, Meta.CursorFactory cursorFactory, object? row)
        {
            _columns = result;
            _cursorFactory = cursorFactory;
            _row = row;
        }

        /// <summary>
        /// Gets the value of a column in this row.
        /// </summary>
        /// <param name="ordinal">The zero-based column ordinal.</param>
        /// <returns>The cell, carrying the column's type and mapping.</returns>
        /// <exception cref="IndexOutOfRangeException"><paramref name="ordinal"/> is out of range.</exception>
        /// <exception cref="NullReferenceException">The row is null in an <c>ARRAY</c> or <c>LIST</c> style.</exception>
        /// <exception cref="NotSupportedException">The cursor style is not <c>OBJECT</c>, <c>ARRAY</c> or <c>LIST</c>.</exception>
        public CalciteResultValue GetValue(int ordinal)
        {
            var style = _cursorFactory.style;

            if (style == Meta.Style.OBJECT)
            {
                if (ordinal != 0)
                    throw new IndexOutOfRangeException();

                return Value(ordinal, _row);
            }

            if (style == Meta.Style.ARRAY)
            {
                if (_row is null)
                    throw new NullReferenceException();

                return Value(ordinal, ((object[])_row)[ordinal]);
            }

            if (style == Meta.Style.LIST)
            {
                if (_row is null)
                    throw new NullReferenceException();

                return Value(ordinal, ((java.util.List)_row).get(ordinal));
            }

            throw new NotSupportedException($"Cursor style '{style}' is not yet supported.");
        }

        /// <summary>
        /// Builds a cell from the column's type and its mapping, which the columns resolved once.
        /// </summary>
        CalciteResultValue Value(int ordinal, object? value)
        {
            return new CalciteResultValue(_columns.GetRelType(ordinal), _columns.Registry, _columns.GetMapping(ordinal), value);
        }

    }

}
