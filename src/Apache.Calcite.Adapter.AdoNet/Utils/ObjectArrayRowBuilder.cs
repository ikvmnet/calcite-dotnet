using System;
using System.Data;
using System.Data.Common;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet.Utils
{

    /// <summary>
    /// A linq4j <see cref="Function0"/> that reads the current row of a <see cref="DbDataReader"/> into an
    /// <c>object[]</c>, one element per field, each in Calcite's representation of the field's type.
    /// </summary>
    /// <remarks>
    /// Each call to <see cref="apply"/> reads the row the reader is positioned on, so one instance serves every
    /// row.
    /// </remarks>
    public class ObjectArrayRowBuilder : Function0
    {

        readonly DbDataReader _reader;
        readonly List _fields;

        /// <summary>
        /// Initializes a new instance of <see cref="ObjectArrayRowBuilder"/>.
        /// </summary>
        /// <param name="reader">The reader to read rows from.</param>
        /// <param name="fields">The <see cref="RelDataTypeField"/>s of the row. Each is read at its own index.</param>
        /// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="fields"/> is <see langword="null"/>.</exception>
        public ObjectArrayRowBuilder(DbDataReader reader, List fields)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        }

        /// <summary>
        /// Reads the current row.
        /// </summary>
        /// <returns>An <c>object[]</c> holding the row's values.</returns>
        /// <exception cref="AdoCalciteException">The reader raised a <see cref="DataException"/>.</exception>
        public object apply()
        {
            try
            {
                var values = new object?[_fields.size()];
                for (int i = 0; i < _fields.size(); i++)
                    values[i] = GetValue((RelDataTypeField)_fields.get(i));

                return values;
            }
            catch (DataException e)
            {
                throw new AdoCalciteException("Exception while reading a row from the data reader.", e);
            }
        }

        /// <summary>
        /// Reads one field of the current row.
        /// </summary>
        /// <param name="field">The field.</param>
        /// <returns>The value in Calcite's representation, or <see langword="null"/>.</returns>
        object? GetValue(RelDataTypeField field)
        {
            return AdoReaderUtil.GetDbReaderValue(_reader, field.getIndex(), field.getType());
        }

    }

}
