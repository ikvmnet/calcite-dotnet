using System;
using System.Data;
using System.Data.Common;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet.Utils
{

    /// <summary>
    /// Reads one row from a <see cref="DbDataReader"/> and returns it as an <c>object[]</c>
    /// array aligned to the projected field list.
    /// </summary>
    /// <remarks>
    /// This class implements the Calcite <c>Function0</c> interface so it can be called
    /// repeatedly by the enumeration loop to produce successive rows.
    /// </remarks>
    public class ObjectArrayRowBuilder : Function0
    {

        readonly DbDataReader _reader;
        readonly List _fields;

        /// <summary>
        /// Initializes a new instance of <see cref="ObjectArrayRowBuilder"/>.
        /// </summary>
        /// <param name="reader">The open <see cref="DbDataReader"/> positioned before the first row.</param>
        /// <param name="fields">The Calcite <c>RelDataTypeField</c> list that defines the projected columns.</param>
        /// <exception cref="ArgumentNullException"><paramref name="reader"/> or <paramref name="fields"/> is <see langword="null"/>.</exception>
        public ObjectArrayRowBuilder(DbDataReader reader, List fields)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _fields = fields ?? throw new ArgumentNullException(nameof(fields));
        }

        /// <inheritdoc />
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
        /// Override this method to implement value retrieval.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        object? GetValue(RelDataTypeField field)
        {
            return AdoReaderUtil.GetDbReaderValue(_reader, field.getIndex(), field.getType());
        }

    }

}
