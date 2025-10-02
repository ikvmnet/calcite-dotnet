using System;
using System.Data;
using System.Data.Common;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.Ado.Utils
{

    /// <summary>
    /// Builds that calls <see cref="DbDataReader"/>
    /// </summary>
    public class ObjectArrayRowBuilder : Function0
    {

        readonly DbDataReader _reader;
        readonly List _fields;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="fields"></param>
        /// <exception cref="AdoSchemaException"></exception>
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
                throw new AdoSchemaException("Exception while reading a row from the data reader.", e);
            }
        }

        /// <summary>
        /// Override this method to implement value retrieval.
        /// </summary>
        /// <param name="field"></param>
        /// <returns></returns>
        object? GetValue(RelDataTypeField field)
        {
            return AdoUtils.GetDbReaderValue(field.getType(), field.getIndex(), _reader);
        }

    }

}
