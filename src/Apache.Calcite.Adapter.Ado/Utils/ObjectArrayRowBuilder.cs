using System;
using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Adapter.Ado.Utils
{

    /// <summary>
    /// Builds that calls <see cref="DbDataReader"/>
    /// </summary>
    abstract class ObjectArrayRowBuilder : Function0
    {

        protected readonly DbDataReader _reader;
        protected readonly int _columnCount;
        protected readonly ColumnMetaData.Rep[] _reps;
        protected readonly DbType[] _types;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="reps"></param>
        /// <param name="types"></param>
        /// <exception cref="AdoSchemaException"></exception>
        public ObjectArrayRowBuilder(DbDataReader reader, ColumnMetaData.Rep[] reps, DbType[] types)
        {
            _reader = reader ?? throw new ArgumentNullException(nameof(reader));
            _reps = reps ?? throw new ArgumentNullException(nameof(reps));
            _types = types ?? throw new ArgumentNullException(nameof(types));

            try
            {
                _columnCount = _reader.FieldCount;
            }
            catch (NotSupportedException e)
            {
                throw new AdoSchemaException("The data reader does not support reading the number of columns.", e);
            }
        }

        /// <inheritdoc />
        public object apply()
        {
            try
            {
                var values = new object?[_columnCount];
                for (int i = 0; i < _columnCount; i++)
                    values[i] = GetValue(i);

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
        /// <param name="i"></param>
        /// <returns></returns>
        protected abstract object? GetValue(int i);

    }

}
