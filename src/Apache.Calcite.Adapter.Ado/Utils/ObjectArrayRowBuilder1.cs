using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica;

namespace Apache.Calcite.Adapter.Ado.Utils
{

    /// <summary>
    /// Row builder that shifts DATE, TIME and TIMESTAMP values into local time zone.
    /// </summary>
    class ObjectArrayRowBuilder1 : ObjectArrayRowBuilder
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="reps"></param>
        /// <param name="types"></param>
        public ObjectArrayRowBuilder1(DbDataReader reader, ColumnMetaData.Rep[] reps, DbType[] types) :
            base(reader, reps, types)
        {

        }

        /// <summary>
        /// Gets the value of the reader at the current position.
        /// </summary>
        /// <param name="i"></param>
        /// <returns></returns>
        protected override object? GetValue(int i)
        {
            return AdoUtils.GetDbReaderValue(_reps[i], _types[i], _reader, i);
        }

    }

}
