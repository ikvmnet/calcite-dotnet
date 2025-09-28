using System.Data;
using System.Data.Common;

using org.apache.calcite.avatica;

namespace Apache.Calcite.Adapter.Ado.Utils
{

    /// <summary>
    /// Row builder that shifts DATE, TIME and TIMESTAMP values into local time zone.
    /// </summary>
    class ObjectArrayRowBuilder2 : ObjectArrayRowBuilder1
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="reps"></param>
        /// <param name="types"></param>
        public ObjectArrayRowBuilder2(DbDataReader reader, ColumnMetaData.Rep[] reps, DbType[] types) :
            base(reader, reps, types)
        {

        }

    }

}
