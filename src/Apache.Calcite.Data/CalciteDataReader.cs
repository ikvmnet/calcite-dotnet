using System.Collections.Generic;
using System.Data;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Reads a forward-only stream of rows from a data source.
    /// </summary>
    public class CalciteDataReader : CalciteDataReaderBase, IEnumerable<IDataRecord>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="command"></param>
        /// <param name="statement"></param>
        internal CalciteDataReader(CalciteCommand command, object statement) :
            base(command, statement)
        {

        }

        /// <summary>
        /// Gets an enumerator that can be used to iterate through the rows.
        /// </summary>
        /// <returns></returns>
        public new IEnumerator<IDataRecord> GetEnumerator()
        {
            return (CalciteEnumerator)base.GetEnumerator();
        }

    }

}
