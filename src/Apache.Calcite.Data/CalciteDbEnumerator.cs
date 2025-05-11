using System.Collections.Generic;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Supports a simple iteration over a collection by the Calcite data provider.
    /// </summary>
    class CalciteDbEnumerator : DbEnumerator, IEnumerator<IDataRecord>
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        public CalciteDbEnumerator(CalciteDataReaderBase reader) :
            base(reader)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="reader"></param>
        /// <param name="closeReader"></param>
        public CalciteDbEnumerator(CalciteDataReaderBase reader, bool closeReader) :
            base(reader, closeReader)
        {

        }

        /// <summary>
        /// Gets the current element in the collection.
        /// </summary>
        public new IDataRecord Current => (IDataRecord)base.Current;

        /// <summary>
        /// Disposes of the instance.
        /// </summary>
        public void Dispose()
        {

        }

    }

}
