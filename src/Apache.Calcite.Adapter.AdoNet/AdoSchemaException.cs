using System;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Describes an error thrown by the ADO schema.
    /// </summary>
    public class AdoSchemaException : CalciteException
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="cause"></param>
        public AdoSchemaException(string message, Exception? cause) :
            base(message, cause)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        public AdoSchemaException(string message) :
            this(message, null)
        {

        }

    }

}
