using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Thrown where no mapping answers a lookup, or where one answers with a value of the wrong class.
    /// </summary>
    public class ClrTypeMappingException : Exception
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrTypeMappingException()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        public ClrTypeMappingException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public ClrTypeMappingException(string message, Exception innerException) :
            base(message, innerException)
        {

        }

    }

}
