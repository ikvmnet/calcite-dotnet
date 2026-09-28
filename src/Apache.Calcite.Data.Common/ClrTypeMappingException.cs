using System;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// The exception thrown when no mapping answers a type lookup, or when a mapping converts a value to a
    /// class other than the one Calcite holds that type in.
    /// </summary>
    public class ClrTypeMappingException : InvalidCastException
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ClrTypeMappingException()
        {

        }

        /// <summary>
        /// Initializes a new instance with a message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public ClrTypeMappingException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance with a message and the exception that caused it.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that caused this one.</param>
        public ClrTypeMappingException(string message, Exception innerException) :
            base(message, innerException)
        {

        }

    }

}
