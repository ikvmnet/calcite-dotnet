using System;
using System.Data.Common;
using System.Runtime.Serialization;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// The exception that is thrown when Apache Calcite fails to load a model, open a connection, or parse,
    /// plan or execute a statement.
    /// </summary>
    /// <remarks>
    /// The error Calcite raised is available through <see cref="Exception.InnerException"/>.
    /// </remarks>
    [Serializable]
    public class CalciteException : DbException
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class.
        /// </summary>
        public CalciteException() :
            base()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public CalciteException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class with a specified error message and a reference to the inner exception that caused this exception.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="innerException">The exception that is the cause of the current exception, or <see langword="null"/> if no inner exception is specified.</param>
        public CalciteException(string message, Exception? innerException) :
            base(message, innerException)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class with serialized data.
        /// </summary>
        /// <param name="info">The object that holds the serialized object data.</param>
        /// <param name="context">The contextual information about the source or destination.</param>
        protected CalciteException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {

        }

    }

}
