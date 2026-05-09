using System;
using System.Data.Common;
using System.Runtime.Serialization;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents an error raised by the Apache Calcite ADO.NET provider.
    /// </summary>
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
        /// <param name="message"></param>
        public CalciteException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class with a specified error message and a reference to the inner exception that caused this exception.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public CalciteException(string message, Exception? innerException) :
            base(message, innerException)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteException"/> class with serialized data.
        /// </summary>
        /// <param name="info"></param>
        /// <param name="context"></param>
        protected CalciteException(SerializationInfo info, StreamingContext context) :
            base(info, context)
        {

        }

    }

}
