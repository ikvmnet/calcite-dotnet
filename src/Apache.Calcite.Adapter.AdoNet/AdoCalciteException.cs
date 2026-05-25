using System;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The exception that is thrown when the Apache Calcite ADO.NET adapter encounters an error
    /// while accessing or querying an ADO.NET-backed schema.
    /// </summary>
    public class AdoCalciteException : CalciteException
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="AdoCalciteException"/> class with a specified error message and the exception that caused it.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        /// <param name="cause">The exception that is the cause of this exception, or <see langword="null"/> if none.</param>
        public AdoCalciteException(string message, Exception? cause) :
            base(message, cause)
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="AdoCalciteException"/> class with a specified error message.
        /// </summary>
        /// <param name="message">The message that describes the error.</param>
        public AdoCalciteException(string message) :
            this(message, null)
        {

        }

    }

}
