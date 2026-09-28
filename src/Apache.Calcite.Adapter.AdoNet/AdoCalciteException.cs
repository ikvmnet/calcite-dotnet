using System;

using org.apache.calcite.runtime;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The exception the ADO.NET adapter throws for a configuration it cannot use, metadata it cannot read, or a
    /// statement the provider rejects.
    /// </summary>
    /// <remarks>
    /// It derives from Calcite's <see cref="CalciteException"/>, a Java <c>RuntimeException</c>. Where it wraps a
    /// provider exception, that exception is the cause.
    /// </remarks>
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
