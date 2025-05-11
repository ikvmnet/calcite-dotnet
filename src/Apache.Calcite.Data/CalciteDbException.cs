using System;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents an exception that occurred accessing a <see cref="CalciteConnection"/>.
    /// </summary>
    public class CalciteDbException : DbException
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteDbException()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        public CalciteDbException(string message) :
            base(message)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="message"></param>
        /// <param name="innerException"></param>
        public CalciteDbException(string message, Exception innerException) :
            base(message, innerException)
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="jdbcException"></param>
        public CalciteDbException(java.sql.SQLException jdbcException) :
            this(jdbcException.getMessage(), jdbcException)
        {

        }

    }

}
