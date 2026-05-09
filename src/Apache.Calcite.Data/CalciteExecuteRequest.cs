using System;
using System.Collections.Generic;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Describes a request to execute a SQL statement against a Calcite session.
    /// </summary>
    internal sealed class CalciteExecuteRequest
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteExecuteRequest"/> class.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="commandTimeoutSeconds"></param>
        public CalciteExecuteRequest(string sql, IReadOnlyList<CalciteParameterValue> parameters, int commandTimeoutSeconds)
        {
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Parameters = parameters ?? Array.Empty<CalciteParameterValue>();
            CommandTimeoutSeconds = commandTimeoutSeconds;
        }

        /// <summary>
        /// Gets the SQL text to execute.
        /// </summary>
        public string Sql { get; }

        /// <summary>
        /// Gets the parameter values bound to the request.
        /// </summary>
        public IReadOnlyList<CalciteParameterValue> Parameters { get; }

        /// <summary>
        /// Gets the command timeout in seconds, or <c>0</c> for no timeout.
        /// </summary>
        public int CommandTimeoutSeconds { get; }

    }

}
