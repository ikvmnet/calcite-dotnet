using System;
using System.Collections.Immutable;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Describes a request to execute a SQL statement against a Calcite session.
    /// </summary>
    internal sealed class CalciteExecuteRequest
    {

        /// <summary>
        /// Builds a <see cref="CalciteExecuteRequest"/> from a <see cref="CalciteParameterCollection"/>.
        /// </summary>
        internal static CalciteExecuteRequest From(string commandText, CalciteParameterCollection parameters, int timeoutSeconds)
        {
            var values = ImmutableArray.CreateBuilder<CalciteParameterValue>(parameters.Items.Count);
            foreach (var p in parameters.Items)
                values.Add(new CalciteParameterValue(p.DbType, p.Value));

            return new CalciteExecuteRequest(commandText, values.ToImmutable(), timeoutSeconds);
        }

        /// <summary>
        /// Clamps a <see cref="long"/> records-affected value to the <see cref="int"/> range.
        /// </summary>
        internal static int ClampToInt32(long value)
        {
            if (value > int.MaxValue) return int.MaxValue;
            if (value < int.MinValue) return int.MinValue;
            return (int)value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteExecuteRequest"/> class.
        /// </summary>
        /// <param name="sql"></param>
        /// <param name="parameters"></param>
        /// <param name="commandTimeoutSeconds"></param>
        public CalciteExecuteRequest(string sql, ImmutableArray<CalciteParameterValue> parameters, int commandTimeoutSeconds)
        {
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Parameters = parameters;
            CommandTimeoutSeconds = commandTimeoutSeconds;
        }

        /// <summary>
        /// Gets the SQL text to execute.
        /// </summary>
        public string Sql { get; }

        /// <summary>
        /// Gets the parameter values bound to the request.
        /// </summary>
        public ImmutableArray<CalciteParameterValue> Parameters { get; }

        /// <summary>
        /// Gets the command timeout in seconds, or <c>0</c> for no timeout.
        /// </summary>
        public int CommandTimeoutSeconds { get; }

    }

}
