using System;
using System.Collections.Generic;
using System.Collections.Immutable;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A statement to execute: its SQL text, a snapshot of its parameter values, its timeout and its hooks.
    /// </summary>
    internal sealed class CalciteExecuteRequest
    {

        /// <summary>
        /// Builds a request, capturing each parameter's current <see cref="System.Data.DbType"/> and value in
        /// collection order.
        /// </summary>
        /// <param name="commandText">The SQL text.</param>
        /// <param name="parameters">The command's parameters.</param>
        /// <param name="timeoutSeconds">The command timeout in seconds, or 0 for none.</param>
        /// <param name="hooks">The hooks to attach while the statement is planned and opened, or
        /// <see langword="null"/>.</param>
        /// <returns>The request.</returns>
        internal static CalciteExecuteRequest From(string commandText, CalciteParameterCollection parameters, int timeoutSeconds, IEnumerable<CalciteHookEntry>? hooks = null)
        {
            var values = ImmutableArray.CreateBuilder<CalciteParameterValue>(parameters.Items.Count);
            foreach (var p in parameters.Items)
                values.Add(new CalciteParameterValue(p.DbType, p.Value));

            return new CalciteExecuteRequest(commandText, values.ToImmutable(), timeoutSeconds, hooks);
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
        /// <param name="sql">The SQL text to execute.</param>
        /// <param name="parameters">The bound parameter values, aligned positionally to the <c>?</c> placeholders in <paramref name="sql"/>.</param>
        /// <param name="commandTimeoutSeconds">The command timeout in seconds, or <c>0</c> for no timeout.</param>
        /// <param name="hooks">The hooks to attach while the statement is planned and opened, or
        /// <see langword="null"/>.</param>
        public CalciteExecuteRequest(string sql, ImmutableArray<CalciteParameterValue> parameters, int commandTimeoutSeconds, IEnumerable<CalciteHookEntry>? hooks = null)
        {
            Sql = sql ?? throw new ArgumentNullException(nameof(sql));
            Parameters = parameters;
            CommandTimeoutSeconds = commandTimeoutSeconds;
            Hooks = hooks;
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

        /// <summary>
        /// Gets the hooks to attach while the statement is planned and opened, or <see langword="null"/>.
        /// </summary>
        public IEnumerable<CalciteHookEntry>? Hooks { get; }

    }

}
