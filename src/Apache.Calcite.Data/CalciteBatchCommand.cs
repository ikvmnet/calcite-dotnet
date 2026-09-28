using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a single SQL statement within a <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Set <see cref="CommandText"/> to the SQL text to run and add any parameters to <see cref="Parameters"/>.
    /// Only <see cref="CommandType.Text"/> is supported. Parameter placeholders are positional <c>?</c>
    /// markers, bound in the order the parameters appear in <see cref="Parameters"/>.
    /// </remarks>
    public sealed class CalciteBatchCommand : DbBatchCommand
    {

        readonly CalciteParameterCollection _parameters = new();
        string _commandText = string.Empty;
        CommandType _commandType = CommandType.Text;
        int _recordsAffected;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatchCommand"/> class with an empty command text.
        /// </summary>
        public CalciteBatchCommand()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatchCommand"/> class with the specified SQL text.
        /// </summary>
        /// <param name="commandText">The SQL statement to execute, or <see langword="null"/> for an empty command text.</param>
        public CalciteBatchCommand(string? commandText)
        {
            _commandText = commandText ?? string.Empty;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Setting <see langword="null"/> sets the empty string.
        /// </remarks>
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets how <see cref="CommandText"/> is interpreted. Only <see cref="CommandType.Text"/> is supported.
        /// </summary>
        /// <exception cref="NotSupportedException">The value is not <see cref="CommandType.Text"/>.</exception>
        public override CommandType CommandType
        {
            get => _commandType;
            set
            {
                if (value != CommandType.Text)
                    throw new NotSupportedException("Only CommandType.Text is supported.");
                _commandType = value;
            }
        }

        /// <summary>
        /// Gets the number of rows this command affected when its batch last executed.
        /// </summary>
        /// <remarks>
        /// After <see cref="DbBatch.ExecuteNonQuery"/>: the number of rows affected for a data modification, 0 for
        /// DDL, -1 for a query. After <see cref="DbBatch.ExecuteReader(CommandBehavior)"/>: 0. Before the batch executes: 0.
        /// </remarks>
        public override int RecordsAffected => _recordsAffected;

        /// <inheritdoc />
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <summary>
        /// Gets the parameters of the command.
        /// </summary>
        /// <remarks>
        /// Add <see cref="CalciteParameter"/> instances in the order of the <c>?</c> placeholders in
        /// <see cref="CommandText"/>.
        /// </remarks>
        public new CalciteParameterCollection Parameters => _parameters;

        internal void SetRecordsAffected(int value) => _recordsAffected = value;

    }

}
