using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a single SQL statement within a <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Set <see cref="DbBatchCommand.CommandText"/> to the SQL text to run and add any parameters to
    /// <see cref="Parameters"/>. Only <see cref="CommandType.Text"/> is supported. Parameter
    /// placeholders are positional <c>?</c> markers, bound in the order parameters were added to
    /// <see cref="Parameters"/>.
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
        public override string CommandText
        {
            get => _commandText;
            set => _commandText = value ?? string.Empty;
        }

        /// <inheritdoc />
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

        /// <inheritdoc />
        public override int RecordsAffected => _recordsAffected;

        /// <inheritdoc />
        protected override DbParameterCollection DbParameterCollection => _parameters;

        /// <summary>
        /// Gets the strongly typed parameter collection for this command.
        /// </summary>
        /// <remarks>
        /// Add <see cref="CalciteParameter"/> instances in the order they correspond to <c>?</c>
        /// placeholders in <see cref="DbBatchCommand.CommandText"/>.
        /// </remarks>
        public new CalciteParameterCollection Parameters => _parameters;

        internal void SetRecordsAffected(int value) => _recordsAffected = value;

    }

}
