using System;
using System.Data;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a single command within a <see cref="CalciteBatch"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// A <see cref="CalciteBatchCommand"/> carries the SQL text and parameter set for one statement
    /// in a batched execution. Only <see cref="CommandType.Text"/> is supported. Parameter
    /// placeholders are positional <c>?</c> markers, bound by ordinal in the order they were added
    /// to <see cref="DbBatchCommand.Parameters"/>.
    /// </remarks>
    public sealed class CalciteBatchCommand : DbBatchCommand
    {

        readonly CalciteParameterCollection _parameters = new();
        string _commandText = string.Empty;
        CommandType _commandType = CommandType.Text;
        int _recordsAffected;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatchCommand"/> class.
        /// </summary>
        public CalciteBatchCommand()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteBatchCommand"/> class with the specified SQL text.
        /// </summary>
        /// <param name="commandText">The SQL text to execute against the Calcite engine, or <see langword="null"/> for an empty command text.</param>
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
        /// Gets the strongly typed <see cref="CalciteParameterCollection"/> associated with this batch command.
        /// </summary>
        public new CalciteParameterCollection Parameters => _parameters;

        internal void SetRecordsAffected(int value) => _recordsAffected = value;

    }

}
