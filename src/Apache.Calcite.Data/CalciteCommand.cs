using System.Data;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a SQL statement or stored procedure to execute against a <see cref="CalciteConnection"/>.
    /// </summary>
    public sealed class CalciteCommand : CalciteCommandBase
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteCommand()
        {

        }

        /// <summary>
        /// Intializes a new instance.
        /// </summary>
        /// <param name="connection"></param>
        public CalciteCommand(CalciteConnection connection) :
            base(connection)
        {

        }

        /// <summary>
        /// Gets the connection associated with this command.
        /// </summary>
        public new CalciteConnection? Connection
        {
            get => (CalciteConnection?)base.Connection;
            set => base.Connection = value;
        }

        /// <summary>
        /// Gets or sets the <see cref="CalciteTransaction"/> within which this <see cref="CalciteCommand"/> object executes.
        /// </summary>
        public new CalciteTransaction? Transaction
        {
            get => (CalciteTransaction?)base.Transaction;
            set => base.Transaction = value;
        }

        /// <summary>
        /// Gets the collection of <see cref="CalciteParameter"/> objects.
        /// </summary>
        public new CalciteParameterCollection Parameters
        {
            get => (CalciteParameterCollection)base.Parameters;
        }

        /// <summary>
        /// Creates a new instance of a <see cref="CalciteParameter"/> object.
        /// </summary>
        /// <returns></returns>
        public new CalciteParameter CreateParameter()
        {
            return (CalciteParameter)base.CreateParameter();
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="CalciteDataReader"/> which can be used to access the results.
        /// </summary>
        /// <param name="behavior"></param>
        /// <returns></returns>
        public new CalciteDataReader ExecuteReader(CommandBehavior behavior)
        {
            return (CalciteDataReader)base.ExecuteReader(behavior);
        }

        /// <summary>
        /// Executes the command against its connection, returning a <see cref="CalciteDataReader"/> which can be used to access the results.
        /// </summary>
        /// <returns></returns>
        public new CalciteDataReader ExecuteReader()
        {
            return (CalciteDataReader)base.ExecuteReader();
        }

    }

}
