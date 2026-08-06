namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Says how a driver names a query parameter.
    /// </summary>
    /// <remarks>
    /// Deliberately not the dialect. A dialect is a property of the <i>server</i> — what SQL it understands
    /// — and Calcite already models it. This is a property of the <i>driver</i>: two providers reaching the
    /// same server disagree, since SqlClient binds <c>@P0</c> where ODBC to that same server binds <c>?</c>.
    /// The two travel together but neither derives from the other, so they are passed side by side rather
    /// than one carrying the other.
    /// </remarks>
    public interface IAdoSqlSyntax
    {

        /// <summary>
        /// Returns the name the driver binds the parameter at the given position by.
        /// </summary>
        /// <param name="index">The zero-based position of the parameter within the statement.</param>
        /// <returns>A parameter name suitable for inclusion in a SQL statement.</returns>
        /// <remarks>
        /// <para>
        /// Calcite writes a parameter as a bare <c>?</c> and records its ordinal, because JDBC binds by
        /// position. ADO.NET matches a <see cref="System.Data.Common.DbParameter"/> to the command text by
        /// name, so the marker has to be written as the name that will be bound.
        /// </para>
        /// <para>
        /// <c>@P0</c> by default, which SqlClient, MySql and several others accept. A driver that spells it
        /// differently overrides this.
        /// </para>
        /// </remarks>
        string GetParameterName(int index) => "@P" + index;

    }

}
