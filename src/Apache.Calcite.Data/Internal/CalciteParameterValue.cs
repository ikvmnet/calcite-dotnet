using System.Data;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// Represents a parameter value bound to a Calcite execution request.
    /// </summary>
    internal readonly struct CalciteParameterValue
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameterValue"/> struct.
        /// </summary>
        /// <param name="dbType"></param>
        /// <param name="value"></param>
        public CalciteParameterValue(DbType dbType, object? value)
        {
            DbType = dbType;
            Value = value;
        }

        /// <summary>
        /// Gets the parameter type.
        /// </summary>
        public DbType DbType { get; }

        /// <summary>
        /// Gets the parameter value.
        /// </summary>
        public object? Value { get; }

    }

}
