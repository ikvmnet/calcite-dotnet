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
        /// <param name="name"></param>
        /// <param name="dbType"></param>
        /// <param name="value"></param>
        public CalciteParameterValue(string name, DbType dbType, object? value)
        {
            Name = name;
            DbType = dbType;
            Value = value;
        }

        /// <summary>
        /// Gets the parameter name.
        /// </summary>
        public string Name { get; }

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
