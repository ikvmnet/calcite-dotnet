using System.Data;

namespace Apache.Calcite.Data.Internal
{

    /// <summary>
    /// A parameter's type and value as captured for one execution request.
    /// </summary>
    internal readonly struct CalciteParameterValue
    {

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameterValue"/> struct.
        /// </summary>
        /// <param name="dbType">The parameter's declared type.</param>
        /// <param name="value">The parameter's value, or <see langword="null"/>.</param>
        public CalciteParameterValue(DbType dbType, object? value)
        {
            DbType = dbType;
            Value = value;
        }

        /// <summary>
        /// Gets the parameter's declared type.
        /// </summary>
        public DbType DbType { get; }

        /// <summary>
        /// Gets the parameter's value.
        /// </summary>
        public object? Value { get; }

    }

}
