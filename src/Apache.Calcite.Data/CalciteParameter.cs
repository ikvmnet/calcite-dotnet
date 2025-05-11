using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

namespace Apache.Calcite.Data
{

    /// <inheritdoc />
    public class CalciteParameter : DbParameter
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteParameter()
        {
            Direction = ParameterDirection.Input;
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        public CalciteParameter(string parameterName, object value) :
            this()
        {
            ParameterName = parameterName;
            Value = value;
        }

        /// <inheritdoc />
#if NET
        [AllowNull]
#endif
        public override string ParameterName { get; set; } = "";

        /// <inheritdoc />
        public override ParameterDirection Direction { get; set; }

        /// <inheritdoc />
        public override DbType DbType { get; set; }

        /// <inheritdoc />
        public override void ResetDbType()
        {
            DbType = DbType.String;
        }

        /// <inheritdoc />
        public override bool IsNullable { get; set; }

        /// <inheritdoc />
        public override int Size { get; set; }

        /// <inheritdoc />
#if NET
        [AllowNull]
#endif
        public override string SourceColumn { get; set; } = "";

        /// <inheritdoc />
        public override bool SourceColumnNullMapping { get; set; }

        /// <inheritdoc />
        public override object? Value { get; set; }

    }

}
