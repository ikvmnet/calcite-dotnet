using System.Data;
using System.Data.Common;

using Apache.Calcite.Data.Internal;


namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a parameter to a <see cref="CalciteCommand"/>. This class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// Calcite parameters are positional. The order in which parameters are added to
    /// <see cref="CalciteParameterCollection"/> determines the binding order of the corresponding
    /// <c>?</c> placeholders in the SQL text; <see cref="ParameterName"/> is informational.
    /// </remarks>
    public sealed class CalciteParameter : DbParameter
    {

        string _parameterName = string.Empty;
        string _sourceColumn = string.Empty;
        object? _value;
        DbType _dbType = DbType.Object;
        bool _dbTypeSet;
        ParameterDirection _direction = ParameterDirection.Input;
        bool _isNullable;
        int _size;
        byte _precision;
        byte _scale;

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class.
        /// </summary>
        public CalciteParameter()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="value"></param>
        public CalciteParameter(string parameterName, object? value)
        {
            ParameterName = parameterName;
            Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class.
        /// </summary>
        /// <param name="parameterName"></param>
        /// <param name="dbType"></param>
        public CalciteParameter(string parameterName, DbType dbType)
        {
            ParameterName = parameterName;
            DbType = dbType;
        }

        /// <inheritdoc />
        public override DbType DbType
        {
            get => _dbTypeSet ? _dbType : (_value is null ? DbType.Object : CalciteTypeMap.ToDbType(_value.GetType()));
            set
            {
                _dbType = value;
                _dbTypeSet = true;
            }
        }

        /// <inheritdoc />
        public override ParameterDirection Direction
        {
            get => _direction;
            set => _direction = value;
        }

        /// <inheritdoc />
        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        /// <inheritdoc />
        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value ?? string.Empty;
        }

        /// <inheritdoc />
        public override int Size
        {
            get => _size;
            set => _size = value;
        }

        /// <inheritdoc />
        public override string SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value ?? string.Empty;
        }

        /// <inheritdoc />
        public override bool SourceColumnNullMapping { get; set; }

        /// <inheritdoc />
        public override object? Value
        {
            get => _value;
            set => _value = value;
        }

        /// <inheritdoc />
        public override byte Precision
        {
            get => _precision;
            set => _precision = value;
        }

        /// <inheritdoc />
        public override byte Scale
        {
            get => _scale;
            set => _scale = value;
        }

        /// <inheritdoc />
        public override void ResetDbType()
        {
            _dbType = DbType.Object;
            _dbTypeSet = false;
        }

    }

}
