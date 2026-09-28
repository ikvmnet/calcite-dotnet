using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Data.Common;
using Apache.Calcite.Data.Internal;


namespace Apache.Calcite.Data
{

    /// <summary>
    /// Represents a parameter to a <see cref="CalciteCommand"/> or <see cref="CalciteBatchCommand"/>. This
    /// class cannot be inherited.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Calcite parameters are positional: the parameter's position in its
    /// <see cref="CalciteParameterCollection"/> decides which <c>?</c> placeholder it binds to, and
    /// <see cref="ParameterName"/> is used only to look the parameter up in the collection.
    /// </para>
    /// <para>
    /// Calcite infers a SQL type for every placeholder while it validates the statement, and the value is
    /// converted to that type. The parameter's <see cref="DbType"/> chooses which .NET type the value is
    /// converted from, where the inferred type has a conversion from it; otherwise the inferred type's default
    /// conversion is used. A <see langword="null"/> or <see cref="System.DBNull"/> value binds SQL null. Only
    /// input parameters are supported; <see cref="Direction"/>, <see cref="Size"/>, <see cref="Precision"/>
    /// and <see cref="Scale"/> are stored and not used.
    /// </para>
    /// <para>
    /// The parameter's type can be stated three ways, as <see cref="DbType"/>, as <see cref="CalciteDbType"/>
    /// or as <see cref="RelDataType"/>. Setting one sets the other two to the nearest equivalent, so they never
    /// disagree. Binding reads <see cref="DbType"/>.
    /// </para>
    /// </remarks>
    public sealed class CalciteParameter : DbParameter
    {

        string _parameterName = string.Empty;
        string _sourceColumn = string.Empty;
        object? _value;
        DbType _dbType = DbType.Object;
        CalciteDbType _calciteDbType = CalciteDbType.Unknown;
        org.apache.calcite.rel.type.RelDataType? _relDataType;
        bool _typeSet;
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
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class with a name and value.
        /// </summary>
        /// <param name="parameterName">The name of the parameter. It does not affect binding, which is by position.</param>
        /// <param name="value">The value of the parameter, or <see langword="null"/>.</param>
        public CalciteParameter(string parameterName, object? value)
        {
            ParameterName = parameterName;
            Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class with a name and database type.
        /// </summary>
        /// <param name="parameterName">The name of the parameter. It does not affect binding, which is by position.</param>
        /// <param name="dbType">The <see cref="DbType"/> of the parameter.</param>
        public CalciteParameter(string parameterName, DbType dbType)
        {
            ParameterName = parameterName;
            DbType = dbType;
        }

        /// <summary>
        /// Gets or sets the <see cref="System.Data.DbType"/> of the parameter.
        /// </summary>
        /// <remarks>
        /// Where no type has been set, the type is inferred from the CLR type of <see cref="Value"/>, and is
        /// <see cref="System.Data.DbType.Object"/> for a <see langword="null"/> value or a type with no corresponding
        /// <see cref="System.Data.DbType"/>. Setting it also sets <see cref="CalciteDbType"/> to the nearest
        /// equivalent and clears <see cref="RelDataType"/>.
        /// </remarks>
        public override DbType DbType
        {
            get => _typeSet ? _dbType : (_value is null ? DbType.Object : CalciteTypeMap.ToDbType(_value.GetType()));
            set
            {
                _dbType = value;
                _calciteDbType = CalciteDbTypes.FromDbType(value);
                _relDataType = null;
                _typeSet = true;
            }
        }

        /// <summary>
        /// Gets or sets the Calcite type of the parameter, including types <see cref="DbType"/> cannot name.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <see cref="Common.CalciteDbType"/> names Calcite types that <see cref="System.Data.DbType"/> has no member
        /// for, such as the unsigned integers, the zoned timestamps, the intervals, and <c>MULTISET</c> as distinct
        /// from <c>ARRAY</c>. Where no type has been set, it is derived from <see cref="DbType"/>.
        /// </para>
        /// <para>
        /// Setting it also sets <see cref="DbType"/> to the nearest equivalent, which is
        /// <see cref="System.Data.DbType.Object"/> where there is none, and clears <see cref="RelDataType"/>.
        /// </para>
        /// </remarks>
        public CalciteDbType CalciteDbType
        {
            get => _typeSet ? _calciteDbType : CalciteDbTypes.FromDbType(DbType);
            set
            {
                _calciteDbType = value;
                _dbType = CalciteDbTypes.ToDbType(value);
                _relDataType = null;
                _typeSet = true;
            }
        }

        /// <summary>
        /// Gets or sets the exact Calcite type of the parameter, or <see langword="null"/> where none is set.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Use this for a type the other two properties cannot express: a nested type such as
        /// <c>INTEGER ARRAY ARRAY</c>, a <c>MAP</c> with its key and value types, a <c>ROW</c>, or a type a schema
        /// supplies. Create it with the connection's <see cref="CalciteConnection.TypeFactory"/>.
        /// </para>
        /// <para>
        /// Setting it also sets <see cref="CalciteDbType"/> and <see cref="DbType"/> to the nearest equivalents,
        /// which may be approximate: an <c>INTEGER ARRAY ARRAY</c> gives <see cref="Common.CalciteDbType.Array"/>
        /// and <see cref="System.Data.DbType.Object"/>. Setting <see cref="DbType"/> or <see cref="CalciteDbType"/> clears
        /// it. Setting it to <see langword="null"/> resets all three, as <see cref="ResetDbType"/> does.
        /// </para>
        /// </remarks>
        public org.apache.calcite.rel.type.RelDataType? RelDataType
        {
            get => _relDataType;
            set
            {
                _relDataType = value;

                if (value is null)
                {
                    ResetDbType();
                    return;
                }

                _calciteDbType = CalciteDbTypes.Of(value);
                _dbType = CalciteDbTypes.ToDbType(_calciteDbType);
                _typeSet = true;
            }
        }

        /// <inheritdoc />
        /// <remarks>
        /// Only <see cref="ParameterDirection.Input"/> is meaningful. Other values are stored and ignored.
        /// </remarks>
        public override ParameterDirection Direction
        {
            get => _direction;
            set => _direction = value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Stored and not used.
        /// </remarks>
        public override bool IsNullable
        {
            get => _isNullable;
            set => _isNullable = value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Setting <see langword="null"/> sets the empty string. The name is used to find the parameter in its
        /// collection, not to bind it.
        /// </remarks>
        [AllowNull]
        public override string ParameterName
        {
            get => _parameterName;
            set => _parameterName = value ?? string.Empty;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Stored and not used.
        /// </remarks>
        public override int Size
        {
            get => _size;
            set => _size = value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Setting <see langword="null"/> sets the empty string.
        /// </remarks>
        [AllowNull]
        public override string SourceColumn
        {
            get => _sourceColumn;
            set => _sourceColumn = value ?? string.Empty;
        }

        /// <inheritdoc />
        public override bool SourceColumnNullMapping { get; set; }

        /// <inheritdoc />
        /// <remarks>
        /// The value is read when the command executes.
        /// </remarks>
        public override object? Value
        {
            get => _value;
            set => _value = value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Stored and not used.
        /// </remarks>
        public override byte Precision
        {
            get => _precision;
            set => _precision = value;
        }

        /// <inheritdoc />
        /// <remarks>
        /// Stored and not used.
        /// </remarks>
        public override byte Scale
        {
            get => _scale;
            set => _scale = value;
        }

        /// <summary>
        /// Clears the parameter's type, so that it is inferred from <see cref="Value"/> again.
        /// </summary>
        /// <remarks>
        /// Resets <see cref="DbType"/>, <see cref="CalciteDbType"/> and <see cref="RelDataType"/>.
        /// </remarks>
        public override void ResetDbType()
        {
            _dbType = DbType.Object;
            _calciteDbType = CalciteDbType.Unknown;
            _relDataType = null;
            _typeSet = false;
        }

    }

}
