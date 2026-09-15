using System.Data;
using System.Data.Common;
using System.Diagnostics.CodeAnalysis;

using Apache.Calcite.Data.Common;
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
        /// <param name="parameterName">An informational name for the parameter. Because Calcite uses positional binding, this name does not affect query execution.</param>
        /// <param name="value">The value of the parameter, or <see langword="null"/>.</param>
        public CalciteParameter(string parameterName, object? value)
        {
            ParameterName = parameterName;
            Value = value;
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteParameter"/> class with a name and database type.
        /// </summary>
        /// <param name="parameterName">An informational name for the parameter.</param>
        /// <param name="dbType">The <see cref="DbType"/> of the parameter.</param>
        public CalciteParameter(string parameterName, DbType dbType)
        {
            ParameterName = parameterName;
            DbType = dbType;
        }

        /// <inheritdoc />
        /// <remarks>
        /// One of three views of one statement. Setting this restates the parameter's type, so
        /// <see cref="CalciteDbType"/> and <see cref="RelDataType"/> follow — see <see cref="CalciteDbType"/>
        /// for what that costs in each direction. Where nothing has been set the value's own type decides,
        /// which is what a caller that only ever assigns <see cref="Value"/> relies on.
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
        /// Gets or sets the Calcite type this parameter is written as, which names what
        /// <see cref="DbType"/> cannot.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Three ways to say one thing, widening: <see cref="DbType"/> is the list every provider shares,
        /// this is Calcite's own list, and <see cref="RelDataType"/> is the type itself. A caller that needs
        /// an unsigned integer, a zoned timestamp, an interval, or a <c>MULTISET</c> rather than an
        /// <c>ARRAY</c>, has no way to say so through <see cref="DbType"/>, where every one of those is
        /// <see cref="System.Data.DbType.Object"/> or a near miss.
        /// </para>
        /// <para>
        /// <b>They are three views of one statement, not three settings.</b> Setting any one restates the
        /// parameter's type and the other two follow, so they can never disagree. What that costs depends
        /// on which way the restatement runs, and it is always a widening that keeps and a narrowing that
        /// approximates: setting <see cref="RelDataType"/> to an <c>INTEGER ARRAY ARRAY</c> leaves this
        /// <see cref="Common.CalciteDbType.Array"/> over
        /// <see cref="Common.CalciteDbType.Unknown"/> and <see cref="DbType"/>
        /// <see cref="System.Data.DbType.Object"/>, because neither list can spell it; setting this to
        /// <see cref="Common.CalciteDbType.UInteger"/> leaves <see cref="DbType"/> exact and
        /// <see cref="RelDataType"/> null, a name not being a type. Only <see cref="RelDataType"/> is never
        /// approximate, so set that one where the type nests or a schema supplied it.
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
        /// Gets or sets the Calcite type this parameter is written as, stated exactly.
        /// </summary>
        /// <remarks>
        /// The escape hatch from both fixed lists, and the only way to name a type that nests — an
        /// <c>INTEGER ARRAY ARRAY</c>, a <c>MAP</c> with stated key and value types, a <c>ROW</c> — or one a
        /// schema supplied itself. Build one from the connection's
        /// <see cref="CalciteConnection.TypeFactory"/>, which is the factory the session will plan against.
        ///
        /// <para>This outranks <see cref="CalciteDbType"/> and <see cref="DbType"/> where it is set, being
        /// the only one of the three that cannot be approximate.</para>
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
        /// <remarks>
        /// <see cref="DbParameter.ParameterName"/> is declared <see cref="AllowNullAttribute"/>, so the
        /// override says so too and reads a null as no name.
        /// </remarks>
        [AllowNull]
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
        /// <inheritdoc cref="ParameterName" path="/remarks" />
        [AllowNull]
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
            _calciteDbType = CalciteDbType.Unknown;
            _relDataType = null;
            _typeSet = false;
        }

    }

}
