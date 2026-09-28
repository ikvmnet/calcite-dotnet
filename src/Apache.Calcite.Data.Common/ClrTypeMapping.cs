using System;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Pairs one Calcite type with the CLR type it is presented as, and converts values between the two.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <see cref="ToCalcite"/> converts a CLR value to the class Calcite holds the type in, and
    /// <see cref="FromCalcite"/> converts back. Neither is called with <see langword="null"/>.
    /// </para>
    /// <para>
    /// The class Calcite holds the type in, <see cref="RepresentationType"/>, is not declared by the mapping
    /// but taken from the session's type factory (<c>JavaTypeFactory.getJavaClass</c>), since a schema can
    /// give a column its own class with <c>createJavaType</c>. The first value a mapping converts to Calcite
    /// is checked against it, and a mismatch throws <see cref="ClrTypeMappingException"/>.
    /// </para>
    /// </remarks>
    public abstract class ClrTypeMapping
    {

        readonly RelDataType _relType;
        readonly Type _clrType;
        readonly Type _representationType;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="context">The context the mapping was resolved in.</param>
        /// <param name="relType">The Calcite type this mapping is for.</param>
        /// <param name="clrType">The CLR type this mapping presents it as.</param>
        protected ClrTypeMapping(ClrTypeContext context, RelDataType relType, Type clrType)
        {
            ArgumentNullException.ThrowIfNull(context);

            _relType = relType ?? throw new ArgumentNullException(nameof(relType));
            _clrType = clrType ?? throw new ArgumentNullException(nameof(clrType));
            _representationType = RepresentationTypeOf(context.TypeFactory, relType);
        }

        /// <summary>
        /// Returns the runtime class Calcite holds a value of <paramref name="relType"/> in.
        /// </summary>
        /// <param name="typeFactory">The type factory whose <c>getJavaClass</c> decides the class.</param>
        /// <param name="relType">The Calcite type.</param>
        /// <returns>The class as a CLR type, with a Java primitive replaced by its boxed class.</returns>
        /// <remarks>
        /// The result is always the boxed class, so that a nullable and a <c>NOT NULL</c> column of the same
        /// type agree: <c>getJavaClass</c> answers <c>int</c> for <c>INTEGER NOT NULL</c> and
        /// <c>java.lang.Integer</c> for a nullable <c>INTEGER</c>.
        /// </remarks>
        public static Type RepresentationTypeOf(JavaTypeFactory typeFactory, RelDataType relType)
        {
            ArgumentNullException.ThrowIfNull(typeFactory);
            ArgumentNullException.ThrowIfNull(relType);

            return ClrPrimitive.Box(ClrTypes.Resolve(typeFactory.getJavaClass(relType)));
        }

        /// <summary>
        /// Gets the Calcite type this mapping is for.
        /// </summary>
        public RelDataType RelType => _relType;

        /// <summary>
        /// Gets the CLR type this mapping presents <see cref="RelType"/> as.
        /// </summary>
        public Type ClrType => _clrType;

        /// <summary>
        /// Gets the runtime class Calcite holds a value of <see cref="RelType"/> in, which is what
        /// <see cref="ToCalcite"/> answers with and what <see cref="FromCalcite"/> is handed.
        /// </summary>
        /// <remarks>
        /// By default this is <see cref="RepresentationTypeOf"/> for <see cref="RelType"/>. A mapping overrides
        /// it where the value at this boundary has a different class from the one <c>getJavaClass</c>
        /// describes inside a plan, as <see cref="RowClrTypeMapping"/> does.
        /// </remarks>
        public virtual Type RepresentationType => _representationType;

        /// <summary>
        /// Gets the <see cref="System.Data.DbType"/> for <see cref="ClrType"/>, which is
        /// <see cref="System.Data.DbType.Object"/> where no member of that enumeration fits.
        /// </summary>
        /// <remarks>
        /// <see cref="System.Data.DbType"/> describes a .NET type, so this is derived from
        /// <see cref="ClrType"/> by <see cref="DbTypeOf"/>; for example <c>DATE</c> and <c>TIMESTAMP</c>, both
        /// read as <see cref="DateTime"/>, are both <see cref="System.Data.DbType.DateTime"/>. Override it
        /// where a mapping means something narrower, such as an ANSI or fixed-length character type. Use
        /// <see cref="CalciteDbType"/> to tell Calcite types apart.
        /// </remarks>
        public virtual System.Data.DbType DbType => DbTypeOf(ClrType);

        /// <summary>
        /// Gets the <see cref="Common.CalciteDbType"/> naming <see cref="RelType"/>, which is
        /// <see cref="Common.CalciteDbType.Unknown"/> where the enumeration has no member for it.
        /// </summary>
        /// <remarks>
        /// Unlike <see cref="DbType"/>, this distinguishes the unsigned integers, the zoned temporal types,
        /// the intervals, <c>UUID</c>, <c>VARIANT</c>, and <c>ARRAY</c> from <c>MULTISET</c>.
        /// </remarks>
        public virtual CalciteDbType CalciteDbType => CalciteDbTypes.Of(RelType);

        /// <summary>
        /// Returns the <see cref="System.Data.DbType"/> naming a CLR type, or
        /// <see cref="System.Data.DbType.Object"/> where none does.
        /// </summary>
        /// <param name="clrType">The CLR type; a <see cref="Nullable{T}"/> is treated as its underlying
        /// type.</param>
        /// <returns>The <see cref="System.Data.DbType"/>.</returns>
        public static System.Data.DbType DbTypeOf(Type clrType)
        {
            ArgumentNullException.ThrowIfNull(clrType);

            var type = Nullable.GetUnderlyingType(clrType) ?? clrType;

            if (type == typeof(bool)) return System.Data.DbType.Boolean;
            if (type == typeof(byte)) return System.Data.DbType.Byte;
            if (type == typeof(sbyte)) return System.Data.DbType.SByte;
            if (type == typeof(short)) return System.Data.DbType.Int16;
            if (type == typeof(ushort)) return System.Data.DbType.UInt16;
            if (type == typeof(int)) return System.Data.DbType.Int32;
            if (type == typeof(uint)) return System.Data.DbType.UInt32;
            if (type == typeof(long)) return System.Data.DbType.Int64;
            if (type == typeof(ulong)) return System.Data.DbType.UInt64;
            if (type == typeof(float)) return System.Data.DbType.Single;
            if (type == typeof(double)) return System.Data.DbType.Double;
            if (type == typeof(decimal)) return System.Data.DbType.Decimal;
            if (type == typeof(string)) return System.Data.DbType.String;
            if (type == typeof(char)) return System.Data.DbType.StringFixedLength;
            if (type == typeof(Guid)) return System.Data.DbType.Guid;
            if (type == typeof(DateTime)) return System.Data.DbType.DateTime;
            if (type == typeof(DateTimeOffset)) return System.Data.DbType.DateTimeOffset;
            if (type == typeof(TimeSpan)) return System.Data.DbType.Time;
            if (type == typeof(DateOnly)) return System.Data.DbType.Date;
            if (type == typeof(TimeOnly)) return System.Data.DbType.Time;
            if (type == typeof(byte[])) return System.Data.DbType.Binary;

            return System.Data.DbType.Object;
        }

        /// <summary>
        /// Gets whether <see cref="RelType"/> determines what a value of it is, as opposed to the value's own
        /// runtime class determining it.
        /// </summary>
        /// <remarks>
        /// <see langword="true"/> except for the mappings of <c>ANY</c> and <c>OTHER</c>, which say nothing
        /// about their values, and <c>VARIANT</c>, whose values carry their own type. A caller that needs to
        /// know whether a column's type describes its values should ask this rather than test the type name.
        /// </remarks>
        public virtual bool DescribesValue => true;

        /// <summary>
        /// Returns whether a non-null value Calcite produced represents SQL null.
        /// </summary>
        /// <param name="value">The value, never <see langword="null"/>.</param>
        /// <returns><see langword="true"/> where the value is a SQL null.</returns>
        /// <remarks>
        /// Calcite holds a SQL null as a Java null for every type except <c>VARIANT</c>, whose null is an
        /// object; only the <c>VARIANT</c> mapping overrides this.
        /// </remarks>
        public virtual bool IsNull(object value) => false;

        /// <summary>
        /// Converts a CLR value to the class Calcite holds <see cref="RelType"/> in.
        /// </summary>
        /// <param name="value">The value, never <see langword="null"/>.</param>
        /// <returns>The value as an instance of <see cref="RepresentationType"/>, or
        /// <see langword="null"/>.</returns>
        public abstract object? ToCalcite(object value);

        /// <summary>
        /// Converts a value of the class Calcite holds <see cref="RelType"/> in to <see cref="ClrType"/>.
        /// </summary>
        /// <param name="value">The value, never <see langword="null"/>.</param>
        /// <returns>The CLR value, or <see langword="null"/>.</returns>
        public abstract object? FromCalcite(object value);

        /// <summary>
        /// Whether a result of <see cref="ToCalcite"/> has been checked against <see cref="RepresentationType"/>.
        /// </summary>
        bool _checked;

        /// <summary>
        /// Converts a CLR value with <see cref="ToCalcite"/>, checking the first result against
        /// <see cref="RepresentationType"/>.
        /// </summary>
        /// <exception cref="ClrTypeMappingException">The first result is neither <see langword="null"/> nor
        /// an instance of <see cref="RepresentationType"/>.</exception>
        /// <remarks>
        /// A value of the wrong class would otherwise fail much later, inside a plan, far from the mapping
        /// that produced it. Mappings are cached per pair of types, so the check runs about once per mapping
        /// rather than once per row.
        /// </remarks>
        /// <param name="value">The CLR value to convert; not <see langword="null"/>.</param>
        /// <returns>What <see cref="ToCalcite"/> answers for the value.</returns>
        internal object? ConvertToCalcite(object value)
        {
            var result = ToCalcite(value);

            if (_checked == false)
            {
                if (result is not null && RepresentationType.IsInstanceOfType(result) == false)
                    throw new ClrTypeMappingException($"The mapping {this} answered with a {result.GetType()}, which is not the {RepresentationType} that {RelType} is held in.");

                _checked = true;
            }

            return result;
        }

        /// <inheritdoc />
        public override string ToString() => $"{ClrType.Name} <-> {RelType} ({RepresentationType.Name})";

    }

}
