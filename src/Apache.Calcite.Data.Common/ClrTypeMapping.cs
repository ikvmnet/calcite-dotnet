using System;

using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.java;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// One CLR type's relationship to one Calcite type, and the conversions across it.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Three boundaries need the same three facts, so they are one object: which .NET type a column is seen
    /// as, what a .NET value becomes on the way in, and what comes back out. A mapping that names a type
    /// without carrying its conversions is where the four tables this replaces drifted apart — the ADO
    /// adapter typed a provider <c>uniqueidentifier</c> as <c>CHAR(36)</c> in one file and had to discover
    /// in another that the value arriving was a <see cref="Guid"/> and not a string.
    /// </para>
    /// <para>
    /// <see cref="RepresentationType"/> is the anchor. Calcite decides what class holds a value of a given
    /// type through <c>JavaTypeFactory.getJavaClass</c>, and that answer is not fixed: a schema that types a
    /// column with <c>createJavaType</c> carries its own class through the whole plan, ahead of every
    /// <c>SqlTypeName</c> the switch in <c>JavaTypeFactoryImpl</c> knows. A mapping therefore states what it
    /// produces and the registry checks the statement against the type factory rather than assuming.
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
        /// <param name="typeFactory"></param>
        /// <param name="relType"></param>
        /// <returns></returns>
        /// <remarks>
        /// Boxed, because a value that has left the plan is a reference whatever the physical type said, and
        /// because a nullable column and a non-nullable one of the same type would otherwise answer
        /// differently — <c>getJavaClass</c> returns <c>int.class</c> for a <c>NOT NULL</c> <c>INTEGER</c>
        /// and <c>Integer.class</c> for a nullable one.
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
        /// Computed from the type factory rather than declared, so that a mapping cannot claim a class the
        /// factory disagrees with. Overridable for the one case where <c>getJavaClass</c> describes the form
        /// a value has <em>inside</em> a plan rather than the form it has at this boundary — see
        /// <see cref="RowClrTypeMapping"/>, where the two genuinely differ.
        /// </remarks>
        public virtual Type RepresentationType => _representationType;

        /// <summary>
        /// Gets the <see cref="System.Data.DbType"/> this mapping presents, which is
        /// <see cref="System.Data.DbType.Object"/> where nothing in that fixed list fits.
        /// </summary>
        /// <remarks>
        /// Derived from <see cref="ClrType"/> rather than stated, because <see cref="System.Data.DbType"/>
        /// names a .NET type and not a SQL one: <c>DATE</c> and <c>TIMESTAMP</c> are both read back as a
        /// <see cref="DateTime"/> and are the same <see cref="System.Data.DbType"/>, while the two Calcite
        /// types are not the same type at all. A mapping that presents a .NET type the list does not name —
        /// an array, a dictionary, a type of a caller's own — is <see cref="System.Data.DbType.Object"/>,
        /// which is what ADO.NET has for "not one of these". Override it where a mapping means a narrower
        /// one than its CLR type implies, such as an ANSI or fixed-length character type.
        /// </remarks>
        public virtual System.Data.DbType DbType => DbTypeOf(ClrType);

        /// <summary>
        /// Gets the <see cref="Common.CalciteDbType"/> naming <see cref="RelType"/>, which is
        /// <see cref="Common.CalciteDbType.Unknown"/> where that fixed list has no name for it.
        /// </summary>
        /// <remarks>
        /// The provider-specific counterpart of <see cref="DbType"/>, and the one that keeps apart what
        /// ADO.NET's list collapses: the unsigned integers, the zoned temporal types, the intervals,
        /// <c>UUID</c>, <c>VARIANT</c>, and an <c>ARRAY</c> from a <c>MULTISET</c>. Best effort, because
        /// Calcite's type model is open and a schema may supply a type that names nothing.
        /// </remarks>
        public virtual CalciteDbType CalciteDbType => CalciteDbTypes.Of(RelType);

        /// <summary>
        /// Returns the <see cref="System.Data.DbType"/> naming a CLR type, or
        /// <see cref="System.Data.DbType.Object"/> where none does.
        /// </summary>
        /// <param name="clrType"></param>
        /// <returns></returns>
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
        /// Converts a CLR value to the representation Calcite holds it in.
        /// </summary>
        /// <param name="value">The value, never <see langword="null"/>.</param>
        /// <returns></returns>
        public abstract object? ToCalcite(object value);

        /// <summary>
        /// Converts the representation Calcite holds a value in to the CLR type this mapping presents.
        /// </summary>
        /// <param name="value">The value, never <see langword="null"/>.</param>
        /// <returns></returns>
        public abstract object? FromCalcite(object value);

        /// <summary>
        /// Whether <see cref="ToCalcite"/> has been checked against <see cref="RepresentationType"/>.
        /// </summary>
        bool _checked;

        /// <summary>
        /// Converts a CLR value as <see cref="ToCalcite"/> does, checking the first result against
        /// <see cref="RepresentationType"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        /// <exception cref="ClrTypeMappingException"></exception>
        /// <remarks>
        /// A mapping that answers with a value of the wrong class does not fail here; it fails somewhere
        /// inside a plan, as a comparator refusing two representations of one value, and the mapping is by
        /// then several frames away. The check is worth its cost once per mapping — mappings are cached per
        /// pair of types, so this runs once and not once per row. It cannot be a check of the declaration
        /// instead: <see cref="RepresentationType"/> is computed from the type factory rather than declared,
        /// exactly so that a mapping cannot claim a class the factory disagrees with.
        /// </remarks>
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
