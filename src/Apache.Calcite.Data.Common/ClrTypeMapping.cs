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
        public Type RepresentationType => _representationType;

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
