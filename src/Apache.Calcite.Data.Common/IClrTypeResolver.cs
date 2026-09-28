using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Maps Calcite types to CLR types and converts values between them.
    /// </summary>
    /// <remarks>
    /// Resolvers form a chain in which the first non-null answer wins. To present a type differently — a
    /// provider type with no Calcite equivalent, a domain type in place of a string, another .NET type for
    /// <c>TIMESTAMP</c> — implement this interface and put the resolver at the front of the chain with
    /// <see cref="ClrTypeMapper.Prepend"/>.
    /// </remarks>
    public interface IClrTypeResolver
    {

        /// <summary>
        /// Resolves a mapping for a CLR type, a Calcite type, or both. At least one is supplied.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> for the Calcite type's default.</param>
        /// <param name="relType">The Calcite type, or <see langword="null"/> where only the CLR type is known, as for a
        /// parameter holding a bare value.</param>
        /// <param name="context">The type factory and registry the lookup is answered against.</param>
        /// <returns>A mapping, or <see langword="null"/> to pass the lookup to the next resolver.</returns>
        ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context);

        /// <summary>
        /// Returns every CLR type this resolver can present <paramref name="relType"/> as, most preferred
        /// first.
        /// </summary>
        /// <param name="relType">The Calcite type.</param>
        /// <param name="context">The type factory and registry the question is answered against.</param>
        /// <returns>The CLR types; empty where the resolver does not handle the type.</returns>
        /// <remarks>
        /// <see cref="GetMapping"/> picks the one mapping to use; this lists every type a caller may ask for,
        /// which is what a schema browser or an object-relational mapper choosing a property type needs. The
        /// default implementation returns only the type of the default mapping, so a resolver that accepts
        /// more should override it.
        /// </remarks>
        IEnumerable<Type> GetClrTypes(RelDataType relType, ClrTypeContext context)
        {
            if (GetMapping(null, relType, context) is ClrTypeMapping mapping)
                yield return mapping.ClrType;
        }

    }

}
