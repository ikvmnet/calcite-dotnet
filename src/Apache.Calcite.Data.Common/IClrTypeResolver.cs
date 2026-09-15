using System;
using System.Collections.Generic;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Answers what a Calcite type looks like to .NET, and what a .NET value looks like to Calcite.
    /// </summary>
    /// <remarks>
    /// The whole extension point is this one method. A caller that wants a type of its own — a provider
    /// type with no Calcite equivalent, a domain type in place of a string, a different .NET type for
    /// <c>TIMESTAMP</c> — writes one of these and puts it in front of the chain.
    /// </remarks>
    public interface IClrTypeResolver
    {

        /// <summary>
        /// Resolves a mapping for a CLR type, a Calcite type, or both. At least one is supplied.
        /// </summary>
        /// <param name="clrType">The CLR type wanted, or <see langword="null"/> where the caller has no preference.</param>
        /// <param name="relType">The Calcite type in play, or <see langword="null"/> where it is not yet decided.</param>
        /// <param name="context">The type factory and registry the lookup is being answered against.</param>
        /// <returns>A mapping, or <see langword="null"/> to pass the question to the next resolver.</returns>
        ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context);

        /// <summary>
        /// Returns every CLR type this resolver will present <paramref name="relType"/> as, most preferred
        /// first.
        /// </summary>
        /// <param name="relType">The Calcite type in play.</param>
        /// <param name="context">The type factory and registry the question is being answered against.</param>
        /// <returns>The CLR types, which may be empty where the resolver claims the type for none.</returns>
        /// <remarks>
        /// <see cref="GetMapping"/> answers which conversion to use; this answers which are permitted, which
        /// is a different question and the one an introspecting caller asks. A schema browser listing what a
        /// column can be read as, and a modelling layer choosing among them, both need the set rather than
        /// the winner — an object-relational mapper picking the property type for a <c>BIGINT</c> wants to
        /// know that <see cref="long"/> is the default and whether anything else is legal at all.
        ///
        /// <para>Defaulted, so that a resolver written before this existed still compiles and still
        /// resolves. The default answers the default mapping alone, which is true of any resolver and
        /// understates one that accepts more.</para>
        /// </remarks>
        IEnumerable<Type> GetClrTypes(RelDataType relType, ClrTypeContext context)
        {
            if (GetMapping(null, relType, context) is ClrTypeMapping mapping)
                yield return mapping.ClrType;
        }

    }

}
