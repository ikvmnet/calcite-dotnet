using System;

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

    }

}
