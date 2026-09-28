using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Builds the mapping for a Calcite type and a CLR type that an entry of a
    /// <see cref="ClrTypeMappingCollection"/> has accepted.
    /// </summary>
    /// <param name="context">The type factory and registry the lookup is answered against.</param>
    /// <param name="relType">The Calcite type the mapping is for.</param>
    /// <param name="clrType">The CLR type the mapping presents it as.</param>
    /// <returns>The mapping, or <see langword="null"/> to decline, in which case the lookup goes on to the
    /// next entry.</returns>
    /// <remarks>
    /// Use a factory where a mapping cannot be written as two conversion delegates, for example one that
    /// resolves its element type's mapping through <see cref="ClrTypeContext.Registry"/>. An entry's
    /// predicates accept a whole family of types; a factory that cannot handle one member of the family
    /// returns <see langword="null"/> rather than throwing, so that the lookup fails the same way as any other
    /// lookup nothing answers.
    /// </remarks>
    public delegate ClrTypeMapping? ClrTypeMappingFactory(ClrTypeContext context, RelDataType relType, Type clrType);

}
