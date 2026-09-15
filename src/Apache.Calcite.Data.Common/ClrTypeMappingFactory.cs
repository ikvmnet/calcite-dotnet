using System;

using org.apache.calcite.rel.type;

namespace Apache.Calcite.Data.Common
{

    /// <summary>
    /// Builds a mapping once the lookup has settled which Calcite type and which CLR type are in play.
    /// </summary>
    /// <param name="context">The type factory and registry the lookup is being answered against.</param>
    /// <param name="relType">The Calcite type the mapping is for.</param>
    /// <param name="clrType">The CLR type the mapping presents it as.</param>
    /// <returns>The mapping.</returns>
    /// <remarks>
    /// An entry that needs more than two conversions carries one of these instead: a collection resolves
    /// its element's mapping through <see cref="ClrTypeContext.Registry"/> and cannot be written as a pair
    /// of delegates over a type it does not know yet.
    /// </remarks>
    public delegate ClrTypeMapping ClrTypeMappingFactory(ClrTypeContext context, RelDataType relType, Type clrType);

}
