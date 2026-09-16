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
    /// <returns>The mapping, or <see langword="null"/> where this entry cannot answer for these two types
    /// after all.</returns>
    /// <remarks>
    /// <para>
    /// An entry that needs more than two conversions carries one of these instead: a collection resolves
    /// its element's mapping through <see cref="ClrTypeContext.Registry"/> and cannot be written as a pair
    /// of delegates over a type it does not know yet.
    /// </para>
    /// <para>
    /// <b>Declining is why the result is nullable.</b> An entry's predicates answer for a whole shape —
    /// the collection entry accepts every array — and whether it can really carry <em>this</em> array is
    /// not known until the element has been looked up. An entry that throws there would make
    /// <c>GetFieldValue&lt;long[]&gt;</c> over an <c>INTEGER ARRAY</c> fail differently from
    /// <c>GetInt64</c> over an <c>INTEGER</c>; answering <see langword="null"/> lets the lookup carry on
    /// to the next candidate and end where every other refusal ends.
    /// </para>
    /// </remarks>
    public delegate ClrTypeMapping? ClrTypeMappingFactory(ClrTypeContext context, RelDataType relType, Type clrType);

}
