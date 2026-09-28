using System.Data.Common;

using Apache.Calcite.Adapter.AdoNet.Extensions;
using Apache.Calcite.Adapter.AdoNet.Utils;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Helpers for reading an <see cref="AdoTable"/>'s rows outside a pushed-down plan.
    /// </summary>
    public static class AdoUtils
    {

        /// <summary>
        /// Returns a row builder factory for <see cref="AdoEnumerable.CreateReader(AdoDataSource, string, Function1)"/>
        /// that reads each row into an <c>object[]</c> of Calcite's representations of the given fields.
        /// </summary>
        /// <param name="fields">The <see cref="RelDataTypeField"/>s of the row, each read at its own index.</param>
        /// <returns>A <see cref="Function1"/> from a <see cref="DbDataReader"/> to an <see cref="ObjectArrayRowBuilder"/>.</returns>
        public static Function1 CreateObjectArrayRowBuilderFactory(List fields)
        {
            return new FuncFunction1<DbDataReader, object>(reader => new ObjectArrayRowBuilder(reader, fields));
        }

    }

}
