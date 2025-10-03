using System.Data.Common;

using Apache.Calcite.Adapter.Ado.Extensions;
using Apache.Calcite.Adapter.Ado.Utils;

using java.util;

using org.apache.calcite.linq4j.function;
using org.apache.calcite.rel.type;

namespace Apache.Calcite.Adapter.Ado
{

    /// <summary>
    /// Various utilities for working with the ADO Calcite adapter.
    /// </summary>
    public static class AdoUtils
    {

        /// <summary>
        /// Creates a row builder factory for converting to the set of <see cref="RelDataTypeField"/>s
        /// </summary>
        /// <param name="fields"></param>
        /// <returns></returns>
        public static Function1 CreateObjectArrayRowBuilderFactory(List fields)
        {
            return new FuncFunction1<DbDataReader, object>(reader => new ObjectArrayRowBuilder(reader, fields));
        }

    }

}
