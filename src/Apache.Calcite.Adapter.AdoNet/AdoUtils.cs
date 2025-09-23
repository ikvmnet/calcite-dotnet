using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;

using Apache.Calcite.Adapter.AdoNet.Extensions;
using Apache.Calcite.Adapter.AdoNet.Utils;

using com.google.common.primitives;
using com.sun.tools.javac.util;

using org.apache.calcite.avatica;
using org.apache.calcite.linq4j.function;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    internal static class AdoUtils
    {

        /// <summary>
        /// 
        /// </summary>
        /// <param name="list"></param>
        /// <returns></returns>
        internal static Function1 RowBuilderFactory2(IEnumerable<(ColumnMetaData.Rep, DbType)> list)
        {
            var r = list.Select(i => i.Item1).ToArray();
            var t = list.Select(i => i.Item2).ToArray();
            return new FuncFunction1<DbDataReader, object>(resultSet => new ObjectArrayRowBuilder2(resultSet, r, t));
        }

    }

}
