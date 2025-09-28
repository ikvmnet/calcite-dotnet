using System;

using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.sql;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    class AdoSetOpFactory : SetOpFactory
    {

        public RelNode createSetOp(SqlKind kind, List inputs, bool all)
        {
            var input = (RelNode)inputs.get(0);
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(input.getConvention() ?? throw new ArgumentNullException("input.getConvention()"));

            switch ((SqlKind.__Enum)kind.ordinal())
            {
                case SqlKind.__Enum.UNION:
                    return new AdoUnion(cluster, traitSet, inputs, all);
                case SqlKind.__Enum.INTERSECT:
                    return new AdoIntersect(cluster, traitSet, inputs, all);
                case SqlKind.__Enum.EXCEPT:
                    return new AdoMinus(cluster, traitSet, inputs, all);
                default:
                    throw new AssertionError("unknown: " + kind);
            }
        }

    }

}
