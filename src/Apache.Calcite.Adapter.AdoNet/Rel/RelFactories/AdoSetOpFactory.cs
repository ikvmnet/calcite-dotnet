using System;

using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.sql;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="SetOpFactory"/> that creates an <see cref="AdoUnion"/>, <see cref="AdoIntersect"/> or
    /// <see cref="AdoMinus"/> in its first input's convention.
    /// </summary>
    public class AdoSetOpFactory : SetOpFactory
    {

        /// <inheritdoc />
        public RelNode createSetOp(SqlKind kind, List inputs, bool all)
        {
            var input = (RelNode)inputs.get(0);
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(input.getConvention() ?? throw new ArgumentNullException("input.getConvention()"));

            // by name: a Java enum's ordinals can change between Calcite versions
            switch (kind.name())
            {
                case nameof(SqlKind.UNION):
                    return new AdoUnion(cluster, traitSet, inputs, all);
                case nameof(SqlKind.INTERSECT):
                    return new AdoIntersect(cluster, traitSet, inputs, all);
                case nameof(SqlKind.EXCEPT):
                    return new AdoMinus(cluster, traitSet, inputs, all);
                default:
                    throw new AssertionError("unknown: " + kind);
            }
        }

    }

}
