using System;

using java.lang;
using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.sql;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="SetOpFactory"/> implementation that creates <see cref="AdoUnion"/>,
    /// <see cref="AdoIntersect"/>, and <see cref="AdoMinus"/> nodes during relational-algebra
    /// construction in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoSetOpFactory : SetOpFactory
    {

        /// <inheritdoc />
        public RelNode createSetOp(SqlKind kind, List inputs, bool all)
        {
            var input = (RelNode)inputs.get(0);
            var cluster = input.getCluster();
            var traitSet = cluster.traitSetOf(input.getConvention() ?? throw new ArgumentNullException("input.getConvention()"));

            // by name, never by ordinal: a Java enum's ordinals are an artefact of declaration order in the
            // version compiled against, and nameof gives a compile-time constant the compiler still checks
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
