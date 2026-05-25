using System;

using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// <see cref="ProjectFactory"/> implementation that creates <see cref="AdoProject"/> nodes
    /// during relational-algebra construction in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoProjectFactory : ProjectFactory
    {

        /// <inheritdoc />
        public RelNode createProject(RelNode input, List hints, List projects, List fieldNames, Set variablesSet)
        {
            if (variablesSet.isEmpty())
                throw new ArgumentException("AdoProject does not allow variables");

            var cluster = input.getCluster();
            var rowType = RexUtil.createStructType(cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER);
            return new AdoProject(cluster, input.getTraitSet(), input, projects, rowType);
        }

        /// <inheritdoc />
        public RelNode createProject(RelNode input, List hints, List childExprs, List fieldNames)
        {
            throw new NotImplementedException();
        }

    }

}
