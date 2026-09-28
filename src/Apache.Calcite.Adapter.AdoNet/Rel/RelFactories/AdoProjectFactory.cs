using System;

using java.util;

using org.apache.calcite.rel;
using org.apache.calcite.rex;
using org.apache.calcite.sql.validate;

using static org.apache.calcite.rel.core.RelFactories;

namespace Apache.Calcite.Adapter.AdoNet.Rel.RelFactories
{

    /// <summary>
    /// A <see cref="ProjectFactory"/> that creates an <see cref="AdoProject"/> with its input's traits.
    /// </summary>
    public class AdoProjectFactory : ProjectFactory
    {

        /// <inheritdoc />
        /// <remarks>
        /// TODO: this throws when <paramref name="variablesSet"/> is empty, where <c>JdbcProjectFactory</c> throws when
        /// it is not.
        /// </remarks>
        public RelNode createProject(RelNode input, List hints, List projects, List fieldNames, Set variablesSet)
        {
            if (variablesSet.isEmpty())
                throw new ArgumentException("AdoProject does not allow variables");

            var cluster = input.getCluster();
            var rowType = RexUtil.createStructType(cluster.getTypeFactory(), projects, fieldNames, SqlValidatorUtil.F_SUGGESTER);
            return new AdoProject(cluster, input.getTraitSet(), input, projects, rowType);
        }

        /// <summary>
        /// Not implemented.
        /// </summary>
        /// <param name="input">The input.</param>
        /// <param name="hints">The hints.</param>
        /// <param name="childExprs">The projected expressions.</param>
        /// <param name="fieldNames">The field names.</param>
        /// <returns>Does not return.</returns>
        /// <exception cref="NotImplementedException">Always.</exception>
        public RelNode createProject(RelNode input, List hints, List childExprs, List fieldNames)
        {
            throw new NotImplementedException();
        }

    }

}
