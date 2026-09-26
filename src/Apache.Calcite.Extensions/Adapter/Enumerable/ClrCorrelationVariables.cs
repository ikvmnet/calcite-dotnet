using System.Collections.Generic;

using org.apache.calcite.rel;
using org.apache.calcite.rel.type;
using org.apache.calcite.rex;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Finds the correlation variables a sub-plan reads, with the row type of each.
    /// </summary>
    /// <remarks>
    /// <c>RelOptUtil.getVariablesUsed</c> answers the names; the type is what a converter out of a Clr
    /// convention needs as well, to build the physical type the sub-plan reads the outer row through, and
    /// every <see cref="RexCorrelVariable"/> carries it.
    /// </remarks>
    static class ClrCorrelationVariables
    {

        /// <summary>
        /// Returns the correlation variables referenced anywhere under a node, in the order first met.
        /// </summary>
        /// <param name="rel"></param>
        /// <returns></returns>
        public static IReadOnlyList<(string Name, RelDataType Type)> Used(RelNode rel)
        {
            var collector = new Collector();
            rel.accept(collector);

            return collector.Found;
        }

        sealed class Collector : RelHomogeneousShuttle
        {

            readonly Rex rex;

            public Collector()
            {
                rex = new Rex(this);
            }

            public List<(string Name, RelDataType Type)> Found { get; } = [];

            public override RelNode visit(RelNode other)
            {
                other.accept(rex);

                return base.visit(other);
            }

            sealed class Rex(Collector collector) : RexShuttle
            {

                public override RexNode visitCorrelVariable(RexCorrelVariable variable)
                {
                    var name = variable.id.getName();
                    if (collector.Found.Exists(v => v.Name == name) == false)
                        collector.Found.Add((name, variable.getType()));

                    return variable;
                }

            }

        }

    }

}
