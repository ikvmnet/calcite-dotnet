using System;

using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// The rule that converts a logical <see cref="Minus"/> (<c>EXCEPT</c>) into an <see cref="AdoMinus"/>. An
    /// <c>EXCEPT ALL</c> is not converted. Mirrors <c>JdbcRules.JdbcMinusRule</c>.
    /// </summary>
    public class AdoMinusRule : AdoConverterRule
    {

        /// <summary>
        /// Creates the rule for a convention.
        /// </summary>
        /// <param name="convention">The convention converted to.</param>
        /// <returns>The rule.</returns>
        public static AdoMinusRule Create(AdoConvention convention)
        {
            return (AdoMinusRule)Config.INSTANCE
                .withConversion(typeof(Minus), Convention.NONE, convention, "AdoMinusRule")
                .withRuleFactory(new DelegateFunction<Config, AdoMinusRule>(c => new AdoMinusRule(c)))
                .toRule(typeof(AdoMinusRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The configuration <see cref="Create"/> builds.</param>
        public AdoMinusRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            var minus = (Minus)rel;
            if (minus.all)
                return null;

            var traitSet = rel.getTraitSet().replace(@out);
            return new AdoMinus(rel.getCluster(), traitSet, convertList(minus.getInputs(), @out), false);
        }

    }

}
