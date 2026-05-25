using System;

using java.util.function;

using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Planner rule that converts a <see cref="Minus"/> (EXCEPT) expressed in the default calling
    /// convention to an <see cref="AdoMinus"/> in the <see cref="AdoConvention"/>.
    /// </summary>
    public class AdoMinusRule : AdoConverterRule
    {

        /// <summary>
        /// Creates a rule instance bound to the specified <see cref="AdoConvention"/>.
        /// </summary>
        /// <param name="convention">The ADO convention that this rule targets.</param>
        /// <returns>A configured <see cref="AdoMinusRule"/> instance.</returns>
        public static AdoMinusRule Create(AdoConvention convention)
        {
            return (AdoMinusRule)Config.INSTANCE
                .withConversion(typeof(Minus), Convention.NONE, convention, "AdoMinusRule")
                .withRuleFactory(new DelegateFunction<Config, AdoMinusRule>(c => new AdoMinusRule(c)))
                .toRule(typeof(AdoMinusRule));
        }

        /// <summary>
        /// Initializes a new instance using the supplied rule configuration.
        /// </summary>
        /// <param name="config">The rule configuration produced by <see cref="Create"/>.</param>
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
