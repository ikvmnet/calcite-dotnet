using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Base class of the rules that convert a logical node into its <see cref="AdoConvention"/> counterpart.
    /// Mirrors <c>JdbcRules.JdbcConverterRule</c>.
    /// </summary>
    public abstract class AdoConverterRule : ConverterRule
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule's configuration, naming the node class and the two conventions.</param>
        protected AdoConverterRule(Config config) :
            base(config)
        {

        }

    }

}
