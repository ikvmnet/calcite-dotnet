using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Base ADO rule.
    /// </summary>
    public abstract class AdoConverterRule : ConverterRule
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        protected AdoConverterRule(Config config) :
            base(config)
        {

        }

    }

}