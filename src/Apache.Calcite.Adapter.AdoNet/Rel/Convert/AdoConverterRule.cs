using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    /// <summary>
    /// Base class for Calcite planner rules that convert a standard relational operator
    /// to its ADO.NET-convention counterpart.
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
