using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Adapter.AdoNet.Rel.Convert
{

    abstract class AdoConverterRule : ConverterRule
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        protected internal AdoConverterRule(Config config) : 
            base(config)
        {

        }

    }

}