using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// An interface to collect all correlation variables required to create a <see cref="AdoCorrelecationDataContext"/>.
    /// </summary>
    interface IAdoCorrelationDataContextBuilder
    {

        /// <summary>
        /// Collect a correlation variable.
        /// </summary>
        /// <param name="id"></param>
        /// <param name="ordinal"></param>
        /// <param name="type"></param>
        /// <returns></returns>
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type);

    }

}