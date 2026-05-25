using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Collects the correlation variables needed to construct an <see cref="AdoCorrelationDataContext"/>
    /// for a correlated sub-query.
    /// </summary>
    public interface IAdoCorrelationDataContextBuilder
    {

        /// <summary>
        /// Registers a single correlation variable.
        /// </summary>
        /// <param name="id">The Calcite correlation identifier for the variable.</param>
        /// <param name="ordinal">The ordinal position of the field within the outer row.</param>
        /// <param name="type">The Java/IKVM reflection type of the field value.</param>
        /// <returns>The ordinal at which the variable was registered.</returns>
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type);

    }

}
