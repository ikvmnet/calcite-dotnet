using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Collects the fields of the outer row that a pushed-down statement reads as parameters, for building the
    /// <see cref="AdoCorrelationDataContext"/> the statement's values come from.
    /// </summary>
    public interface IAdoCorrelationDataContextBuilder
    {

        /// <summary>
        /// Registers one field of a correlation variable and returns the dynamic parameter index to write for it.
        /// </summary>
        /// <param name="id">The correlation variable.</param>
        /// <param name="ordinal">The field's ordinal in the outer row.</param>
        /// <param name="type">The Java class of the field's value.</param>
        /// <returns>The index; <see cref="AdoCorrelationDataContext"/> answers <c>?</c> followed by it with the
        /// field's value.</returns>
        public int Add(CorrelationId id, int ordinal, java.lang.reflect.Type type);

    }

}
