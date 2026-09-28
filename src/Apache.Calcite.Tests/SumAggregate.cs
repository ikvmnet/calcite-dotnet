namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A user-defined integer sum, which Calcite implements through its reflective aggregate implementor
    /// rather than one of <c>RexImpTable</c>'s own.
    /// </summary>
    /// <remarks>
    /// The members carry the lower-case names <c>AggregateFunctionImpl.create</c> looks up by reflection. They
    /// are static, so the accumulator is the value passed between them.
    /// </remarks>
    public class SumAggregate
    {

        /// <summary>
        /// Returns the starting value of the accumulator.
        /// </summary>
        /// <returns>Zero.</returns>
        public static int init() => 0;

        /// <summary>
        /// Adds one value to the accumulator.
        /// </summary>
        /// <param name="accumulator">The running sum.</param>
        /// <param name="value">The value to add.</param>
        /// <returns>The new running sum.</returns>
        public static int add(int accumulator, int value) => accumulator + value;

        /// <summary>
        /// Returns the accumulator as the aggregate's result.
        /// </summary>
        /// <param name="accumulator">The running sum.</param>
        /// <returns>The sum.</returns>
        public static int result(int accumulator) => accumulator;

    }

}
