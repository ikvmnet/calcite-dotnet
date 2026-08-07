namespace Apache.Calcite.Tests
{

    /// <summary>
    /// A user-defined aggregate, which reaches Calcite's reflective implementor rather than one of
    /// <c>RexImpTable</c>'s own.
    /// </summary>
    /// <remarks>
    /// The three members are named as Java names them, because <c>AggregateFunctionImpl.create</c> finds them
    /// by name through reflection. Static, so the accumulator is the value they pass between them.
    ///
    /// <para>Top level rather than nested in the test, because <c>EnumerableConvention</c> writes the class
    /// name into generated Java source and hands it to Janino: a nested CLR type is named with a <c>+</c>,
    /// which is not something a Java compiler will parse.</para>
    /// </remarks>
    public class SumAggregate
    {

        /// <summary>
        /// Returns the starting value of the accumulator.
        /// </summary>
        /// <returns></returns>
        public static int init() => 0;

        /// <summary>
        /// Folds one value into the accumulator.
        /// </summary>
        /// <param name="accumulator"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public static int add(int accumulator, int value) => accumulator + value;

        /// <summary>
        /// Returns the accumulator as the aggregate's result.
        /// </summary>
        /// <param name="accumulator"></param>
        /// <returns></returns>
        public static int result(int accumulator) => accumulator;

    }

}
