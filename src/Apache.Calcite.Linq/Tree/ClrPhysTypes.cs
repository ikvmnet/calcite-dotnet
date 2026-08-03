using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.function;

namespace Apache.Calcite.Linq.Tree
{

    /// <summary>
    /// The members of <see cref="PhysType"/> that yield a linq4j expression, brought across as they are asked
    /// for.
    /// </summary>
    /// <remarks>
    /// <c>PhysType</c> answers every question about a row, and the answers that are expressions are linq4j
    /// because Calcite hands them to Janino. Each is translated where it is produced rather than composed into
    /// a larger linq4j tree first, so a node only ever holds one of these for as long as it takes to translate.
    /// </remarks>
    public static class ClrPhysTypes
    {

        /// <summary>
        /// Returns the comparer for rows of a physical type, or a null constant where the rows compare
        /// themselves.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <returns></returns>
        /// <remarks>
        /// A row of <c>JavaRowFormat.ARRAY</c> is an array, whose own equality is by reference, so a set
        /// operation over one is wrong without this.
        /// </remarks>
        public static Expression Comparer(ClrEnumerableRelImplementor implementor, PhysType physType)
        {
            var comparer = physType.comparer();

            return comparer == null
                ? Expression.Constant(null, typeof(EqualityComparer))
                : JavaCast.To(implementor.Translator.Translate(comparer), typeof(EqualityComparer));
        }

    }

}
