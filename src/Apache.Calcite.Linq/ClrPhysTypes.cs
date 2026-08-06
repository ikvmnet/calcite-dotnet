using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.function;

using Apache.Calcite.Linq.Tree;

namespace Apache.Calcite.Linq
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
    static class ClrPhysTypes
    {

        /// <summary>
        /// Brings a sequence of rows into another row format.
        /// </summary>
        /// <param name="implementor"></param>
        /// <param name="physType"></param>
        /// <param name="source"></param>
        /// <param name="targetFormat"></param>
        /// <returns></returns>
        /// <remarks>
        /// What PhysType.convertTo does, which cannot be reused because it composes a linq4j select onto a
        /// linq4j sequence and the sequence here is not one. The selector is still PhysType's.
        /// </remarks>
        public static System.Linq.Expressions.Expression ConvertTo(ClrEnumerableRelImplementor implementor, PhysType physType, System.Linq.Expressions.Expression source, JavaRowFormat targetFormat)
        {
            if (physType.getFormat() == targetFormat)
                return source;

            var sourceType = ClrTypes.Resolve(physType.getJavaRowType());
            var row = org.apache.calcite.linq4j.tree.Expressions.parameter(physType.getJavaRowType(), "o");
            var fields = new java.util.ArrayList();
            for (int i = 0; i < physType.getRowType().getFieldCount(); i++)
                fields.add(java.lang.Integer.valueOf(i));

            var selector = implementor.Translator.TranslateSelector(physType.generateSelector(row, fields, targetFormat), sourceType);

            return System.Linq.Expressions.Expression.Call(null,
                ClrBuiltInMethod.Select.MakeGenericMethod(sourceType, selector.ReturnType),
                source,
                selector);
        }

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
                : implementor.Translator.Translate(comparer);
        }

    }

}
