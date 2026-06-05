using org.apache.calcite.adapter.enumerable;

using System.Linq.Expressions;

namespace Apache.Calcite.Adapter.Linq.Enumerable
{

    /// <summary>
    /// Initializes a new instance.
    /// </summary>
    /// <param name="expression"></param>
    /// <param name="physType"></param>
    /// <param name="format"></param>
    public class LinqEnumerableRelResult(LambdaExpression expression, PhysType physType, JavaRowFormat format)
    {

        /// <summary>
        /// The LINQ expression that produces the result of this relational expression. The expression is a lambda with no parameters, and
        /// its body is the root of the expression tree that produces the result. The type of the lambda is Func&lt;T&gt; where T is the
        /// physical type of the result rows. The expression tree may contain references to parameters from the surrounding context, which
        /// will be replaced with references to the corresponding parameters in the generated code.
        /// </summary>
        public readonly LambdaExpression Expression = expression;

        /// <summary>
        /// The physical type of the result rows. This is the type of the value returned by the lambda expression, and it may differ from the
        /// logical row type of the relational expression. The physical type includes information about how the result rows are represented
        /// in memory, such as whether they are represented as arrays, tuples, or custom classes. The physical type also includes
        /// information about the types of the individual fields in the result rows, which may differ from the types of the corresponding
        /// fields in the logical row type due to type coercions or optimizations.
        /// </summary>
        public readonly PhysType PhysType = physType;

        /// <summary>
        /// The format of the result rows. This indicates how the result rows are represented in memory, such as whether they are
        /// represented as arrays, tuples, or custom classes. The format also includes information about the types of the individual fields
        /// in the result rows, which may differ from the types of the corresponding fields in the logical row type due to type coercions
        /// or optimizations.
        /// </summary>
        public readonly JavaRowFormat Format = format;

    }

}
