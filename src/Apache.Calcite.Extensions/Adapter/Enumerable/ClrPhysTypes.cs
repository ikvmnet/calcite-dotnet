using System;
using System.Linq.Expressions;

using Apache.Calcite.Extensions.Adapter.Enumerable;
using Apache.Calcite.Extensions.Linq4j.Tree;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.linq4j.function;

using J = org.apache.calcite.linq4j.tree;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
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
        ///
        /// <para>The rows come out boxed, as every sequence here carries them. PhysType's selector answers
        /// with the physical field type — CALCITE-3364's one-column conversion ends in
        /// <c>public int apply(Object[] o)</c> — and a caller of this hands what it gets back up to the node
        /// above.</para>
        /// </remarks>
        public static System.Linq.Expressions.Expression ConvertTo(this PhysType physType, ClrEnumerableRelImplementor implementor, System.Linq.Expressions.Expression source, JavaRowFormat targetFormat)
        {
            if (physType.getFormat() == targetFormat)
                return source;

            var sourceType = physType.RowType();
            var row = org.apache.calcite.linq4j.tree.Expressions.parameter(physType.getJavaRowType(), "o");
            var fields = new java.util.ArrayList();
            for (int i = 0; i < physType.getRowType().getFieldCount(); i++)
                fields.add(java.lang.Integer.valueOf(i));

            var selector = implementor.Translator.TranslateSelector(physType.generateSelector(row, fields, targetFormat), sourceType);
            var rowType = PhysTypeImpl.of(implementor.TypeFactory, physType.getRowType(), targetFormat, false).RowType();

            if (selector.ReturnType != rowType)
            {
                var parameter = Expression.Parameter(sourceType, "row");

                selector = Expression.Lambda(
                    typeof(Func<,>).MakeGenericType(sourceType, rowType),
                    ClrEnumUtils.Convert(Expression.Invoke(selector, parameter), rowType),
                    parameter);
            }

            return System.Linq.Expressions.Expression.Call(null,
                ClrBuiltInMethod.Select.MakeGenericMethod(sourceType, rowType),
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
        public static Expression Comparer(this PhysType physType, ClrEnumerableRelImplementor implementor)
        {
            var comparer = physType.comparer();

            return comparer == null
                ? Expression.Constant(null, typeof(EqualityComparer))
                : implementor.Translator.Translate(comparer);
        }

        /// <summary>
        /// Returns the CLR type of one row of a sequence of the given physical type.
        /// </summary>
        /// <param name="physType"></param>
        /// <returns></returns>
        /// <remarks>
        /// The row type, boxed. A row of a plan holds Java's values and keeps holding them: a field inside an
        /// <c>Object[]</c> row is a <c>java.lang.Integer</c> from the table to the converter that hands it
        /// out, and a one column row is that same value with no array around it. A node may unbox inside
        /// itself, where the values never leave the expression it is building, but what it hands to the node
        /// above has to be what the row type says.
        ///
        /// <para>Calcite has nothing to decide here: there is no <c>Enumerable&lt;int&gt;</c> in Java, so its
        /// sequences carry the box whatever the physical type says, and its element type is erased besides. A
        /// CLR sequence says its element type out loud, so every node has to say the same thing.</para>
        /// </remarks>
        public static Type RowType(this PhysType physType)
        {
            ArgumentNullException.ThrowIfNull(physType);

            return ClrTypes.Resolve(J.Primitive.box(physType.getJavaRowType()));
        }

    }

}
