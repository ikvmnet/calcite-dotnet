using System;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;

using Apache.Calcite.Extensions.Adapter.Enumerable;

namespace Apache.Calcite.Extensions.Adapter.AsyncEnumerable
{

    /// <summary>
    /// Implementation of <see cref="Combine"/> in the <see cref="ClrAsyncEnumerableConvention"/> calling
    /// convention.
    /// </summary>
    /// <remarks>
    /// Combines several query roots into one, which is what multi-root optimisation in the Volcano planner
    /// needs. The output is one column per query, each row holding that query's values for that row index as
    /// a map; the row count is the largest of the inputs, and a query with fewer rows contributes null.
    ///
    /// <para>New in 1.42. No SQL statement produces a <see cref="Combine"/> — a caller builds one with
    /// <c>RelBuilder.combine</c> — so the differential harness cannot reach this by parsing a query.</para>
    /// </remarks>
    public class ClrAsyncEnumerableCombine : Combine, ClrAsyncEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        public ClrAsyncEnumerableCombine(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs) :
            base(cluster, traitSet, inputs)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrAsyncEnumerableCombine(getCluster(), traitSet, inputs);
        }

        /// <inheritdoc />
        public ClrAsyncEnumerableResult Implement(ClrAsyncEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var lists = new java.util.ArrayList();

            for (int ord = 0; ord < getInputs().size(); ord++)
            {
                var input = (ClrAsyncEnumerableRel)getInputs().get(ord);
                var result = implementor.VisitChild(this, ord, input, pref);

                var source = result.Expression;
                var sourceType = source.Type.GetGenericArguments()[0];
                var row = Expression.Parameter(sourceType, $"row{ord}");

                var fields = input.getRowType().getFieldList();
                var fieldCount = fields.size();

                // one name and one value per field, which is what SqlFunctions.map takes. A row of one field
                // is the value itself, because its physical row format is SCALAR
                var args = new Expression[fieldCount * 2];
                for (int i = 0; i < fieldCount; i++)
                {
                    args[i * 2] = Expression.Constant(((RelDataTypeField)fields.get(i)).getName(), typeof(object));
                    args[i * 2 + 1] = fieldCount > 1
                        ? Expression.ArrayIndex(Expression.Convert(row, typeof(object[])), Expression.Constant(i))
                        : ClrEnumUtils.Convert(row, typeof(object));
                }

                var selector = Expression.Lambda(
                    typeof(Func<,>).MakeGenericType(sourceType, typeof(java.util.Map)),
                    Expression.Call(null, MapMethod, Expression.NewArrayInit(typeof(object), args)),
                    row);

                var mapped = ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.Select.MakeGenericMethod(sourceType, typeof(java.util.Map)),
                    source,
                    selector);

                // the sequence itself, where the synchronous node reads it into a list here. Each read has to
                // be awaited and an expression tree cannot await, so the reading moves into the operator and
                // what the tree carries is the input
                lists.add(mapped);
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.ARRAY));

            var arguments = new Expression[lists.size()];
            for (int i = 0; i < lists.size(); i++)
                arguments[i] = (Expression)lists.get(i);

            // which function combines the lists stays this node's decision, as it is in the other
            // convention; the operator only does the reading
            var read = Expression.Parameter(typeof(java.util.List[]), "lists");
            var combine = Expression.Lambda<Func<java.util.List[], java.util.List>>(
                Expression.Call(null, CombineQueryResultsMethod, read),
                read);

            return implementor.Result(physType,
                ClrAsyncBuiltInMethod.Call(ClrAsyncBuiltInMethod.CombineQueryResults.MakeGenericMethod(typeof(object[])),
                    Expression.NewArrayInit(typeof(System.Collections.Generic.IAsyncEnumerable<java.util.Map>), arguments),
                    combine));
        }

        /// <summary>
        /// <c>SqlFunctions.map</c>, which builds one query's row as a map of column name to value.
        /// </summary>
        static readonly System.Reflection.MethodInfo MapMethod = typeof(SqlFunctions)
            .GetMethod(nameof(SqlFunctions.map), [typeof(object[])])
            ?? throw new InvalidOperationException("'SqlFunctions.map' is missing.");

        /// <summary>
        /// <c>SqlFunctions.combineQueryResults</c>, which lines the queries up by row index.
        /// </summary>
        static readonly System.Reflection.MethodInfo CombineQueryResultsMethod = typeof(SqlFunctions)
            .GetMethod(nameof(SqlFunctions.combineQueryResults), [typeof(java.util.List[])])
            ?? throw new InvalidOperationException("'SqlFunctions.combineQueryResults' is missing.");

    }

}
