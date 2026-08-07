using System;
using System.Linq.Expressions;

using org.apache.calcite.adapter.enumerable;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.type;
using org.apache.calcite.runtime;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Implementation of <see cref="Combine"/> in the <see cref="ClrEnumerableConvention"/> calling
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
    public class ClrEnumerableCombine : Combine, ClrEnumerableRel
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="cluster"></param>
        /// <param name="traitSet"></param>
        /// <param name="inputs"></param>
        public ClrEnumerableCombine(RelOptCluster cluster, RelTraitSet traitSet, java.util.List inputs) :
            base(cluster, traitSet, inputs)
        {

        }

        /// <inheritdoc />
        public override RelNode copy(RelTraitSet traitSet, java.util.List inputs)
        {
            return new ClrEnumerableCombine(getCluster(), traitSet, inputs);
        }

        /// <inheritdoc />
        public ClrEnumerableResult Implement(ClrEnumerableRelImplementor implementor, ClrEnumerablePrefer pref)
        {
            var lists = new java.util.ArrayList();

            for (int ord = 0; ord < getInputs().size(); ord++)
            {
                var input = (ClrEnumerableRel)getInputs().get(ord);
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

                var mapped = Expression.Call(null,
                    ClrBuiltInMethod.Select.MakeGenericMethod(sourceType, typeof(java.util.Map)),
                    source,
                    selector);

                lists.add(Expression.Call(null, ClrBuiltInMethod.ToJavaList.MakeGenericMethod(typeof(java.util.Map)), mapped));
            }

            var physType = ClrPhysTypeImpl.Of(implementor.TypeFactory, getRowType(), pref.Prefer(JavaRowFormat.ARRAY));

            var arguments = new Expression[lists.size()];
            for (int i = 0; i < lists.size(); i++)
                arguments[i] = (Expression)lists.get(i);

            var combined = Expression.Call(null, CombineQueryResultsMethod, Expression.NewArrayInit(typeof(java.util.List), arguments));

            return implementor.Result(physType,
                Expression.Call(null, ClrBuiltInMethod.FromJavaList.MakeGenericMethod(typeof(object[])), combined));
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
