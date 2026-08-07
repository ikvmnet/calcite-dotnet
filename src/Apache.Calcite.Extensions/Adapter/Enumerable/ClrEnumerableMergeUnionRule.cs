using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Enumerable
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> over a <see cref="LogicalUnion"/> into a
    /// <see cref="ClrEnumerableMergeUnion"/>.
    /// </summary>
    /// <remarks>
    /// Not a converter rule: the sort has to be directly over the union for the operand to match, which is
    /// why a query that puts a projection between the two never reaches it — <c>SELECT * FROM a UNION ALL
    /// SELECT * FROM b ORDER BY 1</c> does, and the same query naming its columns does not. The sort is
    /// pushed to each input and the union then merges them.
    /// </remarks>
    public class ClrEnumerableMergeUnionRule : RelRule
    {

        /// <summary>
        /// Creates a <see cref="ClrEnumerableMergeUnionRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrEnumerableMergeUnionRule Create()
        {
            var config = org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule.Config.DEFAULT_CONFIG
                .withDescription("ClrEnumerableMergeUnionRule");

            return new ClrEnumerableMergeUnionRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrEnumerableMergeUnionRule(RelRule.Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override bool matches(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var collation = sort.getCollation();
            if (collation == null || collation.getFieldCollations().isEmpty())
                return false;

            var union = (Union)call.rel(1);

            return union.getInputs().size() >= 2;
        }

        /// <inheritdoc />
        public override void onMatch(RelOptRuleCall call)
        {
            var sort = (Sort)call.rel(0);
            var collation = sort.getCollation();
            var union = (Union)call.rel(1);

            // a limit can be pushed to each input, because a row past it on one input is past it on the union
            RexNode? inputFetch = null;
            if (sort.fetch != null)
            {
                if (sort.offset == null)
                    inputFetch = sort.fetch;
                else if (sort.fetch is RexLiteral fetch && sort.offset is RexLiteral offset)
                    inputFetch = call.builder().literal(RexLiteral.bigDecimalValue(fetch)!.add(RexLiteral.bigDecimalValue(offset)));
            }

            var builder = call.builder();
            var unionFieldList = union.getRowType().getFieldList();
            var inputs = new java.util.ArrayList(union.getInputs().size());

            for (int i = 0; i < union.getInputs().size(); i++)
            {
                var input = (RelNode)union.getInputs().get(i);

                // a field whose collation differs from the union's is cast, so that every input sorts the
                // same way
                var fieldsRequiringCastBuilder = ImmutableBitSet.builder();
                for (int j = 0; j < collation.getFieldCollations().size(); j++)
                {
                    var index = ((RelFieldCollation)collation.getFieldCollations().get(j)).getFieldIndex();
                    var unionType = ((org.apache.calcite.rel.type.RelDataTypeField)unionFieldList.get(index)).getType();
                    var inputType = ((org.apache.calcite.rel.type.RelDataTypeField)input.getRowType().getFieldList().get(index)).getType();

                    if (java.util.Objects.equals(unionType.getCollation(), inputType.getCollation()) == false)
                        fieldsRequiringCastBuilder.set(index);
                }

                var fieldsRequiringCast = fieldsRequiringCastBuilder.build();
                RelNode unsortedInput;
                if (fieldsRequiringCast.isEmpty())
                {
                    unsortedInput = input;
                }
                else
                {
                    builder.push(input);
                    var fields = builder.fields();
                    var projFields = new java.util.ArrayList(fields.size());
                    for (int j = 0; j < fields.size(); j++)
                    {
                        var node = (RexNode)fields.get(j);
                        if (fieldsRequiringCast.get(j))
                            node = builder.getRexBuilder().makeCast(((org.apache.calcite.rel.type.RelDataTypeField)unionFieldList.get(j)).getType(), node);

                        projFields.add(node);
                    }

                    builder.project(projFields);
                    unsortedInput = builder.build();
                }

                var newInput = sort.copy(sort.getTraitSet(), unsortedInput, collation, null, inputFetch);
                inputs.add(convert(call.getPlanner(), newInput, newInput.getTraitSet().replace(ClrEnumerableConvention.Instance)));
            }

            RelNode result = ClrEnumerableMergeUnion.Create(sort.getCollation(), inputs, union.all);

            // the merge union's rows are already in order, so an offset or a limit is all that is left of the
            // sort
            if (sort.offset != null || sort.fetch != null)
                result = ClrEnumerableLimit.Create(result, sort.offset, sort.fetch);

            call.transformTo(result);
        }

    }

}
