using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.core;
using org.apache.calcite.rel.logical;
using org.apache.calcite.rex;
using org.apache.calcite.util;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that converts a <see cref="LogicalSort"/> over a <see cref="LogicalUnion"/> into a
    /// <see cref="ClrCursorMergeUnion"/>.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableMergeUnionRule</c> and uses its operand configuration, so the sort must be directly
    /// over the union. The sort is copied onto each input, with any limit pushed down as well, and the union
    /// then merges the sorted inputs. An offset or limit is kept above the union as a
    /// <see cref="ClrCursorLimit"/>.
    /// </remarks>
    public class ClrCursorMergeUnionRule : RelRule
    {

        /// <summary>
        /// Creates the rule from <c>EnumerableMergeUnionRule</c>'s default configuration.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorMergeUnionRule Create()
        {
            var config = org.apache.calcite.adapter.enumerable.EnumerableMergeUnionRule.Config.DEFAULT_CONFIG
                .withDescription("ClrCursorMergeUnionRule");

            return new ClrCursorMergeUnionRule(config);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule's configuration; its operands must match a <see cref="Sort"/> over a
        /// <see cref="Union"/>.</param>
        public ClrCursorMergeUnionRule(RelRule.Config config) :
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
                // pushing the limit down evaluates it once per input, so it is pushed only if it, and the
                // offset added to it, are deterministic
                var safeToReevaluate =
                    RexUtil.isDeterministic(sort.fetch) &&
                    (sort.offset == null || RexUtil.isDeterministic(sort.offset));

                if (safeToReevaluate)
                {
                    if (sort.offset == null)
                        inputFetch = sort.fetch;
                    else
                        // each input must supply offset + fetch rows; makeOffsetFetchSum also handles
                        // bounds that are not literals
                        inputFetch = RexUtil.makeOffsetFetchSum(sort.getCluster().getRexBuilder(), sort.offset, sort.fetch);
                }
            }

            var builder = call.builder();
            var unionFieldList = union.getRowType().getFieldList();
            var inputs = new java.util.ArrayList(union.getInputs().size());

            for (int i = 0; i < union.getInputs().size(); i++)
            {
                var input = (RelNode)union.getInputs().get(i);

                // a sort key whose type collation differs from the union's is cast to the union's type, so
                // that every input sorts the same way
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
                inputs.add(convert(call.getPlanner(), newInput, newInput.getTraitSet().replace(ClrCursorConvention.Instance)));
            }

            RelNode result = ClrCursorMergeUnion.Create(sort.getCollation(), inputs, union.all);

            // the merge union's rows are already in order, so only an offset or limit remains of the sort
            if (sort.offset != null || sort.fetch != null)
                result = ClrCursorLimit.Create(result, sort.offset, sort.fetch);

            call.transformTo(result);
        }

    }

}
