using java.util.function;

using org.apache.calcite.interpreter;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.DataCursor
{

    /// <summary>
    /// Rule that reads a relational expression of <c>BindableConvention</c> as one of the
    /// <see cref="ClrDataCursorConvention"/> calling convention, by interpreting it.
    /// </summary>
    /// <remarks>
    /// The counterpart of <c>EnumerableInterpreterRule</c>, and the same 0.5 cost factor.
    ///
    /// <para>Not in what <see cref="ClrDataCursorRules.Rules"/> returns, because <c>TO_INTERPRETER</c> is not
    /// in <c>ENUMERABLE_RULES</c> either: Calcite registers it from <c>RelOptUtil.registerDefaultRules</c>,
    /// which is Calcite's own function and registers Calcite's rule. A caller who wants an interpreted node
    /// to land in this convention rather than in <c>EnumerableConvention</c> under a converter adds this
    /// one.</para>
    /// </remarks>
    public class ClrDataCursorInterpreterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrDataCursorInterpreterRule"/>.
        /// </summary>
        /// <returns></returns>
        public static ClrDataCursorInterpreterRule Create()
        {
            return (ClrDataCursorInterpreterRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(RelNode), BindableConvention.INSTANCE, ClrDataCursorConvention.Instance, "ClrDataCursorInterpreterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrDataCursorInterpreterRule>(c => new ClrDataCursorInterpreterRule(c)))
                .toRule(typeof(ClrDataCursorInterpreterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config"></param>
        public ClrDataCursorInterpreterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return ClrDataCursorInterpreter.Create(rel, 0.5d);
        }

    }

}
