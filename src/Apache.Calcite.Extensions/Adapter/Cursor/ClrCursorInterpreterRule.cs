using java.util.function;

using org.apache.calcite.interpreter;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;

namespace Apache.Calcite.Extensions.Adapter.Cursor
{

    /// <summary>
    /// Rule that reads a relational expression of <c>BindableConvention</c> as one of the
    /// <see cref="ClrCursorConvention"/> calling convention, by interpreting it.
    /// </summary>
    /// <remarks>
    /// Mirrors <c>EnumerableInterpreterRule</c>, with the same cost factor of 0.5.
    ///
    /// <para>Not among <see cref="ClrCursorRules.Rules"/>, just as <c>TO_INTERPRETER</c> is not among
    /// <c>ENUMERABLE_RULES</c>; <c>RelOptUtil.registerDefaultRules</c> registers Calcite's rule, so by default an
    /// interpreted node lands in <c>EnumerableConvention</c> under a converter. A caller who wants it in this
    /// convention adds this rule.</para>
    /// </remarks>
    public class ClrCursorInterpreterRule : ConverterRule
    {

        /// <summary>
        /// Creates a <see cref="ClrCursorInterpreterRule"/>.
        /// </summary>
        /// <returns>The rule.</returns>
        public static ClrCursorInterpreterRule Create()
        {
            return (ClrCursorInterpreterRule)Config.INSTANCE
                .withConversion((java.lang.Class)typeof(RelNode), BindableConvention.INSTANCE, ClrCursorConvention.Instance, "ClrCursorInterpreterRule")
                .withRuleFactory(new DelegateFunction<Config, ClrCursorInterpreterRule>(c => new ClrCursorInterpreterRule(c)))
                .toRule(typeof(ClrCursorInterpreterRule));
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="config">The rule configuration.</param>
        public ClrCursorInterpreterRule(Config config) :
            base(config)
        {

        }

        /// <inheritdoc />
        public override RelNode? convert(RelNode rel)
        {
            return ClrCursorInterpreter.Create(rel, 0.5d);
        }

    }

}
