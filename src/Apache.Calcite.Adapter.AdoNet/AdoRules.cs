using System;
using System.Collections.Generic;

using com.google.common.collect;

using java.util.function;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.rel;
using org.apache.calcite.rel.convert;
using org.apache.calcite.rel.core;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Rules and relational operators for the <see cref="AdoConvention"/> calling convention.
    /// </summary>
    class AdoRules
    {
        
        /// <summary>
        /// <see cref="Function"/> implementation that simply calls the default constructor of the given type.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <typeparam name="TResult"></typeparam>
        class DelegateFunction<T, TResult> : Function
        {

            readonly Func<T, TResult> _delegate;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="delegate"></param>
            /// <exception cref="ArgumentNullException"></exception>
            public DelegateFunction(Func<T, TResult> @delegate)
            {
                _delegate = @delegate ?? throw new ArgumentNullException(nameof(@delegate));
            }

            public object? apply(object? t)
            {
                return _delegate((T?)t ?? throw new InvalidOperationException());
            }

            public Function compose(Function before)
            {
                return Function.__DefaultMethods.compose(this, before);
            }

            public Function andThen(Function after)
            {
                return Function.__DefaultMethods.andThen(this, after);
            }

        }

        /// <summary>
        /// Abstract base class for rule that converts to ADO.
        /// </summary>
        abstract class AdoConverterRule : ConverterRule
        {

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="config"></param>
            protected internal AdoConverterRule(Config config) :
                base(config)
            {

            }

        }

        /// <summary>
        /// Rule that converts a join to ADO.
        /// </summary>
        class AdoJoinRule : AdoConverterRule
        {

            /// <summary>
            /// Creates a <see cref="AdoJoinRule"/>.
            /// </summary>
            /// <param name="convention"></param>
            /// <returns></returns>
            public static AdoJoinRule Create(JdbcConvention convention)
            {
                return (AdoJoinRule)Config.INSTANCE
                    .withConversion(typeof(Join), Convention.NONE, convention, nameof(AdoJoinRule))
                    .withRuleFactory(new DelegateFunction<Config, AdoJoinRule>(c => new AdoJoinRule(c)))
                    .toRule(typeof(AdoJoinRule));
            }

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="config"></param>
            public AdoJoinRule(Config config) :
                base(config)
            {

            }

            public override RelNode? convert(RelNode rel)
            {
                var join = (Join)rel;
                switch ((JoinRelType.__Enum)join.getJoinType().ordinal())
                {
                    case JoinRelType.__Enum.SEMI:
                    case JoinRelType.__Enum.ANTI:
                        // It's not possible to convert semi-joins or anti-joins. They have fewer columns
                        // than regular joins.
                        return null;
                    default:
                        return convert(join, true);
                }
            }

            /// <summary>
            /// Converts a <see cref="Join"/> into a <see cref="AdoJoin"/>.
            /// </summary>
            /// <param name="join"></param>
            /// <param name="convertInputTraits"></param>
            /// <returns></returns>
            public RelNode? convert(Join join, bool convertInputTraits)
            {

            }

        }

        /// <summary>
        /// Creates a list of rules with the given JDBC convention instance.
        /// </summary>
        /// <param name="convention"></param>
        /// <returns></returns>
        /// <exception cref="NotImplementedException"></exception>
        public static IEnumerable<RelOptRule> Rules(AdoConvention convention)
        {
            var b = ImmutableList.builder();
        }

        static IEnumerable<RelRule> GetRules(AdoConvention convention)
        {
            yield return AdoToEnumerableConverterRule.Create(convention);
        }

    }

}
