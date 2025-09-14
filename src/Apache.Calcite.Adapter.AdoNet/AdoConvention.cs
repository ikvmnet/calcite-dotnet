using System;

using java.lang;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Calling convention for relational operations that occur in an ADO database.
    /// </summary>
    public class AdoConvention : Convention.Impl
    {

        public static AdoConvention Of(SqlDialect dialect, Expression expression, string name)
        {
            return new AdoConvention(dialect, expression, name);
        }

        readonly SqlDialect dialect;
        readonly Expression expression;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="expression"></param>
        /// <param name="name"></param>
        public AdoConvention(SqlDialect dialect, Expression expression, string name) :
            base("ADO." + name, typeof(AdoRel))
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException($"'{nameof(name)}' cannot be null or empty.", nameof(name));

            this.dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            this.expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        /// <inheritdoc />
        public override void register(RelOptPlanner planner)
        {
            foreach (var rule in AdoRules.Rules(this))
                planner.addRule(rule);

            planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
            planner.addRule(CoreRules.PROJECT_REMOVE);
        }

    }
}
