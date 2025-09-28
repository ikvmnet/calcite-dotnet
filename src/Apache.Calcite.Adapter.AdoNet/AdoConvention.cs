using System;

using Apache.Calcite.Adapter.AdoNet.Rel;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Calling convention for relational operations that occur in an ADO database.
    /// 
    /// <p>The convention is a slight misnomer. The operations occur in whatever
    /// data-flow architecture the database uses internally. Nevertheless, the result
    /// pops out in ADO.NET.</p>
    /// 
    /// <p>This is the only convention, thus far, that is not a singleton. Each
    /// instance contains a ADO.NET schema (and therefore a data source). If Calcite is
    /// working with two different databases, it would even make sense to convert
    /// from "JDBC#A" convention to "JDBC#B", even though we don't do it currently.
    /// (That would involve asking database B to open a database link to database
    /// A.)</p>
    /// 
    /// <p>As a result, converter rules from and to this convention need to be
    /// instantiated, at the start of planning, for each ADO database in play.</p>
    /// </summary>
    public class AdoConvention : Convention.Impl
    {

        /// <summary>
        /// Cost of a ADO node veruss implementing an equivalent node in a "typical" calling convention.
        /// </summary>
        public const double CostMultiplier = .8d;

        /// <summary>
        /// Creates a new ADO convention.
        /// </summary>
        /// <param name="dialect"></param>
        /// <param name="expression"></param>
        /// <param name="name"></param>
        /// <returns></returns>
        public static AdoConvention Create(SqlDialect dialect, Expression expression, string name)
        {
            return new AdoConvention(dialect, expression, name);
        }

        readonly SqlDialect _dialect;
        readonly Expression _expression;

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

            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        /// <summary>
        /// Gets the dialect of the convention.
        /// </summary>
        public SqlDialect Dialect => _dialect;

        /// <summary>
        /// Gets the expression of the convention.
        /// </summary>
        public Expression Expression => _expression;

        /// <inheritdoc />
        public override void register(RelOptPlanner planner)
        {
            foreach (var rule in AdoRules.GetRules(this))
                planner.addRule(rule);

            planner.addRule(CoreRules.FILTER_SET_OP_TRANSPOSE);
            planner.addRule(CoreRules.PROJECT_REMOVE);
        }

    }
}
