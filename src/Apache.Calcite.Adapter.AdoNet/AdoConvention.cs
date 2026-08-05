using System;

using Apache.Calcite.Adapter.AdoNet.Rel;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// Calcite calling convention that identifies relational nodes whose results are produced
    /// by executing SQL against a specific ADO.NET data source.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Unlike most Calcite conventions, <see cref="AdoConvention"/> is not a singleton: each
    /// instance is bound to a particular <see cref="AdoDataSource"/> and its <see cref="SqlDialect"/>.
    /// When a query touches two different ADO.NET databases, the planner uses two separate convention
    /// instances and inserts the necessary converter nodes between them.
    /// </para>
    /// <para>
    /// Use <see cref="Create"/> to obtain an instance. Converter rules that push relational
    /// operators into the ADO layer are registered per-instance at the start of planning.
    /// </para>
    /// </remarks>
    public class AdoConvention : Convention.Impl
    {

        /// <summary>
        /// Relative cost multiplier applied to ADO relational nodes during planning.
        /// A value below 1.0 makes the planner prefer pushing operations into the ADO layer
        /// over equivalent in-memory implementations.
        /// </summary>
        public const double CostMultiplier = .8d;

        /// <summary>
        /// Creates a new <see cref="AdoConvention"/> for the given SQL dialect, schema expression, and display name.
        /// </summary>
        /// <param name="dialect">The SQL dialect used to generate SQL for the target database.</param>
        /// <param name="expression">The LINQ expression that represents the schema root at planning time.</param>
        /// <param name="name">A short display name appended to the convention identifier (e.g. the schema name).</param>
        /// <returns>A new <see cref="AdoConvention"/> instance.</returns>
        public static AdoConvention Create(SqlDialect dialect, Expression expression, string name)
        {
            return new AdoConvention(dialect, expression, name);
        }

        readonly SqlDialect _dialect;
        readonly Expression _expression;

        /// <summary>
        /// Initializes a new instance of <see cref="AdoConvention"/>.
        /// Prefer <see cref="Create"/> over calling this constructor directly.
        /// </summary>
        /// <param name="dialect">The SQL dialect used to generate SQL for the target database.</param>
        /// <param name="expression">The LINQ expression that represents the schema root at planning time.</param>
        /// <param name="name">A short display name appended to the convention identifier.</param>
        public AdoConvention(SqlDialect dialect, Expression expression, string name) :
            base("ADO." + name, typeof(AdoRel))
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException($"'{nameof(name)}' cannot be null or empty.", nameof(name));

            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        /// <summary>
        /// Gets the SQL dialect used when generating SQL for the target ADO.NET database.
        /// </summary>
        public SqlDialect Dialect => _dialect;

        /// <summary>
        /// Gets the LINQ expression that represents the schema root for this convention at planning time.
        /// </summary>
        public Expression Expression => _expression;

        /// <inheritdoc />
        public override void register(RelOptPlanner planner)
        {
            foreach (var rule in AdoRules.GetRules(this))
                planner.addRule(rule);

            planner.addRule(CoreRules.PROJECT_REMOVE);
        }

    }
}
