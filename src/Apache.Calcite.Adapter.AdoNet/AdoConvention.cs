using System;

using Apache.Calcite.Adapter.AdoNet.Rel;

using org.apache.calcite.linq4j.tree;
using org.apache.calcite.plan;
using org.apache.calcite.rel.rules;
using org.apache.calcite.sql;

namespace Apache.Calcite.Adapter.AdoNet
{

    /// <summary>
    /// The calling convention of the nodes that run as SQL against one ADO.NET data source. The counterpart of
    /// Calcite's <c>JdbcConvention</c>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Each <see cref="AdoSchema"/> has its own instance, so nodes over two different sources are in different
    /// conventions and are never combined into one statement. The planner joins them in process, through the
    /// converters into <c>EnumerableConvention</c> or <c>ClrCursorConvention</c>.
    /// </para>
    /// <para>
    /// When the planner first meets a node of this convention, <see cref="register"/> adds the rules of
    /// <see cref="AdoRules.GetRules(AdoConvention)"/> for this instance.
    /// </para>
    /// </remarks>
    public class AdoConvention : Convention.Impl
    {

        /// <summary>
        /// The factor <see cref="Rel.AdoProject"/> and <see cref="Rel.AdoUnion"/> multiply their cost by, so that the
        /// planner prefers them to an equivalent node in process. Mirrors <c>JdbcConvention.COST_MULTIPLIER</c>.
        /// </summary>
        public const double CostMultiplier = .8d;

        /// <summary>
        /// Creates a convention.
        /// </summary>
        /// <param name="dialect">The dialect SQL is written in.</param>
        /// <param name="syntax">How the driver names a parameter, and any rewrite the statement needs.</param>
        /// <param name="expression">The linq4j expression that reaches the schema at run time, from which generated
        /// code obtains the <see cref="AdoDataSource"/>.</param>
        /// <param name="name">The schema's name. The convention is named <c>ADO.</c> followed by it.</param>
        /// <returns>The convention.</returns>
        public static AdoConvention Create(SqlDialect dialect, IAdoSqlSyntax syntax, Expression expression, string name)
        {
            return new AdoConvention(dialect, syntax, expression, name);
        }

        readonly SqlDialect _dialect;
        readonly IAdoSqlSyntax _syntax;
        readonly Expression _expression;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="dialect">The dialect SQL is written in.</param>
        /// <param name="syntax">How the driver names a parameter, and any rewrite the statement needs.</param>
        /// <param name="expression">The linq4j expression that reaches the schema at run time, from which generated
        /// code obtains the <see cref="AdoDataSource"/>.</param>
        /// <param name="name">The schema's name. The convention is named <c>ADO.</c> followed by it.</param>
        /// <exception cref="ArgumentException"><paramref name="name"/> is null or empty.</exception>
        /// <exception cref="ArgumentNullException"><paramref name="dialect"/>, <paramref name="syntax"/> or
        /// <paramref name="expression"/> is <see langword="null"/>.</exception>
        public AdoConvention(SqlDialect dialect, IAdoSqlSyntax syntax, Expression expression, string name) :
            base("ADO." + name, typeof(AdoRel))
        {
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException($"'{nameof(name)}' cannot be null or empty.", nameof(name));

            _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
            _syntax = syntax ?? throw new ArgumentNullException(nameof(syntax));
            _expression = expression ?? throw new ArgumentNullException(nameof(expression));
        }

        /// <summary>
        /// Gets the dialect SQL is written in.
        /// </summary>
        public SqlDialect Dialect => _dialect;

        /// <summary>
        /// Gets the linq4j expression that reaches the schema at run time.
        /// </summary>
        public Expression Expression => _expression;

        /// <summary>
        /// Gets how the driver names a parameter, and any rewrite the statement needs.
        /// </summary>
        public IAdoSqlSyntax Syntax => _syntax;

        /// <summary>
        /// Adds this convention's rules, and <c>CoreRules.PROJECT_REMOVE</c>, to the planner.
        /// </summary>
        /// <param name="planner">The planner.</param>
        public override void register(RelOptPlanner planner)
        {
            foreach (var rule in AdoRules.GetRules(this))
                planner.addRule(rule);

            planner.addRule(CoreRules.PROJECT_REMOVE);
        }

    }
}
