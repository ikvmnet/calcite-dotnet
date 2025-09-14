using System;
using System.Data;
using System.Data.Common;
using System.Threading.Tasks;

using java.lang;
using java.util;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.avatica;
using org.apache.calcite.schema;
using org.apache.calcite.sql;

using static org.apache.calcite.plan.SubstitutionVisitor;

namespace Apache.Calcite.Adapter.AdoNet
{

    public class AdoSchema : Schema
    {

#if NET8_0_OR_GREATER

        /// <summary>
        /// Wraps a <see cref="DbDataSource"/> in an <see cref="IAdoConnectionProvider"/>.
        /// </summary>
        class DbDataSourceConnectionProvider : IAdoConnectionProvider
        {

            readonly DbDataSource _ds;

            /// <summary>
            /// Initializes a new instance.
            /// </summary>
            /// <param name="ds"></param>
            public DbDataSourceConnectionProvider(DbDataSource ds)
            {
                _ds = ds ?? throw new ArgumentNullException(nameof(ds));
            }

            /// <inheritdoc />
            public DbConnection OpenConnection()
            {
                return _ds.OpenConnection();
            }

            /// <inheritdoc />
            public async ValueTask<DbConnection> OpenConnectionAsync()
            {
                return await _ds.OpenConnectionAsync();
            }

        }

#endif

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="ds"></param>
        /// <param name="catalog"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, IAdoConnectionProvider ds, string? catalog, string? schema)
        {
            return Create(parentSchema, name, ds, SqlDialectFactoryImpl.INSTANCE, catalog, schema);
        }

        /// <summary>
        /// Creates a new instance.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="ds"></param>
        /// <param name="dialectFactory"></param>
        /// <param name="catalog"></param>
        /// <param name="schema"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, IAdoConnectionProvider ds, SqlDialectFactory dialectFactory, string? catalog, string? schema)
        {
            var expression = Schemas.subSchemaExpression(parentSchema, name, typeof(AdoSchema));
            var dialect = createDialect(dialectFactory, ds);
            var convention = AdoConvention.Of(dialect, expression, name);
            return new AdoSchema(ds, dialect, convention, catalog, schema);
        }

        /// <summary>
        /// Creates a <see cref="AdoSchema"/>, taking credentials from a map.
        /// </summary>
        /// <param name="parentSchema"></param>
        /// <param name="name"></param>
        /// <param name="operand"></param>
        /// <returns></returns>
        public static AdoSchema Create(SchemaPlus parentSchema, string name, Map operand)
        {

            IAdoConnectionProvider ds;

            try
            {
                var dataSourceName = (string)operand.get("dataSource");
                if (dataSourceName != null)
                {
                    ds = (IAdoConnectionProvider)AvaticaUtils.instantiatePlugin(typeof(IAdoConnectionProvider), dataSourceName);
                }
                else
                {
                    final String jdbcUrl = (String)requireNonNull(operand.get("jdbcUrl"), "jdbcUrl");
                    final String jdbcDriver = (String)operand.get("jdbcDriver");
                    final String jdbcUser = (String)operand.get("jdbcUser");
                    final String jdbcPassword = (String)operand.get("jdbcPassword");
                    dataSource = dataSource(jdbcUrl, jdbcDriver, jdbcUser, jdbcPassword);
                }
            }
            catch (Exception e)
            {
                throw new RuntimeException("Error while reading dataSource", e);
            }
            String jdbcCatalog = (String)operand.get("jdbcCatalog");
            String jdbcSchema = (String)operand.get("jdbcSchema");
            String sqlDialectFactory = (String)operand.get("sqlDialectFactory");

            if (sqlDialectFactory == null || sqlDialectFactory.isEmpty())
            {
                return JdbcSchema.create(
                    parentSchema, name, dataSource, jdbcCatalog, jdbcSchema);
            }
            else
            {
                SqlDialectFactory factory =
                    AvaticaUtils.instantiatePlugin(SqlDialectFactory.class,
              sqlDialectFactory);
return JdbcSchema.create(parentSchema, name, dataSource, factory,
    jdbcCatalog, jdbcSchema);
    }
}

/// <summary>
/// Returns a suitable SQL dialect for the given data source.
/// </summary>
/// <param name="dialectFactory"></param>
/// <param name="ds"></param>
/// <returns></returns>
public static SqlDialect createDialect(SqlDialectFactory dialectFactory, IAdoConnectionProvider ds)
{
    return AdoUtils.DialectPool.INSTANCE.get(dialectFactory, ds);
}

readonly IAdoConnectionProvider _dbProvider;
readonly string? _catalog;
readonly string? _schema;
readonly SqlDialect _dialect;
readonly AdoConvention convention;

/// <summary>
/// Initializes a new instance.
/// </summary>
/// <param name="dbProvider"></param>
/// <param name="dialect"></param>
/// <param name="convention"></param>
/// <param name="catalog"></param>
/// <param name="schema"></param>
public AdoSchema(IAdoConnectionProvider dbProvider, SqlDialect dialect, AdoConvention convention, string? catalog, string? schema)
{
    _dbProvider = dbProvider ?? throw new ArgumentNullException(nameof(dbProvider));
    _dialect = dialect ?? throw new ArgumentNullException(nameof(dialect));
    this.convention = convention ?? throw new ArgumentNullException(nameof(convention));
    _catalog = catalog;
    _schema = schema;
}

    }

}
