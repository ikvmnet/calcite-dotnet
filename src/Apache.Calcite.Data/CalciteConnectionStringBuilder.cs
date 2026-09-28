using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Builds and parses connection strings for the Apache Calcite ADO.NET provider.
    /// </summary>
    /// <remarks>
    /// <para>
    /// Most properties correspond to a Calcite connection property of the same name and are passed to Calcite
    /// when a connection opens; where one is not set, Calcite's default applies. <see cref="Model"/>,
    /// <see cref="Pooling"/>, <see cref="ConnectionIdleLifetime"/>, <see cref="ConnectionPruningInterval"/> and
    /// <see cref="TypeSystem"/> are interpreted by the provider instead. Keys are matched ignoring case. A key
    /// the builder does not recognize is kept and passed to Calcite unchanged, so any Calcite connection
    /// property can be set, and a <c>schema.</c>-prefixed key becomes an operand of the schema created by
    /// <see cref="SchemaFactory"/> or <see cref="SchemaType"/>.
    /// </para>
    /// <para>
    /// Pass <see cref="DbConnectionStringBuilder.ConnectionString"/>, or the builder itself through its implicit
    /// conversion to <see cref="string"/>, to <see cref="CalciteConnection"/> or <see cref="CalciteDataSource"/>.
    /// </para>
    /// </remarks>
    public sealed class CalciteConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Implicitly converts a <see cref="CalciteConnectionStringBuilder"/> to its connection string representation.
        /// </summary>
        /// <param name="builder">The builder to convert.</param>
        public static implicit operator string(CalciteConnectionStringBuilder builder) => builder.ConnectionString;

        /// <summary>
        /// Connection string key for the Calcite model: a file path, or inline JSON.
        /// </summary>
        public const string ModelKey = "Model";

        /// <summary>
        /// Connection string key for the default schema.
        /// </summary>
        public const string SchemaKey = "Schema";

        /// <summary>
        /// Connection string key for whether connections sharing this connection string share one root
        /// schema.
        /// </summary>
        public const string PoolingKey = "Pooling";

        /// <summary>
        /// Connection string key for how long, in seconds, the provider keeps a root schema no connection
        /// is using.
        /// </summary>
        public const string ConnectionIdleLifetimeKey = "Connection Idle Lifetime";

        /// <summary>
        /// Connection string key for how often, in seconds, the provider checks for root schemas to release.
        /// </summary>
        public const string ConnectionPruningIntervalKey = "Connection Pruning Interval";

        /// <summary>
        /// Connection string key for whether identifiers are matched case-sensitively.
        /// </summary>
        public const string CaseSensitiveKey = "CaseSensitive";

        /// <summary>
        /// Connection string key for the SQL conformance level.
        /// </summary>
        public const string ConformanceKey = "Conformance";

        /// <summary>
        /// Connection string key for the SQL parser factory.
        /// </summary>
        public const string ParserFactoryKey = "parserFactory";

        /// <summary>
        /// Connection string key for whether approximate results from aggregate functions on DECIMAL types are acceptable.
        /// </summary>
        public const string ApproximateDecimalKey = "approximateDecimal";

        /// <summary>
        /// Connection string key for whether approximate results from COUNT(DISTINCT ...) aggregate functions are acceptable.
        /// </summary>
        public const string ApproximateDistinctCountKey = "approximateDistinctCount";

        /// <summary>
        /// Connection string key for whether approximate results from "Top N" queries are acceptable.
        /// </summary>
        public const string ApproximateTopNKey = "approximateTopN";

        /// <summary>
        /// Connection string key for whether Calcite should create materializations.
        /// </summary>
        public const string CreateMaterializationsKey = "createMaterializations";

        /// <summary>
        /// Connection string key for how NULL values should be sorted.
        /// </summary>
        public const string DefaultNullCollationKey = "defaultNullCollation";

        /// <summary>
        /// Connection string key for how many rows the Druid adapter should fetch at a time.
        /// </summary>
        public const string DruidFetchKey = "druidFetch";

        /// <summary>
        /// Connection string key for whether the planner should try de-correlating as much as possible.
        /// </summary>
        public const string ForceDecorrelateKey = "forceDecorrelate";

        /// <summary>
        /// Connection string key for whether the de-correlation is done by the top-down general
        /// decorrelator.
        /// </summary>
        public const string TopDownGeneralDecorrelationEnabledKey = "topDownGeneralDecorrelationEnabled";

        /// <summary>
        /// Connection string key for the collection of built-in functions and operators.
        /// </summary>
        public const string FunKey = "fun";

        /// <summary>
        /// Connection string key for the lexical policy.
        /// </summary>
        public const string LexKey = "lex";

        /// <summary>
        /// Connection string key for whether Calcite should use materializations.
        /// </summary>
        public const string MaterializationsEnabledKey = "materializationsEnabled";

        /// <summary>
        /// Connection string key for how identifiers are quoted.
        /// </summary>
        public const string QuotingKey = "quoting";

        /// <summary>
        /// Connection string key for how quoted identifiers are stored.
        /// </summary>
        public const string QuotedCasingKey = "quotedCasing";

        /// <summary>
        /// Connection string key for how unquoted identifiers are stored.
        /// </summary>
        public const string UnquotedCasingKey = "unquotedCasing";

        /// <summary>
        /// Connection string key for the schema factory.
        /// </summary>
        public const string SchemaFactoryKey = "schemaFactory";

        /// <summary>
        /// Connection string key for the schema type.
        /// </summary>
        public const string SchemaTypeKey = "schemaType";

        /// <summary>
        /// Connection string key for whether Spark should be used as the processing engine.
        /// </summary>
        public const string SparkKey = "spark";

        /// <summary>
        /// Connection string key for the time zone.
        /// </summary>
        public const string TimeZoneKey = "timeZone";

        /// <summary>
        /// Connection string key for the type system, named as a .NET type.
        /// </summary>
        public const string TypeSystemKey = "typeSystem";

        /// <summary>
        /// Connection string key for whether implicit type coercion is applied during validation.
        /// </summary>
        public const string TypeCoercionKey = "typeCoercion";

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnectionStringBuilder"/> class.
        /// </summary>
        public CalciteConnectionStringBuilder()
        {

        }

        /// <summary>
        /// Initializes a new instance of the <see cref="CalciteConnectionStringBuilder"/> class
        /// populated from the specified connection string.
        /// </summary>
        /// <param name="connectionString">An existing connection string to parse, or <see langword="null"/> for an empty builder.</param>
        /// <exception cref="System.ArgumentException">The connection string is malformed.</exception>
        public CalciteConnectionStringBuilder(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the Calcite model that defines the root schema's schemas, as a file path or as inline JSON.
        /// </summary>
        /// <remarks>
        /// <para>
        /// A value that starts with <c>inline:</c> or <c>{</c> is read as JSON; any other value is a path to a
        /// model file, which must exist. The model is read when the first connection on a data source opens.
        /// See <see href="https://calcite.apache.org/docs/model.html">Calcite's model reference</see> for the
        /// format. A <c>defaultSchema</c> the model names takes precedence over <see cref="Schema"/>.
        /// </para>
        /// <para>
        /// Calcite loads a class a model names, such as a schema <c>factory</c> or a function <c>className</c>,
        /// only where the Java system property <c>calcite.model.classes.allowed</c> lists it or its package, and
        /// the list is empty by default. The property is read once, when Calcite first reads its system
        /// properties, so set it at application startup with <c>java.lang.System.setProperty</c> before the
        /// first connection opens. A .NET class must be listed under both its .NET name and its IKVM name, which
        /// is the .NET name prefixed with <c>cli.</c>.
        /// </para>
        /// </remarks>
        public string? Model
        {
            get => TryGetString(ModelKey);
            set => SetOrRemove(ModelKey, value);
        }

        /// <summary>
        /// Gets or sets the default schema, which resolves unqualified table names.
        /// </summary>
        /// <remarks>
        /// A <c>defaultSchema</c> named by the model takes precedence. Where <see cref="SchemaFactory"/> or
        /// <see cref="SchemaType"/> creates the schema, this is also its name, <c>adhoc</c> where not set.
        /// </remarks>
        public string? Schema
        {
            get => TryGetString(SchemaKey);
            set => SetOrRemove(SchemaKey, value);
        }

        /// <summary>
        /// Gets or sets whether connections sharing this connection string share one root schema. Default is
        /// <see langword="true"/>.
        /// </summary>
        /// <remarks>
        /// Interpreted by the provider. With pooling, every connection opened with an equivalent connection
        /// string draws on one <see cref="CalciteDataSource"/> kept for the process, so the model is read and its
        /// schemas built once, and a table created by DDL on one connection is visible on the others. With
        /// <see langword="false"/>, each connection builds a root schema of its own when it first opens and
        /// releases it when disposed. Two connection strings are equivalent when they have the same keys, in any
        /// order and casing, with the same values.
        /// </remarks>
        public bool? Pooling
        {
            get => TryGetBool(PoolingKey);
            set
            {
                if (value is null)
                    Remove(PoolingKey);
                else
                    this[PoolingKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets how long, in seconds, the provider keeps a root schema no connection is using before
        /// releasing it. Default is 300.
        /// </summary>
        /// <remarks>
        /// Interpreted by the provider. The data source the provider keeps for a connection string is released,
        /// and the disposable schemas on its root disposed, once no connection has been open on it for this long;
        /// the next connection opened with the string builds a new one. The check runs every
        /// <see cref="ConnectionPruningInterval"/> seconds, which must not exceed this value. A data source the
        /// application created is never released this way.
        /// </remarks>
        public int? ConnectionIdleLifetime
        {
            get => TryGetInt(ConnectionIdleLifetimeKey);
            set
            {
                if (value is null)
                    Remove(ConnectionIdleLifetimeKey);
                else
                    this[ConnectionIdleLifetimeKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets how often, in seconds, the provider checks for kept data sources that have passed their
        /// <see cref="ConnectionIdleLifetime"/>. Default is 10.
        /// </summary>
        /// <remarks>
        /// Interpreted by the provider. Must be greater than zero and not greater than
        /// <see cref="ConnectionIdleLifetime"/>; otherwise opening a connection, or creating a
        /// <see cref="CalciteDataSource"/>, throws <see cref="System.ArgumentException"/>.
        /// </remarks>
        public int? ConnectionPruningInterval
        {
            get => TryGetInt(ConnectionPruningIntervalKey);
            set
            {
                if (value is null)
                    Remove(ConnectionPruningIntervalKey);
                else
                    this[ConnectionPruningIntervalKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether identifiers are matched case-sensitively. Default is taken
        /// from <see cref="Lex"/>.
        /// </summary>
        public bool? CaseSensitive
        {
            get => TryGetBool(CaseSensitiveKey);
            set
            {
                if (value is null)
                    Remove(CaseSensitiveKey);
                else
                    this[CaseSensitiveKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets the SQL conformance level, a <c>SqlConformanceEnum</c> name such as <c>DEFAULT</c> (the
        /// default), <c>STRICT_2003</c> or <c>PRAGMATIC_2003</c>.
        /// </summary>
        public string? Conformance
        {
            get => TryGetString(ConformanceKey);
            set => SetOrRemove(ConformanceKey, value);
        }

        /// <summary>
        /// Gets or sets the SQL parser factory, as a Java class name, or <c>Class#FIELD</c> for a static field.
        /// </summary>
        /// <remarks>
        /// DDL requires a parser that accepts it. Calcite's is
        /// <c>org.apache.calcite.server.ServerDdlExecutor#PARSER_FACTORY</c>, in the <c>calcite-server</c>
        /// artifact, which this package does not reference; see the package README.
        /// </remarks>
        public string? ParserFactory
        {
            get => TryGetString(ParserFactoryKey);
            set => SetOrRemove(ParserFactoryKey, value);
        }

        /// <summary>
        /// Gets or sets whether approximate results from aggregate functions on <c>DECIMAL</c> types are acceptable.
        /// </summary>
        public bool? ApproximateDecimal
        {
            get => TryGetBool(ApproximateDecimalKey);
            set
            {
                if (value is null) Remove(ApproximateDecimalKey);
                else this[ApproximateDecimalKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets whether approximate results from <c>COUNT(DISTINCT ...)</c> aggregate functions are acceptable.
        /// </summary>
        public bool? ApproximateDistinctCount
        {
            get => TryGetBool(ApproximateDistinctCountKey);
            set
            {
                if (value is null) Remove(ApproximateDistinctCountKey);
                else this[ApproximateDistinctCountKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets whether approximate results from "Top N" queries (<c>ORDER BY aggFun() DESC LIMIT n</c>) are acceptable.
        /// </summary>
        public bool? ApproximateTopN
        {
            get => TryGetBool(ApproximateTopNKey);
            set
            {
                if (value is null) Remove(ApproximateTopNKey);
                else this[ApproximateTopNKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets whether Calcite should create materializations. Default is <c>true</c>.
        /// </summary>
        public bool? CreateMaterializations
        {
            get => TryGetBool(CreateMaterializationsKey);
            set
            {
                if (value is null) Remove(CreateMaterializationsKey);
                else this[CreateMaterializationsKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets how null values sort where a query specifies neither <c>NULLS FIRST</c> nor
        /// <c>NULLS LAST</c>: <c>HIGH</c> (the default, as in Oracle), <c>LOW</c>, <c>FIRST</c> or <c>LAST</c>.
        /// </summary>
        public string? DefaultNullCollation
        {
            get => TryGetString(DefaultNullCollationKey);
            set => SetOrRemove(DefaultNullCollationKey, value);
        }

        /// <summary>
        /// Gets or sets how many rows Calcite's Druid adapter fetches at a time. Default is 16384.
        /// </summary>
        public int? DruidFetch
        {
            get => TryGetInt(DruidFetchKey);
            set
            {
                if (value is null) Remove(DruidFetchKey);
                else this[DruidFetchKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets whether the planner should try de-correlating as much as possible. Default is <c>true</c>.
        /// </summary>
        public bool? ForceDecorrelate
        {
            get => TryGetBool(ForceDecorrelateKey);
            set
            {
                if (value is null) Remove(ForceDecorrelateKey);
                else this[ForceDecorrelateKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets whether de-correlation is done by <c>TopDownGeneralDecorrelator</c> rather than
        /// <c>RelDecorrelator</c>. Default is <c>false</c>.
        /// </summary>
        /// <remarks>
        /// This chooses which decorrelator runs; <see cref="ForceDecorrelate"/> decides whether one does.
        /// </remarks>
        public bool? TopDownGeneralDecorrelationEnabled
        {
            get => TryGetBool(TopDownGeneralDecorrelationEnabledKey);
            set
            {
                if (value is null) Remove(TopDownGeneralDecorrelationEnabledKey);
                else this[TopDownGeneralDecorrelationEnabledKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets the libraries of built-in functions and operators: <c>standard</c> (the default), or a
        /// comma-separated list of Calcite function library names such as <c>oracle</c>, <c>mysql</c> or
        /// <c>spatial</c>, for example <c>standard,oracle</c>.
        /// </summary>
        public string? Fun
        {
            get => TryGetString(FunKey);
            set => SetOrRemove(FunKey, value);
        }

        /// <summary>
        /// Gets or sets the lexical policy, which sets the defaults for quoting, identifier casing and case
        /// sensitivity. Values are <c>BIG_QUERY</c>, <c>JAVA</c>, <c>MYSQL</c>, <c>MYSQL_ANSI</c>, <c>ORACLE</c>
        /// (the default) and <c>SQL_SERVER</c>.
        /// </summary>
        public string? Lex
        {
            get => TryGetString(LexKey);
            set => SetOrRemove(LexKey, value);
        }

        /// <summary>
        /// Gets or sets whether Calcite should use materializations. Default is <c>true</c>.
        /// </summary>
        /// <remarks>
        /// This provider supplies no materializations to the planner, so materialized views are not substituted
        /// whatever this is set to.
        /// </remarks>
        public bool? MaterializationsEnabled
        {
            get => TryGetBool(MaterializationsEnabledKey);
            set
            {
                if (value is null) Remove(MaterializationsEnabledKey);
                else this[MaterializationsEnabledKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets how identifiers are quoted.
        /// Values are <c>DOUBLE_QUOTE</c>, <c>BACK_TICK</c>, <c>BACK_TICK_BACKSLASH</c>, <c>BRACKET</c>.
        /// If not specified, value from <see cref="Lex"/> is used.
        /// </summary>
        public string? Quoting
        {
            get => TryGetString(QuotingKey);
            set => SetOrRemove(QuotingKey, value);
        }

        /// <summary>
        /// Gets or sets how identifiers are stored if they are quoted.
        /// Values are <c>UNCHANGED</c>, <c>TO_UPPER</c>, <c>TO_LOWER</c>.
        /// If not specified, value from <see cref="Lex"/> is used.
        /// </summary>
        public string? QuotedCasing
        {
            get => TryGetString(QuotedCasingKey);
            set => SetOrRemove(QuotedCasingKey, value);
        }

        /// <summary>
        /// Gets or sets how identifiers are stored if they are not quoted.
        /// Values are <c>UNCHANGED</c>, <c>TO_UPPER</c>, <c>TO_LOWER</c>.
        /// If not specified, value from <see cref="Lex"/> is used.
        /// </summary>
        public string? UnquotedCasing
        {
            get => TryGetString(UnquotedCasingKey);
            set => SetOrRemove(UnquotedCasingKey, value);
        }

        /// <summary>
        /// Gets or sets the schema factory, as a Java class name, or <c>Class#FIELD</c> for a static field.
        /// Ignored if <see cref="Model"/> is specified.
        /// </summary>
        /// <remarks>
        /// The provider creates one schema with this factory, named by <see cref="Schema"/>, passing every
        /// <c>schema.</c>-prefixed key, without the prefix, as an operand. The schema is described to Calcite as a
        /// model, so the factory class must be allowed as described on <see cref="Model"/>.
        /// </remarks>
        public string? SchemaFactory
        {
            get => TryGetString(SchemaFactoryKey);
            set => SetOrRemove(SchemaFactoryKey, value);
        }

        /// <summary>
        /// Gets or sets the type of schema to create where neither <see cref="Model"/> nor
        /// <see cref="SchemaFactory"/> is specified.
        /// </summary>
        /// <remarks>
        /// <c>MAP</c> creates an empty schema and <c>JDBC</c> a <c>JdbcSchema</c>, configured by
        /// <c>schema.</c>-prefixed keys, as described on <see cref="SchemaFactory"/>. Any other value creates
        /// nothing. By default no schema is created. The schema is described to Calcite as a model naming
        /// Calcite's own factory class, which must be allowed as described on <see cref="Model"/>.
        /// </remarks>
        public string? SchemaType
        {
            get => TryGetString(SchemaTypeKey);
            set => SetOrRemove(SchemaTypeKey, value);
        }

        /// <summary>
        /// Gets or sets Calcite's <c>spark</c> property. Default is <c>false</c>.
        /// </summary>
        /// <remarks>
        /// This provider does not use Spark, whatever this is set to.
        /// </remarks>
        public bool? Spark
        {
            get => TryGetBool(SparkKey);
            set
            {
                if (value is null) Remove(SparkKey);
                else this[SparkKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets the session time zone, for example <c>UTC</c> or <c>gmt-3</c>. Default is the process's
        /// default time zone as the Java runtime reports it.
        /// </summary>
        public string? TimeZone
        {
            get => TryGetString(TimeZoneKey);
            set => SetOrRemove(TimeZoneKey, value);
        }

        /// <summary>
        /// Gets or sets the type system, a Calcite <c>RelDataTypeSystem</c> named as a .NET type or static
        /// member. Default is Calcite's default type system.
        /// </summary>
        /// <remarks>
        /// <para>
        /// Interpreted by the provider. A type with a public parameterless constructor, or with a public static
        /// <c>Instance</c> or <c>INSTANCE</c> member (which is preferred), is named as
        /// <c>Namespace.Type, Assembly</c>. The name is resolved with <see cref="System.Type.GetType(string)"/>,
        /// so the assembly can be omitted only for a type in this provider's assembly or the core library. A
        /// Java type system is named by its IKVM type and assembly.
        /// </para>
        /// <para>
        /// An instance held in a static field, property or parameterless method is named
        /// <c>[Namespace.Type, Assembly]::Member</c>, the notation of PowerShell and MSBuild property functions.
        /// Calcite's own type systems are reached this way, for example
        /// <c>[org.apache.calcite.sql.dialect.PostgresqlSqlDialect, calcite.core]::POSTGRESQL_TYPE_SYSTEM</c>.
        /// Calcite's <c>Type#MEMBER</c> notation is also accepted. Because these names contain a comma, quote the
        /// value in a connection string.
        /// </para>
        /// <para>
        /// The type system affects results, not only limits. Its precision and scale limits decide what is
        /// representable and where a value overflows. Its type derivations decide the SQL type, and therefore
        /// the .NET type, of an expression: with Calcite's default <c>deriveSumType</c>, <c>SUM</c> of an
        /// <c>INTEGER</c> column is read as <see cref="int"/>, and a type system that widens it to <c>BIGINT</c>
        /// makes it <see cref="long"/>. Its <c>roundingMode</c> (by default <c>DOWN</c>) decides how a numeric
        /// cast to a narrower type rounds.
        /// </para>
        /// </remarks>
        public string? TypeSystem
        {
            get => TryGetString(TypeSystemKey);
            set => SetOrRemove(TypeSystemKey, value);
        }

        /// <summary>
        /// Gets or sets whether implicit type coercion is applied when there is a type mismatch during SQL node validation.
        /// Default is <c>true</c>.
        /// </summary>
        public bool? TypeCoercion
        {
            get => TryGetBool(TypeCoercionKey);
            set
            {
                if (value is null) Remove(TypeCoercionKey);
                else this[TypeCoercionKey] = value.Value;
            }
        }

        /// <summary>
        /// Enumerates all keys currently present in this connection string.
        /// </summary>
        /// <returns>The keys in the order they are stored by the underlying <see cref="DbConnectionStringBuilder"/>.</returns>
        public IEnumerable<string> EnumerateKeys()
        {
            foreach (var key in Keys)
                if (key is string s)
                    yield return s;
        }

        /// <summary>
        /// The connection string a <see cref="CalciteDataSource"/> is looked up by.
        /// </summary>
        /// <remarks>
        /// Two connection strings that differ only in the order or the casing of their keys describe one
        /// data source, so the key is written with every key lower-cased and sorted.
        /// </remarks>
        internal string DataSourceKey
        {
            get
            {
                var keys = new List<string>();
                foreach (var key in EnumerateKeys())
                    keys.Add(key);

                keys.Sort(System.StringComparer.OrdinalIgnoreCase);

                var canonical = new DbConnectionStringBuilder();
                foreach (var key in keys)
                    canonical[key.ToLowerInvariant()] = this[key];

                return canonical.ConnectionString;
            }
        }

        string? TryGetString(string key)
        {
            return TryGetValue(key, out var v) ? v?.ToString() : null;
        }

        bool? TryGetBool(string key)
        {
            if (TryGetValue(key, out var v) == false || v is null)
                return null;

            if (v is bool b)
                return b;

            return bool.TryParse(v.ToString(), out var parsed) ? parsed : null;
        }

        int? TryGetInt(string key)
        {
            if (TryGetValue(key, out var v) == false || v is null)
                return null;

            if (v is int i)
                return i;

            return int.TryParse(v.ToString(), out var parsed) ? parsed : null;
        }

        void SetOrRemove(string key, string? value)
        {
            if (string.IsNullOrEmpty(value))
                Remove(key);
            else
                this[key] = value;
        }

    }

}
