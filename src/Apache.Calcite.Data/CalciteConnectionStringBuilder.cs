using System.Collections.Generic;
using System.Data.Common;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Builds and parses connection strings for the Apache Calcite ADO.NET provider.
    /// </summary>
    /// <remarks>
    /// Each property corresponds to a Calcite engine option. Set the properties you need, then pass
    /// <see cref="DbConnectionStringBuilder.ConnectionString"/> (or the builder itself, via the
    /// implicit <see langword="string"/> conversion) to <see cref="CalciteConnection"/> or
    /// <see cref="CalciteDataSource"/>. Properties not recognized by the builder are preserved in
    /// the connection string and forwarded to the engine as-is.
    /// </remarks>
    public sealed class CalciteConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Implicitly converts a <see cref="CalciteConnectionStringBuilder"/> to its connection string representation.
        /// </summary>
        /// <param name="builder">The builder to convert.</param>
        public static implicit operator string(CalciteConnectionStringBuilder builder) => builder.ConnectionString;

        /// <summary>
        /// Connection string key for the Calcite model file URI or inline JSON model.
        /// </summary>
        public const string ModelKey = "Model";

        /// <summary>
        /// Connection string key for the default schema.
        /// </summary>
        public const string SchemaKey = "Schema";

        /// <summary>
        /// Connection string key for whether the provider plans queries into the synchronous convention.
        /// </summary>
        public const string SynchronousKey = "Synchronous";

        /// <summary>
        /// Connection string key for how many prepared plans the connection may cache.
        /// </summary>
        public const string PlanCacheSizeKey = "PlanCacheSize";

        /// <summary>
        /// Connection string key for whether identifiers are matched case-sensitively.
        /// </summary>
        public const string CaseSensitiveKey = "CaseSensitive";

        /// <summary>
        /// Connection string key for the SQL conformance level.
        /// </summary>
        public const string ConformanceKey = "Conformance";

        /// <summary>
        /// Connection string key for the SQL parser factory class.
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
        /// Connection string key for the schema factory class name.
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
        /// Connection string key for the type system class name.
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
        public CalciteConnectionStringBuilder(string? connectionString)
        {
            ConnectionString = connectionString ?? string.Empty;
        }

        /// <summary>
        /// Gets or sets the Calcite model file URI or inline JSON model.
        /// </summary>
        public string? Model
        {
            get => TryGetString(ModelKey);
            set => SetOrRemove(ModelKey, value);
        }

        /// <summary>
        /// Gets or sets the default schema name.
        /// </summary>
        public string? Schema
        {
            get => TryGetString(SchemaKey);
            set => SetOrRemove(SchemaKey, value);
        }

        /// <summary>
        /// Gets or sets whether queries are planned into the synchronous convention. Default is
        /// <see langword="false"/>.
        /// </summary>
        /// <remarks>
        /// A provider option rather than an engine one, the way Calcite's own connection can ask for the
        /// bindable convention. By default every query is planned into the asynchronous convention, whichever
        /// entry point asked — a table that cannot produce rows asynchronously is still planned, carried
        /// across a converter, and that part of the plan completes synchronously. Setting this plans into the
        /// synchronous convention instead, for both entry points: <c>ReadAsync</c> answers with completed
        /// tasks, and a query touching a table that can <em>only</em> produce rows asynchronously fails to
        /// plan.
        /// </remarks>
        public bool? Synchronous
        {
            get => TryGetBool(SynchronousKey);
            set
            {
                if (value is null)
                    Remove(SynchronousKey);
                else
                    this[SynchronousKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets how many prepared plans the connection may cache. Default is none.
        /// </summary>
        /// <remarks>
        /// A provider option rather than an engine one, like <see cref="Synchronous"/>. A positive value
        /// gives the session a private least-recently-used cache of that many plans, consulted by
        /// statement text, so re-executing a statement skips parsing, validation and planning. Zero or
        /// absent means every execution plans from scratch, which is also Calcite's own behavior.
        /// <see cref="CalciteConnection.PlanCacheFactory"/> overrides this, for a cache of the caller's
        /// own — including one shared between connections.
        /// </remarks>
        public int? PlanCacheSize
        {
            get => TryGetInt(PlanCacheSizeKey);
            set
            {
                if (value is null)
                    Remove(PlanCacheSizeKey);
                else
                    this[PlanCacheSizeKey] = value.Value;
            }
        }

        /// <summary>
        /// Gets or sets a value indicating whether identifiers are matched case-sensitively.
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
        /// Gets or sets the SQL conformance level (e.g. <c>DEFAULT</c>, <c>STRICT_2003</c>, <c>PRAGMATIC_2003</c>).
        /// </summary>
        public string? Conformance
        {
            get => TryGetString(ConformanceKey);
            set => SetOrRemove(ConformanceKey, value);
        }

        /// <summary>
        /// Gets or sets the SQL parser factory class name used to extend Calcite's parser.
        /// For DDL support, set this to <c>org.apache.calcite.sql.parser.ddl.SqlDdlParserImpl#FACTORY</c>.
        /// </summary>
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
        /// Gets or sets whether Calcite should create materializations. Default is <c>false</c>.
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
        /// Gets or sets how NULL values should be sorted if neither NULLS FIRST nor NULLS LAST are specified.
        /// Default is <c>HIGH</c> (same as Oracle).
        /// </summary>
        public string? DefaultNullCollation
        {
            get => TryGetString(DefaultNullCollationKey);
            set => SetOrRemove(DefaultNullCollationKey, value);
        }

        /// <summary>
        /// Gets or sets how many rows the Druid adapter should fetch at a time when executing SELECT queries.
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
        /// Gets or sets the collection of built-in functions and operators.
        /// Valid values are <c>standard</c> (the default), <c>oracle</c>, <c>spatial</c>,
        /// and may be combined using commas, for example <c>oracle,spatial</c>.
        /// </summary>
        public string? Fun
        {
            get => TryGetString(FunKey);
            set => SetOrRemove(FunKey, value);
        }

        /// <summary>
        /// Gets or sets the lexical policy.
        /// Values are <c>BIG_QUERY</c>, <c>JAVA</c>, <c>MYSQL</c>, <c>MYSQL_ANSI</c>, <c>ORACLE</c> (default), <c>SQL_SERVER</c>.
        /// </summary>
        public string? Lex
        {
            get => TryGetString(LexKey);
            set => SetOrRemove(LexKey, value);
        }

        /// <summary>
        /// Gets or sets whether Calcite should use materializations. Default is <c>false</c>.
        /// </summary>
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
        /// Gets or sets the schema factory class name. Ignored if <see cref="Model"/> is specified.
        /// </summary>
        public string? SchemaFactory
        {
            get => TryGetString(SchemaFactoryKey);
            set => SetOrRemove(SchemaFactoryKey, value);
        }

        /// <summary>
        /// Gets or sets the schema type. Value must be <c>MAP</c> (the default), <c>JDBC</c>, or <c>CUSTOM</c>.
        /// Ignored if <see cref="Model"/> is specified.
        /// </summary>
        public string? SchemaType
        {
            get => TryGetString(SchemaTypeKey);
            set => SetOrRemove(SchemaTypeKey, value);
        }

        /// <summary>
        /// Gets or sets whether Spark should be used as the engine for processing that cannot be pushed to the source system.
        /// Default is <c>false</c>.
        /// </summary>
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
        /// Gets or sets the time zone, for example <c>gmt-3</c>. Default is the JVM's time zone.
        /// </summary>
        public string? TimeZone
        {
            get => TryGetString(TimeZoneKey);
            set => SetOrRemove(TimeZoneKey, value);
        }

        /// <summary>
        /// Gets or sets the type system class name.
        /// </summary>
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
