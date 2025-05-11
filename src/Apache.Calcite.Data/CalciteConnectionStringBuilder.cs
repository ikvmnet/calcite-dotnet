using System;
using System.Data.Common;

using java.util;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.model;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Strongly-typed JDBC connection string builder.
    /// </summary>
    public class CalciteConnectionStringBuilder : DbConnectionStringBuilder
    {

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnectionStringBuilder()
        {

        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnectionStringBuilder(string connectionString)
        {
            ConnectionString = connectionString;
        }

        /// <summary>
        /// Copies the connection string values to a <see cref="CalciteConnectionConfig"/>.
        /// </summary>
        /// <returns></returns>
        public CalciteConnectionConfig ToConfig() => new CalciteConnectionConfigImpl(ToProperties());

        /// <summary>
        /// copies the connection string values to a <see cref="Properties"/>.
        /// </summary>
        /// <returns></returns>
        Properties ToProperties()
        {
            var prop = new Properties();
            foreach (string key in Keys)
                if (TryGetValue(key, out var v) && v is string s)
                    prop.setProperty(key, s);

            return prop;
        }

        /// <summary>
        /// Gets a typed connection string property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <returns></returns>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        T GetValue<T>(CalciteConnectionProperty property)
        {
            if (property is null)
                throw new ArgumentNullException(nameof(property));

            if (typeof(T) == typeof(bool))
                return (T)(object)(TryGetValue(property.camelName(), out var v) && v is string b ? b : ((java.lang.Boolean)property.defaultValue()).booleanValue());

            if (typeof(T) == typeof(int))
                return (T)(object)(TryGetValue(property.camelName(), out var v) && v is string i ? i : ((java.lang.Integer)property.defaultValue()).intValue());

            if (typeof(T) == typeof(string))
                return (T)(object)(TryGetValue(property.camelName(), out var v) && v is string s ? s : ((string)property.defaultValue()));

            if (typeof(java.lang.Enum).IsAssignableFrom(typeof(T)))
                return (T)(TryGetValue(property.camelName(), out var v) && v is string e ? java.lang.Enum.valueOf(typeof(T), e) : property.defaultValue());

            throw new InvalidOperationException();
        }

        /// <summary>
        /// Sets a typed connection string property.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <param name="value"></param>
        /// <exception cref="ArgumentNullException"></exception>
        /// <exception cref="InvalidOperationException"></exception>
        void SetValue<T>(CalciteConnectionProperty property, T value)
        {
            if (property is null)
                throw new ArgumentNullException(nameof(property));

            if (typeof(T) == typeof(bool))
                this[property.camelName()] = ((bool?)(object?)value).ToString();

            if (typeof(T) == typeof(int))
                this[property.camelName()] = ((int?)(object?)value).ToString();

            if (typeof(T) == typeof(string))
                this[property.camelName()] = (string?)(object?)value;

            if (typeof(java.lang.Enum).IsAssignableFrom(typeof(T)))
                this[property.camelName()] = ((java.lang.Enum?)(object?)value)?.name() ?? throw new InvalidOperationException();

            throw new InvalidOperationException();
        }

        /// <summary>
        /// Whether approximate results from <c>COUNT(DISTINCT ...)</c> aggregate functions are acceptable. 
        /// </summary>
        public bool ApproximateDistinctCount
        {
            get => GetValue<bool>(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT);
            set => SetValue<bool>(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT, value);
        }

        /// <summary>
        /// Whether approximate results from "Top N" queries(<c>ORDER BY aggFun DESC LIMIT n</c>) are acceptable.
        /// </summary>
        public bool ApproximateTopN
        {
            get => GetValue<bool>(CalciteConnectionProperty.APPROXIMATE_TOP_N);
            set => SetValue<bool>(CalciteConnectionProperty.APPROXIMATE_TOP_N, value);
        }

        public bool ApproximateDecimal
        {
            get => GetValue<bool>(CalciteConnectionProperty.APPROXIMATE_DECIMAL);
            set => SetValue<bool>(CalciteConnectionProperty.APPROXIMATE_DECIMAL, value);
        }

        public bool NullEqualToEmpty
        {
            get => GetValue<bool>(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY);
            set => SetValue<bool>(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY, value);
        }

        public bool AutoTemp
        {
            get => GetValue<bool>(CalciteConnectionProperty.AUTO_TEMP);
            set => SetValue<bool>(CalciteConnectionProperty.AUTO_TEMP, value);
        }

        public bool MaterializationsEnabled
        {
            get => GetValue<bool>(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED);
            set => SetValue<bool>(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED, value);
        }

        public bool CreateMaterializations
        {
            get => GetValue<bool>(CalciteConnectionProperty.CREATE_MATERIALIZATIONS);
            set => SetValue<bool>(CalciteConnectionProperty.CREATE_MATERIALIZATIONS, value);
        }

        public NullCollation DefaultNullCollation
        {
            get => GetValue<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION);
            set => SetValue<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION, value);
        }

        /// <summary>
        /// Collection of built-in functions and operators. Valid values include "standard", "bigquery", "calcite",
        /// "hive", "mssql", "mysql", "oracle", "postgresql", "redshift", "snowflake","spark", "spatial" and "all"
        /// (operators that could be used in all libraries except "standard" and "spatial"), and also
        /// comma-separated lists, for example "oracle,spatial".
        /// </summary>
        public string Fun
        {
            get => GetValue<string>(CalciteConnectionProperty.FUN);
            set => SetValue<string>(CalciteConnectionProperty.FUN, value);
        }

        /// <summary>
        /// URI of the model.
        /// </summary>
        public string Model
        {
            get => GetValue<string>(CalciteConnectionProperty.MODEL);
            set => SetValue<string>(CalciteConnectionProperty.MODEL, value);
        }

        /// <summary>
        ///  Lexical policy.
        /// </summary>
        public Lex Lex
        {
            get => GetValue<Lex>(CalciteConnectionProperty.LEX);
            set => SetValue<Lex>(CalciteConnectionProperty.LEX, value);
        }

        /// <summary>
        /// How identifiers are quoted.
        /// </summary>
        public Quoting Quoting
        {
            get => GetValue<Quoting>(CalciteConnectionProperty.QUOTING);
            set => SetValue<Quoting>(CalciteConnectionProperty.QUOTING, value);
        }

        /// <summary>
        /// How identifiers are stored if they are quoted.
        /// </summary>
        public Casing UnquotedCasing
        {
            get => GetValue<Casing>(CalciteConnectionProperty.UNQUOTED_CASING);
            set => SetValue<Casing>(CalciteConnectionProperty.UNQUOTED_CASING, value);
        }

        /// <summary>
        /// How identifiers are stored if they are not quoted.
        /// </summary>
        public Casing QuotedCasing
        {
            get => GetValue<Casing>(CalciteConnectionProperty.QUOTED_CASING);
            set => SetValue<Casing>(CalciteConnectionProperty.QUOTED_CASING, value);
        }

        /// <summary>
        /// Whether identifiers are matched case-sensitively.
        /// </summary>
        public bool CaseSensitive
        {
            get => GetValue<bool>(CalciteConnectionProperty.CASE_SENSITIVE);
            set => SetValue<bool>(CalciteConnectionProperty.CASE_SENSITIVE, value);
        }

        /// <summary>
        /// Parser factory.
        /// </summary>
        /// <remarks>
        /// The name of a class that implements <see cref="org.apache.calcite.sql.parser.SqlParserImplFactory"/>.
        /// </remarks>
        public string ParserFactory
        {
            get => GetValue<string>(CalciteConnectionProperty.PARSER_FACTORY);
            set => SetValue<string>(CalciteConnectionProperty.PARSER_FACTORY, value);
        }

        /// <summary>
        /// MetaTableFactory plugin.
        /// </summary>
        public string MetaTableFactory
        {
            get => GetValue<string>(CalciteConnectionProperty.META_TABLE_FACTORY);
            set => SetValue<string>(CalciteConnectionProperty.META_TABLE_FACTORY, value);
        }

        /// <summary>
        /// MetaColumnFactory plugin.
        /// </summary>
        public string MetaColumnFactory
        {
            get => GetValue<string>(CalciteConnectionProperty.META_COLUMN_FACTORY);
            set => SetValue<string>(CalciteConnectionProperty.META_COLUMN_FACTORY, value);
        }

        /// <summary>
        /// Schema factory. Ignored if <see cref="Model"/> is specified.
        /// </summary>
        /// <remarks>
        /// The name of a class that implements <see cref="org.apache.calcite.schema.SchemaFactory"/>.
        /// </remarks>
        public string SchemaFactory
        {
            get => GetValue<string>(CalciteConnectionProperty.META_COLUMN_FACTORY);
            set => SetValue<string>(CalciteConnectionProperty.META_COLUMN_FACTORY, value);
        }

        /// <summary>
        /// Schema type.
        /// </summary>
        public JsonSchema.Type SchemaType
        {
            get => GetValue<JsonSchema.Type>(CalciteConnectionProperty.SCHEMA_TYPE);
            set => SetValue<JsonSchema.Type>(CalciteConnectionProperty.SCHEMA_TYPE, value);
        }

        public bool Spark
        {
            get => GetValue<bool>(CalciteConnectionProperty.SPARK);
            set => SetValue<bool>(CalciteConnectionProperty.SPARK, value);
        }

        public bool ForceDecorrelate
        {
            get => GetValue<bool>(CalciteConnectionProperty.FORCE_DECORRELATE);
            set => SetValue<bool>(CalciteConnectionProperty.FORCE_DECORRELATE, value);
        }

        public string TypeSystem
        {
            get => GetValue<string>(CalciteConnectionProperty.TYPE_SYSTEM);
            set => SetValue<string>(CalciteConnectionProperty.TYPE_SYSTEM, value);
        }

        public SqlConformanceEnum Conformance
        {
            get => GetValue<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE);
            set => SetValue<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE, value);
        }

        public string TimeZone
        {
            get => GetValue<string>(CalciteConnectionProperty.TIME_ZONE);
            set => SetValue<string>(CalciteConnectionProperty.TIME_ZONE, value);
        }

        public string Locale
        {
            get => GetValue<string>(CalciteConnectionProperty.LOCALE);
            set => SetValue<string>(CalciteConnectionProperty.LOCALE, value);
        }

        public bool TypeCoercion
        {
            get => GetValue<bool>(CalciteConnectionProperty.TYPE_COERCION);
            set => SetValue<bool>(CalciteConnectionProperty.TYPE_COERCION, value);
        }

        public bool LenientOperatorLookup
        {
            get => GetValue<bool>(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP);
            set => SetValue<bool>(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, value);
        }

        public bool TopDownOpt
        {
            get => GetValue<bool>(CalciteConnectionProperty.TOPDOWN_OPT);
            set => SetValue<bool>(CalciteConnectionProperty.TOPDOWN_OPT, value);
        }

    }

}
