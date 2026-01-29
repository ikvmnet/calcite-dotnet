using Apache.Calcite.Extensions;

using java.util;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Data
{

    /// <summary>
    /// Provides typed access to <see cref="CalciteConnectionProperty"/> instances stored in a <see cref="Properties"/>.
    /// </summary>
    public class CalciteConnectionProperties
    {

        readonly Properties _properties;
        readonly CalciteConnectionPropertiesSchemaMap _schemaMap;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        /// <param name="properties"></param>
        public CalciteConnectionProperties(Properties properties)
        {
            _properties = properties;
            _schemaMap = new CalciteConnectionPropertiesSchemaMap(_properties);
        }

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public CalciteConnectionProperties()
        {
            _properties = new Properties();
            _schemaMap = new CalciteConnectionPropertiesSchemaMap(_properties);
        }

        /// <summary>
        /// Gets a Calcite connection property that is a Java enum.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <returns></returns>
        T GetEnum<T>(CalciteConnectionProperty property)
            where T : java.lang.Enum
        {
            if (_properties.containsKey(property.camelName()))
                return (T)java.lang.Enum.valueOf(typeof(T), _properties.getProperty(property.camelName()));
            else
                return (T)property.defaultValue();
        }

        /// <summary>
        /// Sets a Calcite connection property that is a Java enum.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <param name="value"></param>
        void SetEnum<T>(CalciteConnectionProperty property, T value)
            where T : java.lang.Enum
        {
            if (value is null)
                _properties.remove(property.camelName());
            else
                _properties.setProperty(property.camelName(), value.name());
        }

        /// <summary>
        /// Gets a Calcite connection property that is a Java enum.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <returns></returns>
        T? GetNullableEnum<T>(CalciteConnectionProperty property)
            where T : java.lang.Enum
        {
            if (_properties.containsKey(property.camelName()))
                return (T?)(T)java.lang.Enum.valueOf(typeof(T), _properties.getProperty(property.camelName()));
            else
                return (T?)property.defaultValue();
        }

        /// <summary>
        /// Sets a Calcite connection property that is a Java enum.
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="property"></param>
        /// <param name="value"></param>
        void SetNullableEnum<T>(CalciteConnectionProperty property, T? value)
            where T : java.lang.Enum
        {
            if (value is null)
                _properties.remove(property.camelName());
            else
                _properties.setProperty(property.camelName(), value.name());
        }

        /// <summary>
        /// Gets a Calcite connection property that is a boolean.
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        bool GetBoolean(CalciteConnectionProperty property)
        {
            return bool.Parse(_properties.getProperty(property.camelName(), ((java.lang.Boolean?)property.defaultValue())?.booleanValue() == true ? "true" : "false"));
        }

        /// <summary>
        /// Sets a Calcite connection property that is a boolean.
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        void SetBoolean(CalciteConnectionProperty property, bool value)
        {
            _properties.setProperty(property.camelName(), value ? "true" : "false");
        }

        /// <summary>
        /// Gets a Calcite connection property that is an integer.
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        int GetInteger(CalciteConnectionProperty property)
        {
            return int.Parse(_properties.getProperty(property.camelName(), ((java.lang.Integer)property.defaultValue()).toString()));
        }

        /// <summary>
        /// Sets a Calcite connection property that is an integer.
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        void SetInteger(CalciteConnectionProperty property, int value)
        {
            _properties.setProperty(property.camelName(), value.ToString());
        }

        /// <summary>
        /// Gets a Calcite connection property that is a string.
        /// </summary>
        /// <param name="property"></param>
        /// <returns></returns>
        string GetString(CalciteConnectionProperty property)
        {
            return _properties.getProperty(property.camelName(), (string)property.defaultValue());
        }

        /// <summary>
        /// Sets a Calcite connection property that is a string.
        /// </summary>
        /// <param name="property"></param>
        /// <param name="value"></param>
        void SetString(CalciteConnectionProperty property, string value)
        {
            _properties.setProperty(property.camelName(), value);
        }

        /// <summary>
        /// Whether approximate results from aggregate functions on DECIMAL types are acceptable.
        /// </summary>
        public bool ApproximateDecimal
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_DECIMAL);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_DECIMAL, value);
        }

        /// <summary>
        /// Whether approximate results from COUNT(DISTINCT ...) aggregate functions are acceptable.
        /// </summary>
        public bool ApproximateDistinctCount
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT, value);
        }

        /// <summary>
        /// Whether approximate results from "Top N" queries (ORDER BY aggFun DESC LIMIT n) are acceptable.
        /// </summary>
        public bool ApproximateTopN
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_TOP_N);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_TOP_N, value);
        }

        /// <summary>
        /// Whether to store query results in temporary tables.
        /// </summary>
        public bool AutoTemp
        {
            get => GetBoolean(CalciteConnectionProperty.AUTO_TEMP);
            set => SetBoolean(CalciteConnectionProperty.AUTO_TEMP, value);
        }

        /// <summary>
        /// Whether Calcite should use materializations.
        /// </summary>
        public bool MaterializationsEnabled
        {
            get => GetBoolean(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED);
            set => SetBoolean(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED, value);
        }

        /// <summary>
        /// How NULL values should be sorted if neither NULLS FIRST nor NULLS LAST are specified.
        /// </summary>
        public NullCollation DefaultNullCollation
        {
            get => GetEnum<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION);
            set => SetEnum<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION, value);
        }

        /// <summary>
        /// How many rows the Druid adapter should fetch at a time when executing "select" queries.
        /// </summary>
        public int DruidFetch
        {
            get => GetInteger(CalciteConnectionProperty.DRUID_FETCH);
            set => SetInteger(CalciteConnectionProperty.DRUID_FETCH, value);
        }

        /// <summary>
        /// URI of the model.
        /// </summary>
        public string Model
        {
            get => GetString(CalciteConnectionProperty.MODEL);
            set => SetString(CalciteConnectionProperty.MODEL, value);
        }

        /// <summary>
        /// Whether to treat empty strings as null for Druid Adapter.
        /// </summary>
        public bool NullEqualToEmpty
        {
            get => GetBoolean(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY);
            set => SetBoolean(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY, value);
        }

        /// <summary>
        /// Lexical policy.
        /// </summary>
        public Lex Lex
        {
            get => GetEnum<Lex>(CalciteConnectionProperty.LEX);
            set => SetEnum<Lex>(CalciteConnectionProperty.LEX, value);
        }

        /// <summary>
        /// Collection of built-in functions and operators.
        /// </summary>
        public string Fun
        {
            get => GetString(CalciteConnectionProperty.FUN);
            set => SetString(CalciteConnectionProperty.FUN, value);
        }

        /// <summary>
        /// How identifiers are quoted.
        /// </summary>
        public Quoting Quoting
        {
            get => GetEnum<Quoting>(CalciteConnectionProperty.QUOTING);
            set => SetEnum<Quoting>(CalciteConnectionProperty.QUOTING, value);
        }

        /// <summary>
        /// How identifiers are stored if they are quoted.
        /// </summary>
        public Casing? QuotedCasing
        {
            get => GetNullableEnum<Casing>(CalciteConnectionProperty.QUOTED_CASING);
            set => SetNullableEnum<Casing>(CalciteConnectionProperty.QUOTED_CASING, value);
        }

        /// <summary>
        /// How identifiers are stored if they are not quoted.
        /// </summary>
        public Casing? UnquotedCasing
        {
            get => GetNullableEnum<Casing>(CalciteConnectionProperty.UNQUOTED_CASING);
            set => SetNullableEnum<Casing>(CalciteConnectionProperty.UNQUOTED_CASING, value);
        }

        /// <summary>
        /// Whether identifiers are matched case-sensitively.
        /// </summary>
        public bool CaseSensitive
        {
            get => GetBoolean(CalciteConnectionProperty.CASE_SENSITIVE);
            set => SetBoolean(CalciteConnectionProperty.CASE_SENSITIVE, value);
        }

        /// <summary>
        /// Parser factory.
        /// </summary>
        public string ParserFactory
        {
            get => GetString(CalciteConnectionProperty.PARSER_FACTORY);
            set => SetString(CalciteConnectionProperty.PARSER_FACTORY, value);
        }

        /// <summary>
        /// MetaTableFactory plugin.
        /// </summary>
        public string MetaTableFactory
        {
            get => GetString(CalciteConnectionProperty.META_TABLE_FACTORY);
            set => SetString(CalciteConnectionProperty.META_TABLE_FACTORY, value);
        }

        /// <summary>
        /// MetaColumnFactory plugin.
        /// </summary>
        public string MetaColumnFactory
        {
            get => GetString(CalciteConnectionProperty.META_COLUMN_FACTORY);
            set => SetString(CalciteConnectionProperty.META_COLUMN_FACTORY, value);
        }

        /// <summary>
        /// Name of initial schema.
        /// </summary>
        public string Schema
        {
            get => GetString(CalciteConnectionProperty.SCHEMA);
            set => SetString(CalciteConnectionProperty.SCHEMA, value);
        }

        /// <summary>
        /// Gets the set of properties prefixed with 'schema.'.
        /// </summary>
        public CalciteConnectionPropertiesSchemaMap SchemaProperties => _schemaMap;

        /// <summary>
        /// Schema factory.
        /// </summary>
        public string SchemaFactory
        {
            get => GetString(CalciteConnectionProperty.SCHEMA_FACTORY);
            set => SetString(CalciteConnectionProperty.SCHEMA_FACTORY, value);
        }

        /// <summary>
        /// Schema type.
        /// </summary>
        public string SchemaType
        {
            get => GetString(CalciteConnectionProperty.SCHEMA_TYPE);
            set => SetString(CalciteConnectionProperty.SCHEMA_TYPE, value);
        }

        /// <summary>
        /// Specifies whether Spark should be used as the engine for processing that cannot be pushed to the source system.
        /// </summary>
        public bool Spark
        {
            get => GetBoolean(CalciteConnectionProperty.SPARK);
            set => SetBoolean(CalciteConnectionProperty.SPARK, value);
        }

        /// <summary>
        /// Returns the time zone from the connect string, for example 'gmt-3'.
        /// </summary>
        public string TimeZone
        {
            get => GetString(CalciteConnectionProperty.TIME_ZONE);
            set => SetString(CalciteConnectionProperty.TIME_ZONE, value);
        }

        /// <summary>
        /// Returns the locale from the connect string.
        /// </summary>
        public string Locale
        {
            get => GetString(CalciteConnectionProperty.LOCALE);
            set => SetString(CalciteConnectionProperty.LOCALE, value);
        }

        /// <summary>
        /// If the planner should try de-correlating as much as it is possible.
        /// </summary>
        public bool ForceDecorrelate
        {
            get => GetBoolean(CalciteConnectionProperty.FORCE_DECORRELATE);
            set => SetBoolean(CalciteConnectionProperty.FORCE_DECORRELATE, value);
        }

        /// <summary>
        /// Type system.
        /// </summary>
        public string TypeSystem
        {
            get => GetString(CalciteConnectionProperty.TYPE_SYSTEM);
            set => SetString(CalciteConnectionProperty.TYPE_SYSTEM, value);
        }

        /// <summary>
        /// SQL conformance level.
        /// </summary>
        public SqlConformanceEnum Conformance
        {
            get => GetEnum<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE);
            set => SetEnum<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE, value);
        }

        /// <summary>
        /// Whether to make implicit type coercion when type mismatch for validation, default true.
        /// </summary>
        public bool TypeCoercion
        {
            get => GetBoolean(CalciteConnectionProperty.TYPE_COERCION);
            set => SetBoolean(CalciteConnectionProperty.TYPE_COERCION, value);
        }

        /// <summary>
        /// Whether to make create implicit functions if functions do not exist in the operator table, default false.
        /// </summary>
        public bool LenientOperatorLookup
        {
            get => GetBoolean(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP);
            set => SetBoolean(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, value);
        }

        /// <summary>
        /// Whether to enable top-down optimization in Volcano planner.
        /// </summary>
        public bool TopdownOpt
        {
            get => GetBoolean(CalciteConnectionProperty.TOPDOWN_OPT);
            set => SetBoolean(CalciteConnectionProperty.TOPDOWN_OPT, value);
        }

    }

}
