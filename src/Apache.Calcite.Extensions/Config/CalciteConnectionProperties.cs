using Apache.Calcite.Extensions;

using java.util;

using org.apache.calcite.avatica.util;
using org.apache.calcite.config;
using org.apache.calcite.sql.validate;

namespace Apache.Calcite.Extensions.Config
{

    /// <summary>
    /// Typed access to the Apache Calcite connection properties held in a Java <see cref="Properties"/> map.
    /// </summary>
    /// <remarks>
    /// Each property reads and writes the map entry named by the corresponding
    /// <c>CalciteConnectionProperty</c>, converting enums, booleans and integers to and from their string
    /// form. A property with no entry reads as Calcite's declared default for it. Where Calcite declares no
    /// default and derives the value from <see cref="Lex"/> instead (<see cref="Quoting"/>,
    /// <see cref="QuotedCasing"/>, <see cref="UnquotedCasing"/>, <see cref="CaseSensitive"/>), the property
    /// reads as <see langword="null"/>, or <see langword="false"/> for <see cref="CaseSensitive"/>; Calcite
    /// still applies the <see cref="Lex"/> value when the connection is made.
    /// </remarks>
    public class CalciteConnectionProperties
    {

        readonly Properties _properties;
        readonly CalciteConnectionPropertiesSchemaMap _schemaMap;

        /// <summary>
        /// Initializes a new instance over an existing <see cref="Properties"/> map.
        /// </summary>
        /// <param name="properties">The map to read from and write to. Changes through this instance are
        /// visible in it.</param>
        public CalciteConnectionProperties(Properties properties)
        {
            _properties = properties;
            _schemaMap = new CalciteConnectionPropertiesSchemaMap(_properties);
        }

        /// <summary>
        /// Initializes a new instance over a new, empty <see cref="Properties"/> map.
        /// </summary>
        public CalciteConnectionProperties()
        {
            _properties = new Properties();
            _schemaMap = new CalciteConnectionPropertiesSchemaMap(_properties);
        }

        /// <summary>
        /// Gets a Calcite connection property that is a Java enum.
        /// </summary>
        /// <typeparam name="T">The Java enum type of the property.</typeparam>
        /// <param name="property">The property to read.</param>
        /// <returns>The constant named by the stored value, or the property's default if it is not set.</returns>
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
        /// <typeparam name="T">The Java enum type of the property.</typeparam>
        /// <param name="property">The property to set.</param>
        /// <param name="value">The constant to store by name; <see langword="null"/> removes the property.</param>
        void SetEnum<T>(CalciteConnectionProperty property, T value)
            where T : java.lang.Enum
        {
            if (value is null)
                _properties.remove(property.camelName());
            else
                _properties.setProperty(property.camelName(), value.name());
        }

        /// <summary>
        /// Gets a Calcite connection property that is a Java enum with no default.
        /// </summary>
        /// <typeparam name="T">The Java enum type of the property.</typeparam>
        /// <param name="property">The property to read.</param>
        /// <returns>The constant named by the stored value, or the property's default, which may be <see langword="null"/>, if it is not set.</returns>
        T? GetNullableEnum<T>(CalciteConnectionProperty property)
            where T : java.lang.Enum
        {
            if (_properties.containsKey(property.camelName()))
                return (T?)(T)java.lang.Enum.valueOf(typeof(T), _properties.getProperty(property.camelName()));
            else
                return (T?)property.defaultValue();
        }

        /// <summary>
        /// Sets a Calcite connection property that is a Java enum with no default, removing it for
        /// <see langword="null"/>.
        /// </summary>
        /// <typeparam name="T">The Java enum type of the property.</typeparam>
        /// <param name="property">The property to set.</param>
        /// <param name="value">The constant to store by name, or <see langword="null"/> to remove the property.</param>
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
        /// <param name="property">The property to read.</param>
        /// <returns>The stored value parsed as a boolean, or the property's default if it is not set.</returns>
        bool GetBoolean(CalciteConnectionProperty property)
        {
            return bool.Parse(_properties.getProperty(property.camelName(), ((java.lang.Boolean?)property.defaultValue())?.booleanValue() == true ? "true" : "false"));
        }

        /// <summary>
        /// Sets a Calcite connection property that is a boolean.
        /// </summary>
        /// <param name="property">The property to set.</param>
        /// <param name="value">The value, stored as <c>true</c> or <c>false</c>.</param>
        void SetBoolean(CalciteConnectionProperty property, bool value)
        {
            _properties.setProperty(property.camelName(), value ? "true" : "false");
        }

        /// <summary>
        /// Gets a Calcite connection property that is an integer.
        /// </summary>
        /// <param name="property">The property to read.</param>
        /// <returns>The stored value parsed as an integer, or the property's default if it is not set.</returns>
        int GetInteger(CalciteConnectionProperty property)
        {
            return int.Parse(_properties.getProperty(property.camelName(), ((java.lang.Integer)property.defaultValue()).toString()));
        }

        /// <summary>
        /// Sets a Calcite connection property that is an integer.
        /// </summary>
        /// <param name="property">The property to set.</param>
        /// <param name="value">The value, stored in its string form.</param>
        void SetInteger(CalciteConnectionProperty property, int value)
        {
            _properties.setProperty(property.camelName(), value.ToString());
        }

        /// <summary>
        /// Gets a Calcite connection property that is a string.
        /// </summary>
        /// <param name="property">The property to read.</param>
        /// <returns>The stored value, or the property's default if it is not set.</returns>
        string GetString(CalciteConnectionProperty property)
        {
            return _properties.getProperty(property.camelName(), (string)property.defaultValue());
        }

        /// <summary>
        /// Sets a Calcite connection property that is a string.
        /// </summary>
        /// <param name="property">The property to set.</param>
        /// <param name="value">The value to store.</param>
        void SetString(CalciteConnectionProperty property, string value)
        {
            _properties.setProperty(property.camelName(), value);
        }

        /// <summary>
        /// Gets or sets whether approximate results from aggregate functions on <c>DECIMAL</c> types are
        /// acceptable. Default <see langword="false"/>.
        /// </summary>
        public bool ApproximateDecimal
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_DECIMAL);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_DECIMAL, value);
        }

        /// <summary>
        /// Gets or sets whether approximate results from <c>COUNT(DISTINCT ...)</c> are acceptable. Default
        /// <see langword="false"/>.
        /// </summary>
        public bool ApproximateDistinctCount
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_DISTINCT_COUNT, value);
        }

        /// <summary>
        /// Gets or sets whether approximate results from Top-N queries (<c>ORDER BY aggFun DESC LIMIT n</c>)
        /// are acceptable. Default <see langword="false"/>.
        /// </summary>
        public bool ApproximateTopN
        {
            get => GetBoolean(CalciteConnectionProperty.APPROXIMATE_TOP_N);
            set => SetBoolean(CalciteConnectionProperty.APPROXIMATE_TOP_N, value);
        }

        /// <summary>
        /// Gets or sets whether query results are stored in temporary tables. Default <see langword="false"/>.
        /// </summary>
        public bool AutoTemp
        {
            get => GetBoolean(CalciteConnectionProperty.AUTO_TEMP);
            set => SetBoolean(CalciteConnectionProperty.AUTO_TEMP, value);
        }

        /// <summary>
        /// Gets or sets whether the planner uses materializations. Default <see langword="true"/>.
        /// </summary>
        public bool MaterializationsEnabled
        {
            get => GetBoolean(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED);
            set => SetBoolean(CalciteConnectionProperty.MATERIALIZATIONS_ENABLED, value);
        }

        /// <summary>
        /// Gets or sets how nulls sort when neither <c>NULLS FIRST</c> nor <c>NULLS LAST</c> is specified.
        /// Default <c>HIGH</c>.
        /// </summary>
        public NullCollation DefaultNullCollation
        {
            get => GetEnum<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION);
            set => SetEnum<NullCollation>(CalciteConnectionProperty.DEFAULT_NULL_COLLATION, value);
        }

        /// <summary>
        /// Gets or sets how many rows the Druid adapter fetches at a time for a select query. Default 16384.
        /// </summary>
        public int DruidFetch
        {
            get => GetInteger(CalciteConnectionProperty.DRUID_FETCH);
            set => SetInteger(CalciteConnectionProperty.DRUID_FETCH, value);
        }

        /// <summary>
        /// Gets or sets the model: a URI, or an inline JSON model prefixed with <c>inline:</c>.
        /// </summary>
        public string Model
        {
            get => GetString(CalciteConnectionProperty.MODEL);
            set => SetString(CalciteConnectionProperty.MODEL, value);
        }

        /// <summary>
        /// Gets or sets whether the Druid adapter treats empty strings as null. Default <see langword="true"/>.
        /// </summary>
        public bool NullEqualToEmpty
        {
            get => GetBoolean(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY);
            set => SetBoolean(CalciteConnectionProperty.NULL_EQUAL_TO_EMPTY, value);
        }

        /// <summary>
        /// Gets or sets the lexical policy, which supplies the defaults for quoting, casing and case
        /// sensitivity. Default <c>ORACLE</c>.
        /// </summary>
        public Lex Lex
        {
            get => GetEnum<Lex>(CalciteConnectionProperty.LEX);
            set => SetEnum<Lex>(CalciteConnectionProperty.LEX, value);
        }

        /// <summary>
        /// Gets or sets the libraries of built-in functions and operators, as a comma-separated list such as
        /// <c>standard,oracle,spatial</c>. Default <c>standard</c>.
        /// </summary>
        public string Fun
        {
            get => GetString(CalciteConnectionProperty.FUN);
            set => SetString(CalciteConnectionProperty.FUN, value);
        }

        /// <summary>
        /// Gets or sets how identifiers are quoted, or <see langword="null"/> to use <see cref="Lex"/>'s.
        /// </summary>
        public Quoting Quoting
        {
            get => GetEnum<Quoting>(CalciteConnectionProperty.QUOTING);
            set => SetEnum<Quoting>(CalciteConnectionProperty.QUOTING, value);
        }

        /// <summary>
        /// Gets or sets how quoted identifiers are stored, or <see langword="null"/> to use
        /// <see cref="Lex"/>'s.
        /// </summary>
        public Casing? QuotedCasing
        {
            get => GetNullableEnum<Casing>(CalciteConnectionProperty.QUOTED_CASING);
            set => SetNullableEnum<Casing>(CalciteConnectionProperty.QUOTED_CASING, value);
        }

        /// <summary>
        /// Gets or sets how unquoted identifiers are stored, or <see langword="null"/> to use
        /// <see cref="Lex"/>'s.
        /// </summary>
        public Casing? UnquotedCasing
        {
            get => GetNullableEnum<Casing>(CalciteConnectionProperty.UNQUOTED_CASING);
            set => SetNullableEnum<Casing>(CalciteConnectionProperty.UNQUOTED_CASING, value);
        }

        /// <summary>
        /// Gets or sets whether identifiers are matched case-sensitively.
        /// </summary>
        /// <remarks>
        /// When no value has been set this reads <see langword="false"/>, but Calcite uses
        /// <see cref="Lex"/>'s setting, which is <see langword="true"/> for <c>ORACLE</c>.
        /// </remarks>
        public bool CaseSensitive
        {
            get => GetBoolean(CalciteConnectionProperty.CASE_SENSITIVE);
            set => SetBoolean(CalciteConnectionProperty.CASE_SENSITIVE, value);
        }

        /// <summary>
        /// Gets or sets the parser factory, as the name of a static field or class that provides one.
        /// </summary>
        public string ParserFactory
        {
            get => GetString(CalciteConnectionProperty.PARSER_FACTORY);
            set => SetString(CalciteConnectionProperty.PARSER_FACTORY, value);
        }

        /// <summary>
        /// Gets or sets the <c>MetaTableFactory</c> plugin, as a class or static field name.
        /// </summary>
        public string MetaTableFactory
        {
            get => GetString(CalciteConnectionProperty.META_TABLE_FACTORY);
            set => SetString(CalciteConnectionProperty.META_TABLE_FACTORY, value);
        }

        /// <summary>
        /// Gets or sets the <c>MetaColumnFactory</c> plugin, as a class or static field name.
        /// </summary>
        public string MetaColumnFactory
        {
            get => GetString(CalciteConnectionProperty.META_COLUMN_FACTORY);
            set => SetString(CalciteConnectionProperty.META_COLUMN_FACTORY, value);
        }

        /// <summary>
        /// Gets or sets the name of the default schema.
        /// </summary>
        public string Schema
        {
            get => GetString(CalciteConnectionProperty.SCHEMA);
            set => SetString(CalciteConnectionProperty.SCHEMA, value);
        }

        /// <summary>
        /// Gets the <c>schema.*</c> entries of the map as a dictionary keyed without the prefix. Calcite
        /// passes them as operands to the schema factory.
        /// </summary>
        public CalciteConnectionPropertiesSchemaMap SchemaProperties => _schemaMap;

        /// <summary>
        /// Gets or sets the schema factory, as a class or static field name, used when there is no model.
        /// </summary>
        public string SchemaFactory
        {
            get => GetString(CalciteConnectionProperty.SCHEMA_FACTORY);
            set => SetString(CalciteConnectionProperty.SCHEMA_FACTORY, value);
        }

        /// <summary>
        /// Gets or sets the schema type: <c>MAP</c>, <c>JDBC</c> or <c>CUSTOM</c>.
        /// </summary>
        public string SchemaType
        {
            get => GetString(CalciteConnectionProperty.SCHEMA_TYPE);
            set => SetString(CalciteConnectionProperty.SCHEMA_TYPE, value);
        }

        /// <summary>
        /// Gets or sets whether Spark is used as the engine for processing that cannot be pushed to the source
        /// system. Default <see langword="false"/>.
        /// </summary>
        public bool Spark
        {
            get => GetBoolean(CalciteConnectionProperty.SPARK);
            set => SetBoolean(CalciteConnectionProperty.SPARK, value);
        }

        /// <summary>
        /// Gets or sets the session time zone, for example <c>gmt-3</c>. Default the JVM's default time zone.
        /// </summary>
        public string TimeZone
        {
            get => GetString(CalciteConnectionProperty.TIME_ZONE);
            set => SetString(CalciteConnectionProperty.TIME_ZONE, value);
        }

        /// <summary>
        /// Gets or sets the session locale. Default <c>Locale.ROOT</c>.
        /// </summary>
        public string Locale
        {
            get => GetString(CalciteConnectionProperty.LOCALE);
            set => SetString(CalciteConnectionProperty.LOCALE, value);
        }

        /// <summary>
        /// Gets or sets whether the planner decorrelates sub-queries as far as possible. Default
        /// <see langword="true"/>.
        /// </summary>
        public bool ForceDecorrelate
        {
            get => GetBoolean(CalciteConnectionProperty.FORCE_DECORRELATE);
            set => SetBoolean(CalciteConnectionProperty.FORCE_DECORRELATE, value);
        }

        /// <summary>
        /// Gets or sets whether decorrelation uses <c>TopDownGeneralDecorrelator</c> rather than
        /// <c>RelDecorrelator</c>. Default <see langword="false"/>.
        /// </summary>
        /// <remarks>
        /// Has an effect only when <see cref="ForceDecorrelate"/> is <see langword="true"/>.
        /// </remarks>
        public bool TopDownGeneralDecorrelationEnabled
        {
            get => GetBoolean(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED);
            set => SetBoolean(CalciteConnectionProperty.TOPDOWN_GENERAL_DECORRELATION_ENABLED, value);
        }

        /// <summary>
        /// Gets or sets the type system, as a class or static field name.
        /// </summary>
        public string TypeSystem
        {
            get => GetString(CalciteConnectionProperty.TYPE_SYSTEM);
            set => SetString(CalciteConnectionProperty.TYPE_SYSTEM, value);
        }

        /// <summary>
        /// Gets or sets the SQL conformance level. Default <c>DEFAULT</c>.
        /// </summary>
        public SqlConformanceEnum Conformance
        {
            get => GetEnum<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE);
            set => SetEnum<SqlConformanceEnum>(CalciteConnectionProperty.CONFORMANCE, value);
        }

        /// <summary>
        /// Gets or sets whether the validator applies implicit type coercion where types do not match.
        /// Default <see langword="true"/>.
        /// </summary>
        public bool TypeCoercion
        {
            get => GetBoolean(CalciteConnectionProperty.TYPE_COERCION);
            set => SetBoolean(CalciteConnectionProperty.TYPE_COERCION, value);
        }

        /// <summary>
        /// Gets or sets whether a call to a function that is not in the operator table is accepted rather than
        /// rejected. Default <see langword="false"/>.
        /// </summary>
        public bool LenientOperatorLookup
        {
            get => GetBoolean(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP);
            set => SetBoolean(CalciteConnectionProperty.LENIENT_OPERATOR_LOOKUP, value);
        }

        /// <summary>
        /// Gets or sets whether the Volcano planner uses top-down optimization. Default the value of the
        /// <c>calcite.planner.topdown.opt</c> system property.
        /// </summary>
        public bool TopdownOpt
        {
            get => GetBoolean(CalciteConnectionProperty.TOPDOWN_OPT);
            set => SetBoolean(CalciteConnectionProperty.TOPDOWN_OPT, value);
        }

    }

}
