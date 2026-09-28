using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;

using org.apache.calcite;
using org.apache.calcite.linq4j;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.sql.type;

using Apache.Calcite.Data.Common;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers reading and writing values whose Calcite type does not say what they are (<c>ANY</c> and
    /// <c>VARIANT</c>), and collection values, which Calcite's runtime holds as Java objects.
    /// </summary>
    /// <remarks>
    /// No value a reader returns is a Java object. Under <c>ANY</c> the value's runtime class decides the
    /// .NET type; under every other type the column's type decides, and typed accessors stay strict.
    /// </remarks>
    public class CalciteAnyValueTests
    {

        /// <summary>
        /// Opens a connection with <see cref="AnyTable"/> registered as <c>ANYT</c> and
        /// <see cref="AnyClassFunction"/> as <c>ANYCLASS</c>.
        /// </summary>
        static CalciteConnection Open()
        {
            return new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString)
                .ConfigureRootSchema(root => root.add("ANYT", new AnyTable()))
                .ConfigureRootSchema(root => root.add("ANYCLASS", ScalarFunctionImpl.create((java.lang.Class)typeof(AnyClassFunction), "eval")))
                .Build()
                .OpenConnection();
        }

        /// <summary>
        /// Runs a statement and advances to its single row.
        /// </summary>
        static CalciteDataReader Row(CalciteConnection c, string sql)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            return r;
        }

        // ------------------------------------------------------------------------------------
        // ANY: the runtime class decides.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// An <c>ANY</c> column reports <see cref="object"/>, since its type is <c>java.lang.Object</c> and
        /// the field type is answered without reading a row.
        /// </summary>
        [Fact]
        public void Any_column_should_report_object_as_its_field_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT * FROM \"ANYT\"");

            for (var i = 0; i < r.FieldCount; i++)
                Assert.Equal(typeof(object), r.GetFieldType(i));
        }

        /// <summary>
        /// Whatever a table puts in an <c>ANY</c> column, the reader returns a .NET value.
        /// </summary>
        [Fact]
        public void Any_column_should_never_hand_out_a_java_object()
        {
            using var c = Open();
            using var r = Row(c, "SELECT * FROM \"ANYT\"");

            for (var i = 0; i < r.FieldCount; i++)
            {
                var name = r.GetValue(i).GetType().FullName!;
                Assert.False(name.StartsWith("java.", StringComparison.Ordinal), $"column {r.GetName(i)} gave {name}");
                Assert.False(name.StartsWith("org.apache.calcite.", StringComparison.Ordinal), $"column {r.GetName(i)} gave {name}");
            }
        }

        [Fact]
        public void Any_column_holding_a_string_should_read_as_a_string()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"S\" FROM \"ANYT\"");

            Assert.Equal("hello", r.GetValue(0));
            Assert.Equal("hello", r.GetString(0));
            Assert.Equal("hello", r.GetFieldValue<string>(0));
        }

        [Fact]
        public void Any_column_holding_an_integer_should_read_as_an_int()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"I\" FROM \"ANYT\"");

            Assert.Equal(7, r.GetValue(0));
            Assert.Equal(7, r.GetInt32(0));
        }

        /// <summary>
        /// The runtime class stands in for the declared type and is as strict: a <c>java.lang.Integer</c> in
        /// an <c>ANY</c> column is read as an <c>INTEGER</c>, so the accessors for other types refuse it.
        /// </summary>
        [Fact]
        public void Any_column_holding_an_integer_should_still_refuse_another_width()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"I\" FROM \"ANYT\"");

            Assert.Throws<InvalidCastException>(() => r.GetInt64(0));
            Assert.Throws<InvalidCastException>(() => r.GetInt16(0));
            Assert.Throws<InvalidCastException>(() => r.GetDecimal(0));
            Assert.Throws<InvalidCastException>(() => r.GetDouble(0));
        }

        [Fact]
        public void Any_column_holding_a_map_should_read_as_a_dictionary()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"M\" FROM \"ANYT\"");

            var value = r.GetValue(0);
            Assert.IsAssignableFrom<IDictionary>(value);

            var typed = Assert.IsType<Dictionary<string, int>>(value);
            Assert.Equal(1, typed["a"]);
            Assert.Equal(2, typed["b"]);
        }

        /// <summary>
        /// Naming the dictionary's element types builds it to those types instead of the ones taken from the
        /// values.
        /// </summary>
        [Fact]
        public void Any_column_holding_a_map_should_answer_the_element_types_a_caller_names()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"M\" FROM \"ANYT\"");

            var typed = r.GetFieldValue<IDictionary<string, object>>(0);
            Assert.Equal(1, typed["a"]);
        }

        [Fact]
        public void Any_column_holding_a_list_should_read_as_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"L\" FROM \"ANYT\"");

            Assert.Equal(new[] { 10, 20 }, Assert.IsType<int[]>(r.GetValue(0)));
            Assert.Equal(new List<int> { 10, 20 }, r.GetFieldValue<IList<int>>(0));
        }

        [Fact]
        public void Any_column_holding_a_uuid_should_read_as_a_guid()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"G\" FROM \"ANYT\"");

            Assert.Equal(AnyTable.Uuid, r.GetValue(0));
            Assert.Equal(AnyTable.Uuid, r.GetGuid(0));
        }

        /// <summary>
        /// A <c>java.sql.Timestamp</c> in an <c>ANY</c> column reads as a <see cref="DateTime"/>, through
        /// <see cref="CalciteDataReader.GetDateTime"/> as well as <see cref="CalciteDataReader.GetValue"/>.
        /// </summary>
        [Fact]
        public void Any_column_holding_a_timestamp_should_read_as_a_date_time()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"T\" FROM \"ANYT\"");

            Assert.Equal(AnyTable.Moment, r.GetValue(0));
            Assert.Equal(AnyTable.Moment, r.GetDateTime(0));
        }

        [Fact]
        public void Any_column_holding_a_local_date_should_read_as_a_date_only()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"D\" FROM \"ANYT\"");

            Assert.Equal(new DateOnly(2020, 1, 2), r.GetValue(0));
            Assert.Equal(new DateOnly(2020, 1, 2), r.GetDateOnly(0));

            // GetDateTime reads a moment, and a java.time.LocalDate is not one; adding a zero time would be
            // a conversion
            Assert.Throws<InvalidCastException>(() => r.GetDateTime(0));
        }

        /// <summary>
        /// <c>GetFieldValue&lt;object&gt;</c> returns what <see cref="CalciteDataReader.GetValue"/> returns,
        /// not the Java object behind it.
        /// </summary>
        [Fact]
        public void Any_column_read_as_object_should_answer_what_GetValue_answers()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"M\" FROM \"ANYT\"");

            Assert.IsType<Dictionary<string, int>>(r.GetFieldValue<object>(0));
        }

        /// <summary>
        /// A column with a declared type is read by that type, so a string is not read as a number, a
        /// <see cref="Guid"/> or a <see cref="DateTime"/>.
        /// </summary>
        [Fact]
        public void A_typed_column_should_still_refuse_a_value_that_is_not_what_was_asked_for()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST('x' AS VARCHAR)");

            Assert.Throws<InvalidCastException>(() => r.GetInt32(0));
            Assert.Throws<InvalidCastException>(() => r.GetGuid(0));
            Assert.Throws<InvalidCastException>(() => r.GetDateTime(0));
        }

        /// <summary>
        /// A <c>BIGINT</c> column reads through <see cref="CalciteDataReader.GetInt64"/> and not
        /// <see cref="CalciteDataReader.GetInt32"/>.
        /// </summary>
        [Fact]
        public void A_typed_integer_column_should_still_refuse_another_width()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(7 AS BIGINT)");

            Assert.Equal(7L, r.GetInt64(0));
            Assert.Throws<InvalidCastException>(() => r.GetInt32(0));
        }

        // ------------------------------------------------------------------------------------
        // The collection types, whose values Calcite's runtime holds as Java objects.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void A_map_column_should_read_as_a_dictionary()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MAP['a', 1, 'b', 2]");

            var typed = Assert.IsType<Dictionary<string, int>>(r.GetValue(0));
            Assert.Equal(1, typed["a"]);
            Assert.Equal(2, typed["b"]);
        }

        [Fact]
        public void An_array_column_should_read_as_an_array_of_its_component()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2, 3]");

            Assert.Equal(typeof(int[]), r.GetFieldType(0));
            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(r.GetValue(0)));
        }

        [Fact]
        public void A_multiset_column_should_read_as_an_array_of_its_component()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MULTISET[1, 2, 3]");

            Assert.Equal(new[] { 1, 2, 3 }, Assert.IsType<int[]>(r.GetValue(0)));
        }

        /// <summary>
        /// Each element is held as a count of days, and only the component type says it is a date.
        /// </summary>
        [Fact]
        public void An_array_of_dates_should_read_as_dates()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[DATE '2020-01-02', DATE '2020-01-03']");

            Assert.Equal(new[] { new DateTime(2020, 1, 2), new DateTime(2020, 1, 3) }, Assert.IsType<DateTime[]>(r.GetValue(0)));
        }

        /// <summary>
        /// A nullable component reads as an array of <see cref="Nullable{T}"/> rather than of
        /// <see cref="object"/>.
        /// </summary>
        [Fact]
        public void An_array_holding_a_null_should_read_as_an_array_of_the_nullable_component()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(r.GetValue(0)));
        }

        /// <summary>
        /// Calcite accepts a map literal with a null key, which a <see cref="Dictionary{TKey, TValue}"/>
        /// cannot hold, so a map whose declared key type is nullable reads as an array of pairs.
        /// </summary>
        /// <remarks>
        /// The pair types come from the column's declared key and value types, so every row of the result has
        /// the same shape whether or not its keys are null.
        /// </remarks>
        [Fact]
        public void A_map_holding_a_null_key_should_read_as_pairs()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MAP[CAST(NULL AS VARCHAR), 1]");

            var pairs = Assert.IsType<KeyValuePair<string, int>[]>(r.GetValue(0));
            Assert.Single(pairs);
            Assert.Null(pairs[0].Key);
            Assert.Equal(1, pairs[0].Value);
        }

        /// <summary>
        /// A row's fields may differ in type, so a row reads as an array of <see cref="object"/>.
        /// </summary>
        [Fact]
        public void A_row_column_should_read_as_an_array_of_object()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ROW(1, 'x')");

            Assert.Equal(typeof(object[]), r.GetFieldType(0));
            Assert.Equal(new object[] { 1, "x" }, Assert.IsType<object[]>(r.GetValue(0)));
        }

        // ------------------------------------------------------------------------------------
        // VARIANT: the type is carried with each value rather than by the column.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A variant's payload type is not known until a row is read, so the column reports
        /// <see cref="object"/>.
        /// </summary>
        [Fact]
        public void Variant_column_should_report_object_as_its_field_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS VARIANT)");

            Assert.Equal(typeof(object), r.GetFieldType(0));
            Assert.Equal("VARIANT", r.GetDataTypeName(0));
        }

        [Fact]
        public void Variant_should_never_hand_out_a_java_object()
        {
            using var c = Open();

            foreach (var expression in new[] { "CAST(1 AS VARIANT)", "CAST('x' AS VARIANT)", "CAST(ARRAY[1,2] AS VARIANT)", "CAST(MAP['a',1] AS VARIANT)", "CAST(DATE '2020-01-02' AS VARIANT)" })
            {
                using var r = Row(c, "SELECT " + expression);

                var name = r.GetValue(0).GetType().FullName!;
                Assert.False(name.StartsWith("java.", StringComparison.Ordinal), $"{expression} gave {name}");
                Assert.False(name.StartsWith("org.apache.calcite.", StringComparison.Ordinal), $"{expression} gave {name}");
            }
        }

        [Fact]
        public void Variant_holding_an_integer_should_read_as_an_int()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(7 AS VARIANT)");

            Assert.Equal(7, r.GetValue(0));
            Assert.Equal(7, r.GetInt32(0));
        }

        /// <summary>
        /// The payload's type stands in for the column's and is as strict: an <c>INTEGER</c> payload is
        /// refused by the accessors for other types.
        /// </summary>
        [Fact]
        public void Variant_holding_an_integer_should_still_refuse_another_width()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(7 AS VARIANT)");

            Assert.Throws<InvalidCastException>(() => r.GetInt64(0));
            Assert.Throws<InvalidCastException>(() => r.GetDecimal(0));
        }

        [Fact]
        public void Variant_holding_a_string_should_read_as_a_string()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST('hello' AS VARIANT)");

            Assert.Equal("hello", r.GetValue(0));
            Assert.Equal("hello", r.GetString(0));
        }

        [Fact]
        public void Variant_holding_a_boolean_should_read_as_a_bool()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(TRUE AS VARIANT)");

            Assert.Equal(true, r.GetValue(0));
            Assert.True(r.GetBoolean(0));
        }

        /// <summary>
        /// A variant keeps Calcite's storage form, so a <c>DATE</c> payload is a count of days and only the
        /// payload's type says it is a date.
        /// </summary>
        [Fact]
        public void Variant_holding_a_date_should_read_as_a_date_time()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(DATE '2020-01-02' AS VARIANT)");

            Assert.Equal(new DateTime(2020, 1, 2), r.GetValue(0));
            Assert.Equal(new DateTime(2020, 1, 2), r.GetDateTime(0));
        }

        [Fact]
        public void Variant_holding_a_timestamp_should_read_as_a_date_time()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(TIMESTAMP '2020-01-02 03:04:05' AS VARIANT)");

            Assert.Equal(new DateTime(2020, 1, 2, 3, 4, 5), r.GetValue(0));
        }

        [Fact]
        public void Variant_holding_a_decimal_should_read_as_a_decimal()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(CAST(1.25 AS DECIMAL(10, 2)) AS VARIANT)");

            Assert.Equal(1.25m, r.GetValue(0));
            Assert.Equal(1.25m, r.GetDecimal(0));
        }

        /// <summary>
        /// The elements are read one at a time with <c>item</c>, each by its own type, and the array's
        /// element type is the type they convert to.
        /// </summary>
        [Fact]
        public void Variant_holding_an_array_should_read_as_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(ARRAY[10, 20] AS VARIANT)");

            Assert.Equal(new[] { 10, 20 }, Assert.IsType<int[]>(r.GetValue(0)));
        }

        [Fact]
        public void Variant_holding_a_nested_array_should_read_as_a_nested_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(ARRAY[ARRAY[1, 2]] AS VARIANT)");

            var outer = Assert.IsType<int[][]>(r.GetValue(0));
            Assert.Equal(new[] { 1, 2 }, outer[0]);
        }

        [Fact]
        public void Variant_holding_an_array_with_a_null_should_read_as_an_array_of_the_nullable_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(ARRAY[CAST(NULL AS INTEGER), 5] AS VARIANT)");

            Assert.Equal(new int?[] { null, 5 }, Assert.IsType<int?[]>(r.GetValue(0)));
        }

        [Fact]
        public void Variant_holding_a_map_should_read_as_a_dictionary()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(MAP['a', 1, 'b', 2] AS VARIANT)");

            var typed = Assert.IsType<Dictionary<string, int>>(r.GetValue(0));
            Assert.Equal(1, typed["a"]);
            Assert.Equal(2, typed["b"]);
        }

        [Fact]
        public void Variant_holding_a_null_should_read_as_db_null()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(NULL AS VARIANT)");

            Assert.True(r.IsDBNull(0));
            Assert.Equal(DBNull.Value, r.GetValue(0));
        }

        /// <summary>
        /// A <c>ROW</c> payload answers <c>item</c> only for its field names, which the variant does not
        /// expose, and a <c>MULTISET</c> payload answers <c>item</c> with null for every index. Neither has a
        /// route to its contents, so both throw <see cref="ClrTypeMappingException"/> rather than returning
        /// the <c>VariantValue</c>.
        /// </summary>
        [Fact]
        public void Variant_holding_a_row_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(ROW(1, 'x') AS VARIANT)");

            Assert.Throws<ClrTypeMappingException>(() => r.GetValue(0));
        }

        [Fact]
        public void Variant_holding_a_multiset_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(MULTISET[1, 2] AS VARIANT)");

            Assert.Throws<ClrTypeMappingException>(() => r.GetValue(0));
        }

        /// <summary>
        /// <see cref="ClrTypeMappingException"/> derives from <see cref="InvalidCastException"/>, so a caller
        /// catching the latter catches the refusal.
        /// </summary>
        [Fact]
        public void A_refused_variant_should_still_be_an_invalid_cast()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(MULTISET[1, 2] AS VARIANT)");

            Assert.Throws<ClrTypeMappingException>(() => r.GetValue(0));
            Assert.IsAssignableFrom<InvalidCastException>(Record.Exception(() => r.GetValue(0)));
        }

        // ------------------------------------------------------------------------------------
        // Input: the same conversion the other way round.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// <see cref="DbType"/> has no member for a dictionary, so the parameter is
        /// <see cref="DbType.Object"/> and the value's own type decides how it is written. The function
        /// returns the class Calcite's runtime received.
        /// </summary>
        [Fact]
        public void A_dictionary_parameter_should_arrive_as_a_java_map()
        {
            using var c = Open();

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ANYCLASS(?)";
            cmd.Parameters.Add(new CalciteParameter("p", new Dictionary<string, int> { ["a"] = 1 }));

            Assert.Equal("java.util.LinkedHashMap", cmd.ExecuteScalar());
        }

        [Fact]
        public void A_sequence_parameter_should_arrive_as_a_java_list()
        {
            using var c = Open();

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ANYCLASS(?)";
            cmd.Parameters.Add(new CalciteParameter("p", new List<int> { 1, 2, 3 }));

            Assert.Equal("java.util.ArrayList", cmd.ExecuteScalar());
        }

        /// <summary>
        /// A one-character value infers <see cref="DbType.StringFixedLength"/>, and Calcite's runtime
        /// holds the character types as strings, so a <see cref="char"/> is written as a string of one
        /// character.
        /// </summary>
        [Fact]
        public void A_char_parameter_should_bind_as_a_string()
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT CAST(? AS VARCHAR)";
            cmd.Parameters.Add(new CalciteParameter("p", 'x'));

            Assert.Equal("x", cmd.ExecuteScalar());
        }

    }

    /// <summary>
    /// A one-row table whose columns are all <c>ANY</c>, holding a string, an integer, a map, a list, a
    /// UUID, a timestamp and a date as Java objects.
    /// </summary>
    sealed class AnyTable : AbstractTable, ScannableTable
    {

        /// <summary>
        /// The value the <c>G</c> column holds.
        /// </summary>
        public static readonly Guid Uuid = new("01234567-89ab-cdef-0123-456789abcdef");

        /// <summary>
        /// The moment the <c>T</c> column holds: 2020-01-02T03:04:05Z.
        /// </summary>
        public static readonly DateTime Moment = new(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);

        /// <inheritdoc />
        public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
            new RelDataTypeFactory.Builder(typeFactory)
                .add("S", SqlTypeName.ANY)
                .add("I", SqlTypeName.ANY)
                .add("M", SqlTypeName.ANY)
                .add("L", SqlTypeName.ANY)
                .add("G", SqlTypeName.ANY)
                .add("T", SqlTypeName.ANY)
                .add("D", SqlTypeName.ANY)
                .build();

        /// <inheritdoc />
        public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
        {
            var map = new java.util.LinkedHashMap();
            map.put("a", java.lang.Integer.valueOf(1));
            map.put("b", java.lang.Integer.valueOf(2));

            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(10));
            list.add(java.lang.Integer.valueOf(20));

            return Linq4j.singletonEnumerable(new object[]
            {
                "hello",
                java.lang.Integer.valueOf(7),
                map,
                list,
                java.util.UUID.fromString(Uuid.ToString()),
                new java.sql.Timestamp(1577934245000L),
                java.time.LocalDate.of(2020, 1, 2),
            });
        }

    }

    /// <summary>
    /// A scalar function that returns the class name of its argument, so a test can see what Calcite's
    /// runtime received for a parameter.
    /// </summary>
    public class AnyClassFunction
    {

        /// <summary>
        /// Returns the class name of <paramref name="value"/>.
        /// </summary>
        /// <param name="value">The value to name the class of.</param>
        /// <returns>The full name of the value's class, or <c>"null"</c> where it is null.</returns>
        public static string eval(object value)
        {
            return value is null ? "null" : value.GetType().FullName!;
        }

    }

}
