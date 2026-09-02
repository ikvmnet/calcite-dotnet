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

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers what an ADO.NET caller reads and writes where Calcite's type does not say what the value
    /// is — <c>ANY</c>, whose runtime representation is <c>java.lang.Object</c> — and where it says the
    /// value is a collection, which Calcite's runtime holds as Java objects either way.
    /// </summary>
    /// <remarks>
    /// The rule the whole suite holds: no value a reader hands out is a Java object. Under <c>ANY</c> the
    /// runtime class decides which .NET type it becomes, because there is nothing else to decide it;
    /// under every other type the column decides, and an accessor over a column whose type says what it
    /// holds stays as strict as it was.
    /// </remarks>
    public class CalciteAnyValueTests
    {

        /// <summary>
        /// Opens a connection with <see cref="AnyTable"/> registered as <c>ANYT</c>.
        /// </summary>
        static CalciteConnection Open()
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            c.RootSchema.add("ANYT", new AnyTable());
            return c;
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
        /// The claim the column makes about itself. <c>ANY</c> is <see cref="object"/> and can be nothing
        /// else: the type is <c>java.lang.Object</c> and no row has been read when it is asked.
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
        /// The whole of the contract in one assertion: whatever a table put in an <c>ANY</c> column, what
        /// comes out is a type of this runtime.
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
        /// The runtime class stands in for the declared type; it does not remove it. A
        /// <c>java.lang.Integer</c> in an <c>ANY</c> column is an <c>INTEGER</c>, and reading an
        /// <c>INTEGER</c> through <see cref="CalciteDataReader.GetInt64"/> is the refusal ADO.NET makes
        /// whatever column it came out of.
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
        /// The measured element types are not the only ones a caller may want, so naming them builds the
        /// dictionary to them instead.
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
        /// A <c>java.sql.Timestamp</c> is a moment whatever column it came out of, and under <c>ANY</c>
        /// there is no column type to say otherwise. This is the case the strict accessors could not
        /// reach before, because they read the SQL type and <c>ANY</c> is not <c>TIMESTAMP</c>.
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

            // a date is a date: GetDateTime is the accessor for a moment, and a java.time.LocalDate is
            // not one however easily a zero time could be bolted on
            Assert.Throws<InvalidCastException>(() => r.GetDateTime(0));
        }

        /// <summary>
        /// <c>GetFieldValue&lt;object&gt;</c> is <see cref="CalciteDataReader.GetValue"/> by another name
        /// and answers the same thing, rather than the Java object behind it.
        /// </summary>
        [Fact]
        public void Any_column_read_as_object_should_answer_what_GetValue_answers()
        {
            using var c = Open();
            using var r = Row(c, "SELECT \"M\" FROM \"ANYT\"");

            Assert.IsType<Dictionary<string, int>>(r.GetFieldValue<object>(0));
        }

        /// <summary>
        /// <c>ANY</c> loosens the accessors because it has no source type to be strict about. A column
        /// that does have one is strict, and reading a string as a number is the refusal ADO.NET makes.
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
        /// A <c>BIGINT</c> is a <c>BIGINT</c>: the widening an <c>ANY</c> gets is not on offer where the
        /// column names its type.
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
        // The collection types, whose values are Java objects however well the column describes them.
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
        /// The component's type is what says a count of days is a date; the value carries nothing that
        /// would.
        /// </summary>
        [Fact]
        public void An_array_of_dates_should_read_as_dates()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[DATE '2020-01-02', DATE '2020-01-03']");

            Assert.Equal(new[] { new DateTime(2020, 1, 2), new DateTime(2020, 1, 3) }, Assert.IsType<DateTime[]>(r.GetValue(0)));
        }

        /// <summary>
        /// An array holding a null cannot be an array of a value type, and <c>Nullable{T}</c> still names
        /// what it holds where <see cref="object"/> would not.
        /// </summary>
        [Fact]
        public void An_array_holding_a_null_should_read_as_an_array_of_the_nullable_component()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(r.GetValue(0)));
        }

        /// <summary>
        /// Calcite validates and runs a map literal with a null key, and no dictionary the framework
        /// ships accepts one, so the entries come out as pairs rather than being dropped.
        /// </summary>
        [Fact]
        public void A_map_holding_a_null_key_should_read_as_pairs()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MAP[CAST(NULL AS VARCHAR), 1]");

            var pairs = Assert.IsType<KeyValuePair<object, object>[]>(r.GetValue(0));
            Assert.Single(pairs);
            Assert.Null(pairs[0].Key);
            Assert.Equal(1, pairs[0].Value);
        }

        /// <summary>
        /// A row's fields are heterogeneous, so it stays an array of <see cref="object"/> rather than
        /// being unified the way a collection's elements are.
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
        // VARIANT: the type is carried with the value rather than by the column, which is the same
        // problem as ANY written the other way round, and reads the same way.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A variant's payload type is not known until a row is read, so the column can claim no more
        /// than <see cref="object"/>.
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
        /// The payload's type stands in for the one the column does not declare, and does nothing more
        /// than stand in for it: an <c>INTEGER</c> in a variant is an <c>INTEGER</c>.
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
        /// A variant keeps Calcite's storage form, so a <c>DATE</c> inside one is a count of days and
        /// only the payload's type says so. Reading it as a number would be the same defect the ANY and
        /// collection paths exist to avoid.
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
        /// Walked with <c>item</c> rather than cast, so each element carries its own type and the array
        /// is measured from what they convert to, exactly as a real <c>ARRAY</c> column is.
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
        /// A <c>ROW</c> payload answers <c>item</c> only for its field names, and a variant does not
        /// carry them; a <c>MULTISET</c> payload answers <c>item</c> with null for every index. Neither
        /// has a public route to its contents in Calcite 1.42, so both are refused rather than guessed
        /// at — handing back the <c>VariantValue</c> would put a Java object in a caller's hands.
        /// </summary>
        [Fact]
        public void Variant_holding_a_row_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(ROW(1, 'x') AS VARIANT)");

            Assert.Throws<InvalidCastException>(() => r.GetValue(0));
        }

        [Fact]
        public void Variant_holding_a_multiset_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(MULTISET[1, 2] AS VARIANT)");

            Assert.Throws<InvalidCastException>(() => r.GetValue(0));
        }

        // ------------------------------------------------------------------------------------
        // Input: the same conversion the other way round.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A dictionary is not a type <see cref="DbType"/> names, so the parameter is
        /// <see cref="DbType.Object"/> and the value's own type is what says how to write it. The
        /// function reads back the class Calcite's runtime received.
        /// </summary>
        [Fact]
        public void A_dictionary_parameter_should_arrive_as_a_java_map()
        {
            using var c = Open();
            c.RootSchema.add("ANYCLASS", ScalarFunctionImpl.create((java.lang.Class)typeof(AnyClassFunction), "eval"));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ANYCLASS(?)";
            cmd.Parameters.Add(new CalciteParameter("p", new Dictionary<string, int> { ["a"] = 1 }));

            Assert.Equal("java.util.LinkedHashMap", cmd.ExecuteScalar());
        }

        [Fact]
        public void A_sequence_parameter_should_arrive_as_a_java_list()
        {
            using var c = Open();
            c.RootSchema.add("ANYCLASS", ScalarFunctionImpl.create((java.lang.Class)typeof(AnyClassFunction), "eval"));

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT ANYCLASS(?)";
            cmd.Parameters.Add(new CalciteParameter("p", new List<int> { 1, 2, 3 }));

            Assert.Equal("java.util.ArrayList", cmd.ExecuteScalar());
        }

        /// <summary>
        /// A one-character value infers <see cref="DbType.StringFixedLength"/>, and Calcite's runtime
        /// holds the character family as a string, so a <see cref="char"/> is a string of one rather than
        /// a cast that fails.
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
    /// A table whose columns are all <c>ANY</c>, holding one of each thing Calcite's runtime and a
    /// schema of its own can put there.
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
    /// Names the class of whatever it is handed, which is how a test reads what Calcite's runtime
    /// actually received for a parameter.
    /// </summary>
    public class AnyClassFunction
    {

        /// <summary>
        /// Returns the class name of <paramref name="value"/>.
        /// </summary>
        /// <param name="value"></param>
        /// <returns></returns>
        public static string eval(object value)
        {
            return value is null ? "null" : value.GetType().FullName!;
        }

    }

}
