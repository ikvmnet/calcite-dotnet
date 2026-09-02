using System;
using System.Collections;
using System.Collections.Generic;

using Xunit;

namespace Apache.Calcite.Data.Internal.Tests
{

    /// <summary>
    /// Covers the conversion in both directions with no type to go on, which is the <c>ANY</c> case: the
    /// value's own runtime class is the whole of the information available, and a Java object is never an
    /// answer.
    /// </summary>
    public class CalciteValuesTests
    {

        static object? ToClr(object? value) => CalciteValues.ToClr(value, null);

        [Fact]
        public void ToClr_should_pass_null_through()
        {
            Assert.Null(ToClr(null));
        }

        [Fact]
        public void ToClr_should_read_the_boxed_primitives()
        {
            Assert.Equal(true, ToClr(java.lang.Boolean.valueOf(true)));
            Assert.Equal((sbyte)-1, ToClr(java.lang.Byte.valueOf(unchecked((byte)-1))));
            Assert.Equal((short)2, ToClr(java.lang.Short.valueOf(2)));
            Assert.Equal(3, ToClr(java.lang.Integer.valueOf(3)));
            Assert.Equal(4L, ToClr(java.lang.Long.valueOf(4)));
            Assert.Equal(5f, ToClr(java.lang.Float.valueOf(5)));
            Assert.Equal(6d, ToClr(java.lang.Double.valueOf(6)));
            Assert.Equal('c', ToClr(java.lang.Character.valueOf('c')));
        }

        [Fact]
        public void ToClr_should_read_a_string_as_itself()
        {
            Assert.Equal("hello", ToClr("hello"));
        }

        [Fact]
        public void ToClr_should_read_a_big_decimal_as_decimal()
        {
            Assert.Equal(1.25m, ToClr(new java.math.BigDecimal("1.25")));
        }

        [Fact]
        public void ToClr_should_read_a_uuid_as_guid()
        {
            var g = Guid.NewGuid();
            Assert.Equal(g, ToClr(java.util.UUID.fromString(g.ToString())));
        }

        [Fact]
        public void ToClr_should_read_a_byte_string_as_bytes()
        {
            Assert.Equal(new byte[] { 1, 2, 3 }, ToClr(new org.apache.calcite.avatica.util.ByteString(new byte[] { 1, 2, 3 })));
        }

        [Fact]
        public void ToClr_should_read_a_local_date_as_date_only()
        {
            Assert.Equal(new DateOnly(2020, 1, 2), ToClr(java.time.LocalDate.of(2020, 1, 2)));
        }

        [Fact]
        public void ToClr_should_read_a_local_time_as_time_only()
        {
            Assert.Equal(new TimeOnly(1, 2, 3), ToClr(java.time.LocalTime.of(1, 2, 3)));
        }

        [Fact]
        public void ToClr_should_read_a_sql_timestamp_as_date_time()
        {
            Assert.Equal(new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc), ToClr(new java.sql.Timestamp(1577934245000L)));
        }

        /// <summary>
        /// The measured element type: every element of the list is a <c>java.lang.Integer</c>, so the
        /// array that carries them is of <c>int</c> rather than of <see cref="object"/>.
        /// </summary>
        [Fact]
        public void ToClr_should_read_a_list_as_an_array_of_the_shared_type()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));
            list.add(java.lang.Integer.valueOf(2));

            var value = ToClr(list);

            Assert.Equal(new[] { 1, 2 }, Assert.IsType<int[]>(value));
        }

        [Fact]
        public void ToClr_should_read_a_list_holding_a_null_as_an_array_of_the_nullable_type()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));
            list.add(null);

            var value = ToClr(list);

            Assert.Equal(new int?[] { 1, null }, Assert.IsType<int?[]>(value));
        }

        [Fact]
        public void ToClr_should_read_a_list_of_unlike_elements_as_an_array_of_object()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));
            list.add("two");

            var value = ToClr(list);

            Assert.Equal(new object[] { 1, "two" }, Assert.IsType<object[]>(value));
        }

        [Fact]
        public void ToClr_should_read_a_map_as_a_dictionary_of_the_shared_types()
        {
            var map = new java.util.LinkedHashMap();
            map.put("a", java.lang.Integer.valueOf(1));
            map.put("b", java.lang.Integer.valueOf(2));

            var value = ToClr(map);

            var typed = Assert.IsType<Dictionary<string, int>>(value);
            Assert.Equal(2, typed.Count);
            Assert.Equal(1, typed["a"]);
            Assert.Equal(2, typed["b"]);
        }

        [Fact]
        public void ToClr_should_read_a_nested_map_as_a_nested_dictionary()
        {
            var inner = new java.util.LinkedHashMap();
            inner.put("x", java.lang.Integer.valueOf(9));

            var map = new java.util.LinkedHashMap();
            map.put("a", inner);

            var typed = Assert.IsType<Dictionary<string, Dictionary<string, int>>>(ToClr(map));
            Assert.Equal(9, typed["a"]["x"]);
        }

        /// <summary>
        /// No dictionary the framework ships takes a null key, and Calcite reaches one, so the entries
        /// come out as pairs rather than being lost.
        /// </summary>
        [Fact]
        public void ToClr_should_read_a_map_holding_a_null_key_as_pairs()
        {
            var map = new java.util.LinkedHashMap();
            map.put(null, java.lang.Integer.valueOf(1));

            var pairs = Assert.IsType<KeyValuePair<object, object>[]>(ToClr(map));
            Assert.Single(pairs);
            Assert.Null(pairs[0].Key);
            Assert.Equal(1, pairs[0].Value);
        }

        /// <summary>
        /// A row is its fields, so it keeps its shape however alike they are.
        /// </summary>
        [Fact]
        public void ToClr_should_read_an_object_array_as_an_object_array()
        {
            var value = ToClr(new object[] { java.lang.Integer.valueOf(1), java.lang.Integer.valueOf(2) });

            Assert.Equal(new object[] { 1, 2 }, Assert.IsType<object[]>(value));
        }

        /// <summary>
        /// A class the framework has no counterpart for is the one thing that comes back as it went in;
        /// everything Calcite's own runtime produces is named.
        /// </summary>
        [Fact]
        public void ToClr_should_hand_back_an_unmapped_class_untouched()
        {
            var value = new java.lang.StringBuilder("x");

            Assert.Same(value, ToClr(value));
        }

        [Fact]
        public void ToJava_should_pass_null_and_db_null_through()
        {
            Assert.Null(CalciteValues.ToJava(null));
            Assert.Null(CalciteValues.ToJava(DBNull.Value));
        }

        [Fact]
        public void ToJava_should_box_the_primitives_the_way_java_does()
        {
            Assert.IsType<java.lang.Boolean>(CalciteValues.ToJava(true));
            Assert.IsType<java.lang.Byte>(CalciteValues.ToJava((sbyte)1));
            Assert.IsType<java.lang.Short>(CalciteValues.ToJava((short)1));
            Assert.IsType<java.lang.Integer>(CalciteValues.ToJava(1));
            Assert.IsType<java.lang.Long>(CalciteValues.ToJava(1L));
            Assert.IsType<java.lang.Float>(CalciteValues.ToJava(1f));
            Assert.IsType<java.lang.Double>(CalciteValues.ToJava(1d));
            Assert.IsType<java.math.BigDecimal>(CalciteValues.ToJava(1m));
        }

        /// <summary>
        /// A <c>CHAR</c> is a string in Calcite's runtime, and one character is a string of one.
        /// </summary>
        [Fact]
        public void ToJava_should_write_a_char_as_a_string()
        {
            Assert.Equal("c", CalciteValues.ToJava('c'));
        }

        [Fact]
        public void ToJava_should_write_a_dictionary_as_a_java_map()
        {
            var map = Assert.IsAssignableFrom<java.util.Map>(CalciteValues.ToJava(new Dictionary<string, int> { ["a"] = 1 }));

            Assert.Equal(1, map.size());
            Assert.IsType<java.lang.Integer>(map.get("a"));
        }

        /// <summary>
        /// A <c>LinkedHashMap</c>, because that is what Calcite's own <c>SqlFunctions.map</c> builds and
        /// the order a map's entries come out in is the order they went in.
        /// </summary>
        [Fact]
        public void ToJava_should_keep_a_dictionarys_order()
        {
            var source = new Dictionary<string, int> { ["b"] = 2, ["a"] = 1 };
            var map = (java.util.Map)CalciteValues.ToJava(source)!;

            var keys = new List<object?>();
            for (var i = map.keySet().iterator(); i.hasNext();)
                keys.Add(i.next());

            Assert.Equal(new object?[] { "b", "a" }, keys);
        }

        [Fact]
        public void ToJava_should_write_a_sequence_as_a_java_list()
        {
            var list = Assert.IsAssignableFrom<java.util.List>(CalciteValues.ToJava(new[] { 1, 2, 3 }));

            Assert.Equal(3, list.size());
            Assert.IsType<java.lang.Integer>(list.get(0));
        }

        /// <summary>
        /// A string enumerates its characters and binary is a sequence of bytes; neither is a collection
        /// as far as Calcite's runtime is concerned.
        /// </summary>
        [Fact]
        public void ToJava_should_not_read_a_string_or_binary_as_a_sequence()
        {
            Assert.Equal("abc", CalciteValues.ToJava("abc"));
            Assert.IsType<org.apache.calcite.avatica.util.ByteString>(CalciteValues.ToJava(new byte[] { 1, 2 }));
        }

        [Fact]
        public void ToJava_should_convert_the_elements_of_a_nested_collection()
        {
            var list = (java.util.List)CalciteValues.ToJava(new List<Dictionary<string, int>> { new() { ["a"] = 1 } })!;

            var map = Assert.IsAssignableFrom<java.util.Map>(list.get(0));
            Assert.IsType<java.lang.Integer>(map.get("a"));
        }

        /// <summary>
        /// Every value that goes out and comes back is the same value, which is the whole point of having
        /// the two directions in one place.
        /// </summary>
        [Fact]
        public void ToJava_and_ToClr_should_round_trip_a_dictionary()
        {
            var source = new Dictionary<string, int> { ["a"] = 1, ["b"] = 2 };
            var back = Assert.IsType<Dictionary<string, int>>(ToClr(CalciteValues.ToJava(source)));

            Assert.Equal(source, back);
        }

        [Fact]
        public void ToJava_and_ToClr_should_round_trip_a_sequence()
        {
            var back = Assert.IsType<int[]>(ToClr(CalciteValues.ToJava(new[] { 1, 2, 3 })));

            Assert.Equal(new[] { 1, 2, 3 }, back);
        }

        [Fact]
        public void TryConvertTo_should_build_the_element_types_the_caller_named()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));
            list.add(java.lang.Integer.valueOf(2));

            Assert.True(CalciteValues.TryConvertTo(list, null, typeof(IList<object>), out var value));
            Assert.Equal(new List<object> { 1, 2 }, Assert.IsType<List<object>>(value));
        }

        /// <summary>
        /// Naming an element type says which of the types the values already have is wanted, not that
        /// they should be converted into it: an <c>IList&lt;long&gt;</c> over a list of
        /// <c>java.lang.Integer</c> is the same refusal <c>GetInt64</c> makes over an <c>INTEGER</c>.
        /// </summary>
        [Fact]
        public void TryConvertTo_should_refuse_an_element_type_the_values_do_not_have()
        {
            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));

            Assert.Throws<InvalidCastException>(() => CalciteValues.TryConvertTo(list, null, typeof(IList<long>), out _));
        }

        [Fact]
        public void TryConvertTo_should_build_a_dictionary_of_the_named_types()
        {
            var map = new java.util.LinkedHashMap();
            map.put("a", java.lang.Integer.valueOf(1));

            Assert.True(CalciteValues.TryConvertTo(map, null, typeof(IDictionary<string, object>), out var value));
            var typed = Assert.IsType<Dictionary<string, object>>(value);
            Assert.Equal(1, typed["a"]);
        }

        [Fact]
        public void TryConvertTo_should_refuse_a_target_that_is_not_a_collection()
        {
            Assert.False(CalciteValues.TryConvertTo(java.lang.Integer.valueOf(1), null, typeof(int), out _));
        }

        /// <summary>
        /// The non-generic shapes an ADO.NET consumer reaches for come for free from the measured types,
        /// and this holds that they do.
        /// </summary>
        [Fact]
        public void ToClr_should_answer_the_non_generic_collection_interfaces()
        {
            var map = new java.util.LinkedHashMap();
            map.put("a", java.lang.Integer.valueOf(1));

            var list = new java.util.ArrayList();
            list.add(java.lang.Integer.valueOf(1));

            Assert.IsAssignableFrom<IDictionary>(ToClr(map));
            Assert.IsAssignableFrom<IList>(ToClr(list));
        }

    }

}
