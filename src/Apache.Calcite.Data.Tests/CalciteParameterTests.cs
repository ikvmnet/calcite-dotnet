using System;
using System.Data;


using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteParameterTests
    {

        [Fact]
        public void Default_should_have_input_direction_and_object_type()
        {
            var p = new CalciteParameter();
            Assert.Equal(ParameterDirection.Input, p.Direction);
            Assert.Equal(DbType.Object, p.DbType);
            Assert.Equal(string.Empty, p.ParameterName);
            Assert.Equal(string.Empty, p.SourceColumn);
        }

        [Fact]
        public void DbType_should_infer_from_value_when_not_set()
        {
            var p = new CalciteParameter("@p", 42);
            Assert.Equal(DbType.Int32, p.DbType);
        }

        [Fact]
        public void DbType_should_infer_string_from_value()
        {
            var p = new CalciteParameter("@p", "hello");
            Assert.Equal(DbType.String, p.DbType);
        }

        [Fact]
        public void DbType_should_be_explicit_once_set()
        {
            var p = new CalciteParameter("@p", 42) { DbType = DbType.Int64 };
            Assert.Equal(DbType.Int64, p.DbType);
        }

        [Fact]
        public void ResetDbType_should_revert_to_inferred_value()
        {
            var p = new CalciteParameter("@p", 42) { DbType = DbType.Int64 };
            p.ResetDbType();
            Assert.Equal(DbType.Int32, p.DbType);
        }

        [Fact]
        public void ParameterName_should_normalize_null_to_empty()
        {
            var p = new CalciteParameter { ParameterName = null! };
            Assert.Equal(string.Empty, p.ParameterName);
        }

        [Fact]
        public void Constructor_with_dbtype_should_set_explicit_dbtype()
        {
            var p = new CalciteParameter("@p", DbType.Decimal);
            Assert.Equal(DbType.Decimal, p.DbType);
            Assert.Null(p.Value);
        }

        [Fact]
        public void Properties_should_round_trip()
        {
            var p = new CalciteParameter
            {
                ParameterName = "@x",
                Value = "v",
                Size = 50,
                Precision = 10,
                Scale = 2,
                IsNullable = true,
                Direction = ParameterDirection.Input,
                SourceColumn = "col",
            };

            Assert.Equal("@x", p.ParameterName);
            Assert.Equal("v", p.Value);
            Assert.Equal(50, p.Size);
            Assert.Equal((byte)10, p.Precision);
            Assert.Equal((byte)2, p.Scale);
            Assert.True(p.IsNullable);
            Assert.Equal(ParameterDirection.Input, p.Direction);
            Assert.Equal("col", p.SourceColumn);
        }

    }

    public class CalciteParameterCollectionTests
    {

        [Fact]
        public void Add_should_increment_count()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@a", 1));
            c.Add(new CalciteParameter("@b", 2));
            Assert.Equal(2, c.Count);
        }

        [Fact]
        public void Add_object_should_reject_non_calcite_parameter()
        {
            var c = new CalciteParameterCollection();
            Assert.Throws<InvalidCastException>(() => c.Add("not a parameter"));
        }

        [Fact]
        public void Add_null_should_throw()
        {
            var c = new CalciteParameterCollection();
            Assert.Throws<ArgumentNullException>(() => c.Add((CalciteParameter)null!));
        }

        [Fact]
        public void IndexOf_should_match_case_insensitive_name()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@FOO", 1));
            Assert.Equal(0, c.IndexOf("@foo"));
        }

        [Fact]
        public void IndexOf_should_return_negative_one_for_unknown()
        {
            var c = new CalciteParameterCollection();
            Assert.Equal(-1, c.IndexOf("@missing"));
        }

        [Fact]
        public void Contains_by_name_should_be_case_insensitive()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@foo", 1));
            Assert.True(c.Contains("@FOO"));
            Assert.False(c.Contains("@bar"));
        }

        [Fact]
        public void RemoveAt_by_name_should_remove_parameter()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@a", 1));
            c.Add(new CalciteParameter("@b", 2));
            c.RemoveAt("@a");
            Assert.Equal(1, c.Count);
            Assert.Equal(0, c.IndexOf("@b"));
        }

        [Fact]
        public void Clear_should_remove_all()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@a", 1));
            c.Clear();
            Assert.Equal(0, c.Count);
        }

        [Fact]
        public void Insert_should_place_parameter_at_index()
        {
            var c = new CalciteParameterCollection();
            c.Add(new CalciteParameter("@a", 1));
            c.Insert(0, new CalciteParameter("@b", 2));
            Assert.Equal(0, c.IndexOf("@b"));
            Assert.Equal(1, c.IndexOf("@a"));
        }

        [Fact]
        public void Indexer_by_name_should_set_and_get()
        {
            var c = new CalciteParameterCollection();
            var p = new CalciteParameter("@x", 1);
            c.Add(p);
            Assert.Same(p, c["@x"]);
        }

        [Fact]
        public void GetParameter_with_unknown_name_should_throw()
        {
            var c = new CalciteParameterCollection();
            Assert.Throws<IndexOutOfRangeException>(() => c["@missing"]);
        }

    }

}
