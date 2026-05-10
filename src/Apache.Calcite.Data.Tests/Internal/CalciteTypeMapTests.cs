using System;
using System.Data;

using Xunit;

namespace Apache.Calcite.Data.Internal.Tests
{

    public class CalciteTypeMapTests
    {

        [Theory]
        [InlineData(typeof(bool), DbType.Boolean)]
        [InlineData(typeof(int), DbType.Int32)]
        [InlineData(typeof(long), DbType.Int64)]
        [InlineData(typeof(short), DbType.Int16)]
        [InlineData(typeof(byte), DbType.Byte)]
        [InlineData(typeof(float), DbType.Single)]
        [InlineData(typeof(double), DbType.Double)]
        [InlineData(typeof(decimal), DbType.Decimal)]
        [InlineData(typeof(string), DbType.String)]
        [InlineData(typeof(Guid), DbType.Guid)]
        [InlineData(typeof(DateTime), DbType.DateTime)]
        [InlineData(typeof(DateTimeOffset), DbType.DateTimeOffset)]
        [InlineData(typeof(TimeSpan), DbType.Time)]
        [InlineData(typeof(DateOnly), DbType.Date)]
        [InlineData(typeof(TimeOnly), DbType.Time)]
        [InlineData(typeof(byte[]), DbType.Binary)]
        public void ToDbType_should_map_known_types(Type clr, DbType expected)
        {
            Assert.Equal(expected, CalciteTypeMap.ToDbType(clr));
        }

        [Fact]
        public void ToDbType_should_resolve_nullable_underlying()
        {
            Assert.Equal(DbType.Int32, CalciteTypeMap.ToDbType(typeof(int?)));
        }

        [Fact]
        public void ToDbType_unknown_should_default_to_object()
        {
            Assert.Equal(DbType.Object, CalciteTypeMap.ToDbType(typeof(System.Text.StringBuilder)));
        }

        [Theory]
        [InlineData(DbType.Boolean, typeof(bool))]
        [InlineData(DbType.Int32, typeof(int))]
        [InlineData(DbType.String, typeof(string))]
        [InlineData(DbType.Decimal, typeof(decimal))]
        [InlineData(DbType.DateTime, typeof(DateTime))]
        [InlineData(DbType.Binary, typeof(byte[]))]
        [InlineData(DbType.Guid, typeof(Guid))]
        public void ToClrType_should_map_known_dbtypes(DbType db, Type expected)
        {
            Assert.Equal(expected, CalciteTypeMap.ToClrType(db));
        }

        [Fact]
        public void ToDbType_null_should_throw()
        {
            Assert.Throws<ArgumentNullException>(() => CalciteTypeMap.ToDbType(null!));
        }

    }

}
