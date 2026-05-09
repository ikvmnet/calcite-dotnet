using System;
using System.Data;
using System.Reflection;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    public class CalciteTypeMapTests
    {

        // The mapping helper is internal; access it via reflection so the tests do not require InternalsVisibleTo.
        static readonly Type _mapType = typeof(Apache.Calcite.Data.CalciteConnection).Assembly
            .GetType("Apache.Calcite.Data.CalciteTypeMap", throwOnError: true)!;

        static DbType ToDbType(Type clrType)
        {
            var m = _mapType.GetMethod("ToDbType", BindingFlags.Public | BindingFlags.Static)!;
            return (DbType)m.Invoke(null, new object[] { clrType })!;
        }

        static Type ToClrType(DbType dbType)
        {
            var m = _mapType.GetMethod("ToClrType", BindingFlags.Public | BindingFlags.Static)!;
            return (Type)m.Invoke(null, new object[] { dbType })!;
        }

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
        [InlineData(typeof(byte[]), DbType.Binary)]
        public void ToDbType_should_map_known_types(Type clr, DbType expected)
        {
            Assert.Equal(expected, ToDbType(clr));
        }

        [Fact]
        public void ToDbType_should_resolve_nullable_underlying()
        {
            Assert.Equal(DbType.Int32, ToDbType(typeof(int?)));
        }

        [Fact]
        public void ToDbType_unknown_should_default_to_object()
        {
            Assert.Equal(DbType.Object, ToDbType(typeof(System.Text.StringBuilder)));
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
            Assert.Equal(expected, ToClrType(db));
        }

        [Fact]
        public void ToDbType_null_should_throw()
        {
            Assert.Throws<TargetInvocationException>(() => ToDbType(null!));
        }

    }

}
