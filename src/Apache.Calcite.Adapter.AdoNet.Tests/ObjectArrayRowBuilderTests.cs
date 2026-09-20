using System;
using System.Collections.Generic;
using System.Data.Common;

using FluentAssertions;

using Microsoft.Data.Sqlite;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Covers the row a scan hands to Calcite.
    /// </summary>
    /// <remarks>
    /// Calcite's enumerable convention reads a row as an <see cref="object"/> array with one element per
    /// field, in row type order, holding values in Calcite's own representation. This is what builds one.
    /// </remarks>
    public class ObjectArrayRowBuilderTests : IDisposable
    {

        SqliteConnection _connection = null!;
        readonly List<DbCommand> _commands = [];
        JavaTypeFactoryImpl _types = null!;

        /// <summary>
        /// Initializes a new instance.
        /// </summary>
        public ObjectArrayRowBuilderTests()
        {
            _types = new JavaTypeFactoryImpl();
            _connection = new SqliteConnection("Data Source=:memory:");
            _connection.Open();
        }

        /// <inheritdoc />
        public void Dispose()
        {
            foreach (var command in _commands)
                command.Dispose();

            _commands.Clear();
            _connection?.Dispose();
        }

        /// <summary>
        /// Returns a reader positioned on the first row of the given query.
        /// </summary>
        /// <param name="sql"></param>
        /// <returns></returns>
        DbDataReader Query(string sql)
        {
            var command = _connection.CreateCommand();
            command.CommandText = sql;
            _commands.Add(command);

            var reader = command.ExecuteReader();
            Assert.True(reader.Read(), "expected one row");
            return reader;
        }

        /// <summary>
        /// Returns the field list of a row type built from the given columns.
        /// </summary>
        /// <param name="columns"></param>
        /// <returns></returns>
        java.util.List Fields(params (string Name, SqlTypeName Type)[] columns)
        {
            var builder = _types.builder();
            foreach (var (name, type) in columns)
                builder.add(name, _types.createSqlType(type));

            return builder.build().getFieldList();
        }

        [Fact]
        public void ARowIsAnObjectArrayInFieldOrder()
        {
            using var reader = Query("SELECT 1 AS A, 'two' AS B, 3.5 AS C");
            var builder = new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.INTEGER), ("B", SqlTypeName.VARCHAR), ("C", SqlTypeName.DOUBLE)));

            var row = (object?[])builder.apply();

            Assert.Equal(3, row.Length);
            Assert.Equal(1, ((java.lang.Integer)row[0]!).intValue());
            Assert.Equal("two", row[1]);
            Assert.Equal(3.5d, ((java.lang.Double)row[2]!).doubleValue());
        }

        [Fact]
        public void ANullColumnIsANullElement()
        {
            using var reader = Query("SELECT 1 AS A, NULL AS B");
            var builder = new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.INTEGER), ("B", SqlTypeName.VARCHAR)));

            var row = (object?[])builder.apply();

            Assert.Equal(1, ((java.lang.Integer)row[0]!).intValue());
            Assert.Null(row[1]);
        }

        /// <summary>
        /// The field list decides how a value is read, not the provider: a column the provider surfaces one
        /// way is brought to the type the plan was built against.
        /// </summary>
        [Fact]
        public void TheFieldTypeDecidesHowAValueIsRead()
        {
            using var reader = Query("SELECT 42 AS A");

            var asInteger = (object?[])new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.INTEGER))).apply();
            Assert.IsAssignableFrom<java.lang.Integer>(asInteger[0]);

            var asBigint = (object?[])new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.BIGINT))).apply();
            Assert.IsAssignableFrom<java.lang.Long>(asBigint[0]);
        }

        [Fact]
        public void AnEmptyFieldListYieldsAnEmptyRow()
        {
            using var reader = Query("SELECT 1");
            var row = (object?[])new Utils.ObjectArrayRowBuilder(reader, _types.builder().build().getFieldList()).apply();

            Assert.Empty(row);
        }

        [Fact]
        public void ARowIsBuiltFreshEachTime()
        {
            using var reader = Query("SELECT 1 AS A");
            var builder = new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.INTEGER)));

            var first = builder.apply();
            var second = builder.apply();

            second.Should().NotBeSameAs(first, "a reused array would alias every row in the sequence");
        }

        [Fact]
        public void TheReaderIsRequired()
        {
            Assert.Throws<ArgumentNullException>(
                () => new Utils.ObjectArrayRowBuilder(null!, Fields(("A", SqlTypeName.INTEGER))));
        }

        [Fact]
        public void TheFieldListIsRequired()
        {
            using var reader = Query("SELECT 1");
            Assert.Throws<ArgumentNullException>(() => new Utils.ObjectArrayRowBuilder(reader, null!));
        }

        /// <summary>
        /// The builder is a <c>Function0</c> because that is what Calcite's generated code calls; it has to
        /// remain reachable through that interface.
        /// </summary>
        [Fact]
        public void TheBuilderIsCallableAsAFunction0()
        {
            using var reader = Query("SELECT 1 AS A");
            org.apache.calcite.linq4j.function.Function0 function =
                new Utils.ObjectArrayRowBuilder(reader, Fields(("A", SqlTypeName.INTEGER)));

            Assert.Equal(1, ((java.lang.Integer)((object?[])function.apply())[0]!).intValue());
        }

    }

}
