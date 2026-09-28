using System;
using System.Data;

using Apache.Calcite.Data.Common;

using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers what a bound value becomes on its way into a plan.
    /// </summary>
    /// <remarks>
    /// The placeholder's type decides how a value is written, and the <see cref="System.Data.DbType"/> a
    /// caller names is only a preference. Calcite refuses a placeholder it cannot infer a type for, so every
    /// placeholder in a plan has a type, and the plan reads the value as that type.
    /// </remarks>
    public class CalciteParameterBindingTests
    {

        static CalciteConnection Open()
        {
            return new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString).Build().OpenConnection();
        }

        static object? Scalar(string sql, params CalciteParameter[] parameters)
        {
            using var c = Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            foreach (var p in parameters)
                cmd.Parameters.Add(p);

            return cmd.ExecuteScalar();
        }

        /// <summary>
        /// The placeholder is a <c>TIMESTAMP</c>, so the value is written as one even though the caller
        /// named a <c>DATE</c>; the plan reads a <c>TIMESTAMP</c> as milliseconds, not the days a
        /// <c>DATE</c> is written as.
        /// </summary>
        [Fact]
        public void A_value_should_be_written_as_the_placeholder_s_type_and_not_the_named_one()
        {
            var moment = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            var p = new CalciteParameter("p", moment) { DbType = DbType.Date };

            Assert.Equal(moment, Scalar("SELECT CAST(? AS TIMESTAMP)", p));
        }

        /// <summary>
        /// A <see cref="ushort"/> is bound to an <c>INTEGER</c> placeholder although no mapping names that
        /// pair, because the conversion widens.
        /// </summary>
        [Fact]
        public void A_named_type_the_placeholder_has_no_mapping_for_should_still_bind()
        {
            var p = new CalciteParameter("p", (ushort)7) { DbType = DbType.UInt16 };

            Assert.Equal(7, Scalar("SELECT CAST(? AS INTEGER)", p));
        }

        /// <summary>
        /// A parameter with no <c>DbType</c> set is converted by the mapping for the placeholder's own type.
        /// </summary>
        [Fact]
        public void An_unnamed_value_should_be_written_by_the_placeholder_s_default()
        {
            Assert.Equal(5, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", 5)));
            Assert.Equal("x", Scalar("SELECT CAST(? AS VARCHAR)", new CalciteParameter("p", "x")));
        }

        /// <summary>
        /// Both <see langword="null"/> and <see cref="DBNull"/> bind as SQL null without reaching a
        /// mapping, and the result reads back as <see cref="DBNull"/>.
        /// </summary>
        [Fact]
        public void Either_spelling_of_null_should_bind_as_null()
        {
            Assert.Equal(DBNull.Value, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", null)));
            Assert.Equal(DBNull.Value, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", DBNull.Value)));
        }

        /// <summary>
        /// A <see cref="Guid"/> binds to a placeholder cast to <c>UUID</c> and reads back as the same value.
        /// </summary>
        [Fact]
        public void A_guid_should_bind_as_a_uuid()
        {
            var value = Guid.NewGuid();

            Assert.Equal(value, Scalar("SELECT CAST(? AS UUID)", new CalciteParameter("p", value)));
        }

        /// <summary>
        /// A resolver the caller registers applies to parameters as well as results.
        /// </summary>
        /// <remarks>
        /// Registered before the connection opens, because the session reads the chain once, at open.
        /// </remarks>
        [Fact]
        public void A_caller_mapping_should_reach_a_parameter()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.TypeMapper.Prepend(new ShoutingResolver());
            c.Open();

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT CAST(? AS VARCHAR)";
            cmd.Parameters.Add(new CalciteParameter("p", "x"));

            Assert.Equal("X", cmd.ExecuteScalar());
        }

        /// <summary>
        /// The session reads the chain once, at open, so a resolver added afterwards would never run;
        /// <see cref="CalciteConnection.TypeMapper"/> throws rather than hand out a chain nothing reads.
        /// </summary>
        [Fact]
        public void A_caller_mapping_registered_after_opening_should_be_refused()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();

            Assert.Throws<InvalidOperationException>(() => c.TypeMapper);
        }

        /// <summary>
        /// Writes a string in upper case, so that a value having crossed the chain is visible.
        /// </summary>
        sealed class ShoutingResolver : IClrTypeResolver
        {

            /// <inheritdoc />
            public ClrTypeMapping? GetMapping(Type? clrType, org.apache.calcite.rel.type.RelDataType? relType, ClrTypeContext context)
            {
                if (relType is not null && relType.getSqlTypeName() == SqlTypeName.VARCHAR && (clrType is null || clrType == typeof(string)))
                    return new DelegateClrTypeMapping(context, relType, typeof(string), v => ((string)v).ToUpperInvariant(), v => (string)v);

                return null;
            }

        }

    }

}
