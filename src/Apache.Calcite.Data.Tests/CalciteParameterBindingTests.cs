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
    /// The rule these hold is that the placeholder's type is the constraint and the caller's word is a
    /// preference. Calcite refuses a placeholder it cannot infer a type for, so by the time there is a plan
    /// there is a type, and the plan reads the value as that type whatever was said about it.
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
        /// named a <c>DATE</c>. Naming the date used to hand the plan a count of days where it read a count
        /// of milliseconds, and threw partway through the scan.
        /// </summary>
        [Fact]
        public void A_value_should_be_written_as_the_placeholder_s_type_and_not_the_named_one()
        {
            var moment = new DateTime(2020, 1, 2, 3, 4, 5, DateTimeKind.Utc);
            var p = new CalciteParameter("p", moment) { DbType = DbType.Date };

            Assert.Equal(moment, Scalar("SELECT CAST(? AS TIMESTAMP)", p));
        }

        /// <summary>
        /// A widening the table has no entry for is still bound, because there is nothing wrong with putting
        /// a <see cref="ushort"/> in an <c>INTEGER</c>. Reading one back as a <see cref="ushort"/> is the
        /// narrowing that stays refused.
        /// </summary>
        [Fact]
        public void A_named_type_the_placeholder_has_no_mapping_for_should_still_bind()
        {
            var p = new CalciteParameter("p", (ushort)7) { DbType = DbType.UInt16 };

            Assert.Equal(7, Scalar("SELECT CAST(? AS INTEGER)", p));
        }

        /// <summary>
        /// Naming nothing leaves the placeholder's own default, which is the ordinary case.
        /// </summary>
        [Fact]
        public void An_unnamed_value_should_be_written_by_the_placeholder_s_default()
        {
            Assert.Equal(5, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", 5)));
            Assert.Equal("x", Scalar("SELECT CAST(? AS VARCHAR)", new CalciteParameter("p", "x")));
        }

        /// <summary>
        /// Both spellings of a SQL null reach the plan as one, and neither reaches a mapping. What comes
        /// back is <see cref="DBNull"/>, which is ADO.NET's spelling of the same thing.
        /// </summary>
        [Fact]
        public void Either_spelling_of_null_should_bind_as_null()
        {
            Assert.Equal(DBNull.Value, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", null)));
            Assert.Equal(DBNull.Value, Scalar("SELECT CAST(? AS INTEGER)", new CalciteParameter("p", DBNull.Value)));
        }

        /// <summary>
        /// A <see cref="Guid"/> is a <c>UUID</c> now, so a placeholder cast to one carries it.
        /// </summary>
        [Fact]
        public void A_guid_should_bind_as_a_uuid()
        {
            var value = Guid.NewGuid();

            Assert.Equal(value, Scalar("SELECT CAST(? AS UUID)", new CalciteParameter("p", value)));
        }

        /// <summary>
        /// The mappings a caller registers reach parameters too, which is the half of the chain that only
        /// wiring the binder could show.
        /// </summary>
        /// <remarks>
        /// Registered before the connection opens, because the chain is read once and bound to the type
        /// factory the session creates. A resolver added afterwards is registered for the next session.
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
        /// And one registered after the connection opens does not, which is the same fact stated so that it
        /// cannot quietly stop being true.
        /// </summary>
        [Fact]
        public void A_caller_mapping_registered_after_opening_should_not_reach_a_parameter()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            c.TypeMapper.Prepend(new ShoutingResolver());

            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT CAST(? AS VARCHAR)";
            cmd.Parameters.Add(new CalciteParameter("p", "x"));

            Assert.Equal("x", cmd.ExecuteScalar());
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
