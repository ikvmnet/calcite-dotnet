using System;
using System.Data;

using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// A mapping of a caller's own, registered on a connection and reached through the ADO.NET surface.
    /// </summary>
    /// <remarks>
    /// The registry has its own tests over the resolution rules; these are the ones that say the surface is
    /// actually wired to it. Both directions have to be here, because a mapping that names a type without
    /// carrying its conversions is the failure the whole arrangement exists to prevent, and it is only
    /// visible end to end.
    /// </remarks>
    public class CalciteTypeMappingTests
    {

        /// <summary>
        /// Reads and writes a VARCHAR as a <see cref="Uri"/>, which nothing built in does.
        /// </summary>
        sealed class UriResolver : IClrTypeResolver
        {

            readonly ClrTypeMappingCollection _mappings = new();

            public UriResolver()
            {
                _mappings.Add(typeof(Uri), SqlTypeName.VARCHAR, v => ((Uri)v).ToString(), v => new Uri((string)v));
            }

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context) => _mappings.GetMapping(clrType, relType, context);

        }

        static CalciteConnection Open(bool mapped)
        {
            var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            if (mapped)
                c.TypeMapper.Prepend(new UriResolver());

            c.Open();
            return c;
        }

        [Fact]
        public void A_registered_mapping_should_decide_what_a_column_reads_back_as()
        {
            using var c = Open(mapped: true);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST('https://calcite.apache.org/' AS VARCHAR(32)))";

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(typeof(Uri), r.GetFieldType(0));
            Assert.Equal(new Uri("https://calcite.apache.org/"), r.GetValue(0));
        }

        [Fact]
        public void A_registered_mapping_should_be_reachable_by_name()
        {
            using var c = Open(mapped: true);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST('https://calcite.apache.org/' AS VARCHAR(32)))";

            using var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(new Uri("https://calcite.apache.org/"), r.GetFieldValue<Uri>(0));
        }

        [Fact]
        public void A_registered_mapping_should_carry_a_parameter_in()
        {
            // no DbType, so the value's own CLR type is what selects the mapping. The cast is Calcite's
            // requirement rather than the mapping's: a bare VALUES (?) has no type to infer
            using var c = Open(mapped: true);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS VARCHAR(32)))";
            var p = cmd.CreateParameter();
            p.Value = new Uri("https://calcite.apache.org/");
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(new Uri("https://calcite.apache.org/"), r.GetValue(0));
        }

        [Fact]
        public void An_unregistered_connection_should_be_unaffected()
        {
            // the chain is per connection, so one caller's mapping is not every caller's
            using var c = Open(mapped: false);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST('https://calcite.apache.org/' AS VARCHAR(32)))";

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(typeof(string), r.GetFieldType(0));
            Assert.Equal("https://calcite.apache.org/", r.GetValue(0));
        }

        [Fact]
        public void A_registered_mapping_should_leave_alone_what_it_does_not_claim()
        {
            using var c = Open(mapped: true);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(42 AS INTEGER))";

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(typeof(int), r.GetFieldType(0));
            Assert.Equal(42, r.GetValue(0));
        }

        [Fact]
        public void The_mapping_should_be_the_one_the_reader_and_the_parameter_binder_share()
        {
            // a stated DbType names the Calcite type instead, and the value still crosses through the
            // caller's mapping. A value written and then read back is the same value, which is the whole
            // point of the two tables having become one
            using var c = Open(mapped: true);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(? AS VARCHAR(32)))";
            var p = cmd.CreateParameter();
            p.DbType = DbType.String;
            p.Value = new Uri("https://calcite.apache.org/");
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(new Uri("https://calcite.apache.org/"), r.GetValue(0));
        }

        // ------------------------------------------------------------------------------------
        // The type the plan reads a placeholder as is the validator's, not the caller's.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void A_DbType_naming_another_representation_should_not_reach_the_plan()
        {
            // Calcite infers TIMESTAMP for the placeholder, whose representation is a count of milliseconds
            // in a Long. DbType.Date names DATE, a count of days in an Integer. Binding the caller's name
            // rather than the validator's handed the plan the wrong class and threw partway through the
            // scan -- an InvalidCastException from inside a generated lambda, several frames from here.
            using var c = Open(mapped: false);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (? > TIMESTAMP '2020-01-01 00:00:00')";
            var p = cmd.CreateParameter();
            p.DbType = DbType.Date;
            p.Value = new DateTime(2024, 1, 15, 0, 0, 0, DateTimeKind.Utc);
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(true, r.GetValue(0));
        }

        [Fact]
        public void A_narrower_DbType_than_the_validator_inferred_should_be_carried_across()
        {
            using var c = Open(mapped: false);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(9000000000 AS BIGINT) + ?)";
            var p = cmd.CreateParameter();
            p.DbType = DbType.Int32;
            p.Value = 1;
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(9000000001L, r.GetValue(0));
        }

        [Fact]
        public void A_value_narrower_than_the_validator_inferred_should_be_carried_across()
        {
            // nothing stated, so the value's CLR type is the caller's side of the lookup and the inferred
            // BIGINT is Calcite's; the mapping converts rather than casting
            using var c = Open(mapped: false);
            using var cmd = c.CreateCommand();
            cmd.CommandText = "VALUES (CAST(9000000000 AS BIGINT) + ?)";
            var p = cmd.CreateParameter();
            p.Value = 1;
            cmd.Parameters.Add(p);

            using var r = cmd.ExecuteReader();
            Assert.True(r.Read());
            Assert.Equal(9000000001L, r.GetValue(0));
        }

    }

}
