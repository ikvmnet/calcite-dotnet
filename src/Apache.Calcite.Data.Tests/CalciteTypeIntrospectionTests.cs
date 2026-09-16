using System;
using System.Linq;

using Apache.Calcite.Data.Common;

using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Covers the three ways a caller can ask what a column's type is, and the one way it can change the
    /// answer.
    /// </summary>
    /// <remarks>
    /// The three widen: <c>GetFieldType</c> is one .NET type, <c>GetCalciteDbType</c> is Calcite's own fixed
    /// list, and <c>GetRelDataType</c> is the type itself. Only the last describes a type that nests, which
    /// is why all three exist rather than the narrowest one.
    /// </remarks>
    public class CalciteTypeIntrospectionTests
    {

        static CalciteConnection Open()
        {
            return new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString).Build().OpenConnection();
        }

        static CalciteDataReader Row(CalciteConnection c, string sql)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            return r;
        }

        [Fact]
        public void A_scalar_column_should_name_itself_in_all_three_ways()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS INTEGER)");

            Assert.Equal(typeof(int), r.GetFieldType(0));
            Assert.Equal(CalciteDbType.Integer, r.GetCalciteDbType(0));
            Assert.Equal(SqlTypeName.INTEGER, r.GetRelDataType(0).getSqlTypeName());
        }

        /// <summary>
        /// The unsigned integers are the plainest case of what ADO.NET's list cannot say and Calcite's can.
        /// </summary>
        [Fact]
        public void An_unsigned_column_should_be_named_by_the_calcite_list()
        {
            using var c = Open();
            using var r = Row(c, "SELECT CAST(1 AS INTEGER UNSIGNED)");

            Assert.Equal(CalciteDbType.UInteger, r.GetCalciteDbType(0));
            Assert.Equal(typeof(uint), r.GetFieldType(0));
        }

        /// <summary>
        /// One level of nesting is what the flag is for, and is where it stops.
        /// </summary>
        [Fact]
        public void An_array_column_should_carry_its_element_in_the_flag()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2, 3]");

            Assert.Equal(CalciteDbType.Array | CalciteDbType.Integer, r.GetCalciteDbType(0));
        }

        [Fact]
        public void A_multiset_column_should_be_told_apart_from_an_array()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MULTISET[1, 2, 3]");

            Assert.Equal(CalciteDbType.Multiset | CalciteDbType.Integer, r.GetCalciteDbType(0));
        }

        /// <summary>
        /// A second level has nowhere to go in one flag, so the base is <see cref="CalciteDbType.Unknown"/>
        /// and the type itself is the only complete answer. This is the whole reason
        /// <c>GetRelDataType</c> exists.
        /// </summary>
        [Fact]
        public void A_nested_array_column_should_be_unknown_in_the_flag_and_exact_in_the_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[ARRAY[1, 2]]");

            Assert.Equal(CalciteDbType.Array, r.GetCalciteDbType(0));
            Assert.Equal(CalciteDbType.Unknown, CalciteDbTypes.BaseType(r.GetCalciteDbType(0)));

            var type = r.GetRelDataType(0);
            Assert.Equal(SqlTypeName.ARRAY, type.getSqlTypeName());
            Assert.Equal(SqlTypeName.ARRAY, type.getComponentType().getSqlTypeName());
            Assert.Equal(SqlTypeName.INTEGER, type.getComponentType().getComponentType().getSqlTypeName());
        }

        /// <summary>
        /// Three levels, to show the type keeps describing where the flag stopped at one.
        /// </summary>
        [Fact]
        public void A_thrice_nested_array_column_should_still_be_described_by_the_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[ARRAY[ARRAY[1, 2]]]");

            var type = r.GetRelDataType(0);
            var depth = 0;
            for (var t = type; t.getComponentType() is RelDataType component; t = component)
                depth++;

            Assert.Equal(3, depth);
            Assert.Equal("INTEGER ARRAY ARRAY ARRAY", r.GetDataTypeName(0));
        }

        /// <summary>
        /// A map's key type is a fact Calcite carries and neither fixed list has room for.
        /// </summary>
        [Fact]
        public void A_map_column_should_carry_its_key_and_value_types()
        {
            using var c = Open();
            using var r = Row(c, "SELECT MAP['a', 1]");

            Assert.Equal(CalciteDbType.Map, r.GetCalciteDbType(0));

            var type = r.GetRelDataType(0);
            Assert.Equal(SqlTypeName.CHAR, type.getKeyType().getSqlTypeName());
            Assert.Equal(SqlTypeName.INTEGER, type.getValueType().getSqlTypeName());
        }

        /// <summary>
        /// Nullability is on the type, which is what a collection's element needs and a column's
        /// <c>GetFieldType</c> deliberately does not report.
        /// </summary>
        [Fact]
        public void A_nullable_element_should_say_so_on_the_type()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, CAST(NULL AS INTEGER)]");

            Assert.True(r.GetRelDataType(0).getComponentType().isNullable());
        }

        // ------------------------------------------------------------------------------------
        // There is one way to reach the Java object, and it is named.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// <c>GetFieldValue</c> answers the column's reading, a reading the chain carries under another
        /// name, or a shape of one. Naming the class Calcite holds the value in is none of those, and used
        /// to be answered by a last arm that handed the Java object over — a second way out of the rule that
        /// no Java object reaches a caller, and one a caller could take without meaning to.
        /// </summary>
        [Theory]
        [InlineData("SELECT 1")]
        [InlineData("SELECT ARRAY[1, 2]")]
        [InlineData("SELECT CAST('x' AS VARCHAR)")]
        public void Naming_the_java_class_should_be_refused(string sql)
        {
            using var c = Open();
            using var r = Row(c, sql);

            Assert.Throws<InvalidCastException>(() => r.GetFieldValue<java.lang.Object>(0));
        }

        [Fact]
        public void Naming_the_java_class_of_a_collection_should_be_refused()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2]");

            Assert.Throws<InvalidCastException>(() => r.GetFieldValue<java.util.List>(0));
        }

        /// <summary>
        /// And the one way that is left says what it is in its name.
        /// </summary>
        [Fact]
        public void The_calcite_value_should_be_the_only_way_to_the_java_object()
        {
            using var c = Open();
            using var r = Row(c, "SELECT 1");

            Assert.Equal(java.lang.Integer.valueOf(1), r.GetCalciteValue(0));
            Assert.Equal(1, r.GetFieldValue<int>(0));
        }

        /// <summary>
        /// A collection likewise: the list is reachable, and only by asking for it.
        /// </summary>
        [Fact]
        public void The_calcite_value_of_a_collection_should_be_the_list()
        {
            using var c = Open();
            using var r = Row(c, "SELECT ARRAY[1, 2]");

            Assert.IsAssignableFrom<java.util.List>(r.GetCalciteValue(0));
            Assert.Equal(new[] { 1, 2 }, r.GetFieldValue<int[]>(0));
        }

        // ------------------------------------------------------------------------------------
        // The registry, used the way something outside this assembly would use it.
        // ------------------------------------------------------------------------------------

        /// <summary>
        /// A resolver of a caller's own, which is the whole extension point.
        /// </summary>
        sealed class MoneyResolver : IClrTypeResolver
        {

            public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
            {
                // claim DECIMAL for a type of our own, in both directions
                if (relType is not null && relType.getSqlTypeName() == SqlTypeName.DECIMAL && (clrType is null || clrType == typeof(Money)))
                    return new DelegateClrTypeMapping(context, relType, typeof(Money),
                        v => Apache.Calcite.Data.Common.CalciteValues.ToDecimal(((Money)v).Amount),
                        v => new Money((decimal)Apache.Calcite.Data.Common.CalciteValues.FromDecimal(v)));

                return null;
            }

        }

        readonly record struct Money(decimal Amount);

        /// <summary>
        /// Built, chained and resolved without touching a connection's internals, which is what an
        /// out-of-assembly consumer has to be able to do.
        /// </summary>
        [Fact]
        public void A_caller_should_be_able_to_build_and_chain_its_own_registry()
        {
            using var c = Open();

            var registry = new ClrTypeMapper()
                .Prepend(new MoneyResolver())
                .Bind(c.TypeFactory);

            var decimalType = c.TypeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);

            // the prepended resolver decides what a DECIMAL reads back as
            Assert.Equal(typeof(Money), registry.GetClrType(decimalType));

            // and both directions go through it
            var held = registry.ToCalcite(typeof(Money), decimalType, new Money(12.34m));
            Assert.Equal(new Money(12.34m), registry.FromCalcite(typeof(Money), decimalType, held));
        }

        /// <summary>
        /// Resetting and not prepending leaves the built-in answer, so the chain is doing the work rather
        /// than the entry being replaced outright.
        /// </summary>
        [Fact]
        public void The_built_in_chain_should_answer_where_nothing_was_prepended()
        {
            using var c = Open();

            var registry = new ClrTypeMapper().Bind(c.TypeFactory);
            var decimalType = c.TypeFactory.createSqlType(SqlTypeName.DECIMAL, 10, 2);

            Assert.Equal(typeof(decimal), registry.GetClrType(decimalType));
        }

        /// <summary>
        /// The question an introspecting caller asks, which is not the one <c>GetMapping</c> answers: which
        /// conversions are permitted, rather than which one wins.
        /// </summary>
        [Fact]
        public void A_caller_should_be_able_to_enumerate_what_a_type_can_be_read_as()
        {
            using var c = Open();

            var mapper = new ClrTypeMapper();
            var registry = mapper.Bind(c.TypeFactory);
            var timestamp = c.TypeFactory.createSqlType(SqlTypeName.TIMESTAMP);
            var context = new ClrTypeContextProbe(registry).Context;

            var types = DefaultClrTypeResolver.Instance.GetClrTypes(timestamp, context).ToArray();

            // the default comes first, and the conversions that are legal only when asked for follow
            Assert.Equal(typeof(DateTime), types[0]);
            Assert.Contains(typeof(DateOnly), types);
            Assert.Contains(typeof(TimeOnly), types);
            Assert.Contains(typeof(DateTimeOffset), types);
        }

        /// <summary>
        /// A <see cref="ClrTypeContext"/> is handed to a resolver rather than constructed by one, so a test
        /// that wants to call a resolver directly borrows the one the registry made.
        /// </summary>
        sealed class ClrTypeContextProbe
        {

            public ClrTypeContextProbe(ClrTypeRegistry registry)
            {
                Context = Capture(registry);
            }

            public ClrTypeContext Context { get; }

            static ClrTypeContext Capture(ClrTypeRegistry registry)
            {
                ClrTypeContext? captured = null;
                var mapper = new ClrTypeMapper().Prepend(new CapturingResolver(c => captured = c));
                mapper.Bind(registry.TypeFactory).GetMapping(null, registry.TypeFactory.createSqlType(SqlTypeName.INTEGER));

                return captured ?? throw new InvalidOperationException("no context was handed to a resolver");
            }

            sealed class CapturingResolver(Action<ClrTypeContext> capture) : IClrTypeResolver
            {

                public ClrTypeMapping? GetMapping(Type? clrType, RelDataType? relType, ClrTypeContext context)
                {
                    capture(context);
                    return null;
                }

            }

        }

        // ------------------------------------------------------------------------------------
        // Parameters: three ways to name the type, widening.
        // ------------------------------------------------------------------------------------

        [Fact]
        public void A_parameter_should_accept_a_calcite_type_name()
        {
            var p = new CalciteParameter { CalciteDbType = CalciteDbType.UInteger };

            Assert.Equal(CalciteDbType.UInteger, p.CalciteDbType);
            Assert.Equal(SqlTypeName.UINTEGER, CalciteDbTypes.ToSqlTypeName(p.CalciteDbType));
        }

        [Fact]
        public void A_parameter_should_accept_a_calcite_type()
        {
            using var c = Open();

            var type = c.TypeFactory.createArrayType(c.TypeFactory.createArrayType(c.TypeFactory.createSqlType(SqlTypeName.INTEGER), -1), -1);
            var p = new CalciteParameter { RelDataType = type };

            Assert.Same(type, p.RelDataType);

            // the flag reads the type it was given, down to where one bit runs out
            Assert.Equal(CalciteDbType.Array, p.CalciteDbType);

            // and the shared list has nothing for a collection at all
            Assert.Equal(System.Data.DbType.Object, p.DbType);
        }

        /// <summary>
        /// The three are views of one statement, so naming the type any of the three ways restates the
        /// other two rather than sitting beside them.
        /// </summary>
        [Fact]
        public void Setting_the_ado_name_should_restate_the_other_two()
        {
            var p = new CalciteParameter { DbType = System.Data.DbType.Int32 };

            Assert.Equal(System.Data.DbType.Int32, p.DbType);
            Assert.Equal(CalciteDbType.Integer, p.CalciteDbType);
            Assert.Null(p.RelDataType);
        }

        [Fact]
        public void Setting_the_calcite_name_should_restate_the_ado_one()
        {
            var p = new CalciteParameter { CalciteDbType = CalciteDbType.UInteger };

            Assert.Equal(CalciteDbType.UInteger, p.CalciteDbType);
            Assert.Equal(System.Data.DbType.UInt32, p.DbType);
            Assert.Null(p.RelDataType);
        }

        [Fact]
        public void Setting_the_calcite_type_should_restate_both_names()
        {
            using var c = Open();

            var p = new CalciteParameter { RelDataType = c.TypeFactory.createSqlType(SqlTypeName.UINTEGER) };

            Assert.Equal(CalciteDbType.UInteger, p.CalciteDbType);
            Assert.Equal(System.Data.DbType.UInt32, p.DbType);
        }

        /// <summary>
        /// A name cannot state a type, so restating from one drops the exact type rather than keeping a
        /// stale one beside a name that disagrees with it.
        /// </summary>
        [Fact]
        public void Naming_a_type_after_stating_one_should_drop_the_stated_type()
        {
            using var c = Open();

            var p = new CalciteParameter { RelDataType = c.TypeFactory.createSqlType(SqlTypeName.INTEGER) };
            Assert.NotNull(p.RelDataType);

            p.CalciteDbType = CalciteDbType.BigInt;

            Assert.Null(p.RelDataType);
            Assert.Equal(System.Data.DbType.Int64, p.DbType);
        }

        /// <summary>
        /// Clearing the stated type leaves the parameter as though nothing had been said about it, which is
        /// the state where the value's own type decides.
        /// </summary>
        [Fact]
        public void Clearing_the_stated_type_should_leave_the_value_deciding()
        {
            using var c = Open();

            var p = new CalciteParameter { Value = 5, RelDataType = c.TypeFactory.createSqlType(SqlTypeName.BIGINT) };
            Assert.Equal(System.Data.DbType.Int64, p.DbType);

            p.RelDataType = null;

            Assert.Equal(System.Data.DbType.Int32, p.DbType);
            Assert.Equal(CalciteDbType.Integer, p.CalciteDbType);
        }

        /// <summary>
        /// With nothing said, the value decides, and the Calcite name follows the same inference.
        /// </summary>
        [Fact]
        public void An_unstated_parameter_should_be_named_by_its_value()
        {
            Assert.Equal(CalciteDbType.Integer, new CalciteParameter { Value = 1 }.CalciteDbType);
            Assert.Equal(CalciteDbType.VarChar, new CalciteParameter { Value = "x" }.CalciteDbType);
            Assert.Equal(CalciteDbType.Unknown, new CalciteParameter().CalciteDbType);
        }

        [Fact]
        public void Resetting_a_parameter_should_clear_every_way_of_naming_its_type()
        {
            using var c = Open();

            var p = new CalciteParameter
            {
                DbType = System.Data.DbType.Int32,
                CalciteDbType = CalciteDbType.UInteger,
                RelDataType = c.TypeFactory.createSqlType(SqlTypeName.INTEGER),
            };

            p.ResetDbType();

            Assert.Equal(System.Data.DbType.Object, p.DbType);
            Assert.Equal(CalciteDbType.Unknown, p.CalciteDbType);
            Assert.Null(p.RelDataType);
        }

    }

}
