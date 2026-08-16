using System;
using System.Collections.Generic;

using Apache.Calcite.Data.Internal;

using org.apache.calcite.jdbc;
using org.apache.calcite.rel.type;
using org.apache.calcite.sql.type;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// A type system with a distinctive <c>DECIMAL</c> precision, for proving the <c>TypeSystem</c>
    /// connection string option reaches the session's type factory. Public with a public default
    /// constructor, which is what the plugin machinery requires of a plain class name.
    /// </summary>
    public class TestTypeSystem : RelDataTypeSystemImpl
    {

        public override int getMaxPrecision(SqlTypeName typeName)
        {
            return typeName == SqlTypeName.DECIMAL ? 21 : base.getMaxPrecision(typeName);
        }

    }

    /// <summary>
    /// Holds the constructor region <c>CalciteSession</c> ports from <c>CalciteConnectionImpl</c>: the
    /// <c>typeSystem</c> property, the conformance-driven ragged-union wrapper, the conformance-gated
    /// <c>DUAL</c> view, and the root schema and type factory injection seam. None of it is visible to the
    /// differential tests, because every convention inside one session shares the session's type factory
    /// and so agrees with the divergence.
    /// </summary>
    public class CalciteSessionConstructionTests
    {

        /// <summary>
        /// Reads all rows of the first column as strings.
        /// </summary>
        static List<string?> ReadStrings(CalciteConnection c, string sql)
        {
            using var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            using var reader = cmd.ExecuteReader();
            var values = new List<string?>();
            while (reader.Read())
                values.Add(reader.IsDBNull(0) ? null : reader.GetString(0));
            return values;
        }

        const string RaggedUnionSql =
            "SELECT * FROM (VALUES CAST('ab' AS CHAR(2))) UNION SELECT * FROM (VALUES CAST('abcde' AS CHAR(5)))";

        [Fact]
        public void Ragged_union_should_derive_varying_under_pragmatic_conformance()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";Conformance=PRAGMATIC_2003");
            c.Open();
            var values = ReadStrings(c, RaggedUnionSql);
            Assert.Contains("ab", values);
            Assert.Contains("abcde", values);
        }

        [Fact]
        public void Ragged_union_should_derive_padded_char_under_default_conformance()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            var values = ReadStrings(c, RaggedUnionSql);
            Assert.Contains("ab   ", values);
            Assert.Contains("abcde", values);
        }

        [Fact]
        public void TypeSystem_option_should_reach_the_type_factory()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";TypeSystem=\"" + typeof(TestTypeSystem).AssemblyQualifiedName + "\"");
            c.Open();
            var typeSystem = c.TypeFactory.getTypeSystem();
            Assert.IsType<TestTypeSystem>(typeSystem);
            Assert.Equal(21, typeSystem.getMaxPrecision(SqlTypeName.DECIMAL));
        }

        [Fact]
        public void TypeSystem_option_should_default_when_unset()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            Assert.Same(RelDataTypeSystem.DEFAULT, c.TypeFactory.getTypeSystem());
        }

        [Fact]
        public void TypeSystem_option_should_fail_clearly_when_unresolvable()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";TypeSystem=No.Such.Type");
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("No.Such.Type", e.InnerException?.Message);
        }

        [Fact]
        public void Dual_should_answer_non_simple_queries_under_oracle_conformance()
        {
            // deliberately not one of the SIMPLE_SQLS fast-path strings, so the catalog must hold DUAL
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString + ";Conformance=ORACLE_12");
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT 2 FROM DUAL";
            Assert.Equal(2, Convert.ToInt32(cmd.ExecuteScalar()));
        }

        [Fact]
        public void Dual_should_not_exist_under_default_conformance()
        {
            using var c = new CalciteConnection(TestModels.InlineEmptyModelConnectionString);
            c.Open();
            using var cmd = c.CreateCommand();
            cmd.CommandText = "SELECT 2 FROM DUAL";
            Assert.Throws<CalciteException>(() => cmd.ExecuteScalar());
        }

        [Fact]
        public void Injected_type_factory_should_bypass_type_system_and_ragged_union_wrapper()
        {
            var typeFactory = new JavaTypeFactoryImpl();
            var session = new CalciteSession(
                new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString + ";Conformance=PRAGMATIC_2003;TypeSystem=\"" + typeof(TestTypeSystem).AssemblyQualifiedName + "\""),
                typeFactory: typeFactory);
            Assert.Same(typeFactory, session.TypeFactory);
            Assert.Same(RelDataTypeSystem.DEFAULT, session.TypeFactory.getTypeSystem());
        }

        [Fact]
        public void Injected_root_schema_should_be_used_verbatim()
        {
            var root = CalciteSchema.createRootSchema(true);
            root.plus().add("PRE", new org.apache.calcite.schema.impl.AbstractSchema());
            var session = new CalciteSession(
                new CalciteConnectionStringBuilder(),
                rootSchema: root);
            Assert.Same(root, session.RootSchema.unwrap((java.lang.Class)typeof(CalciteSchema)));
            Assert.NotNull(session.RootSchema.getSubSchema("PRE"));
        }

        [Fact]
        public void Model_should_apply_on_top_of_an_injected_root_schema()
        {
            var root = CalciteSchema.createRootSchema(true);
            root.plus().add("PRE", new org.apache.calcite.schema.impl.AbstractSchema());
            var session = new CalciteSession(
                new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString),
                rootSchema: root);
            Assert.NotNull(session.RootSchema.getSubSchema("PRE"));
            Assert.NotNull(session.RootSchema.getSubSchema("adhoc"));
        }

        [Fact]
        public void Dual_should_be_added_to_an_injected_root_schema()
        {
            // DUAL is a view macro, so it registers as a nullary function rather than a plain table
            var root = CalciteSchema.createRootSchema(true);
            var session = new CalciteSession(
                new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString + ";Conformance=ORACLE_12"),
                rootSchema: root);
            Assert.False(session.RootSchema.getFunctions("DUAL").isEmpty());
        }

    }

}
