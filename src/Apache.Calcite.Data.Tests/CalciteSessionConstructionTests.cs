using System;
using System.Collections.Generic;
using System.Threading;

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

        /// <summary>
        /// A public static field, which is upstream's only <c>#Member</c> form.
        /// </summary>
        public static readonly TestTypeSystem Handle = new TestTypeSystem();

        public override int getMaxPrecision(SqlTypeName typeName)
        {
            return typeName == SqlTypeName.DECIMAL ? 21 : base.getMaxPrecision(typeName);
        }

    }

    /// <summary>
    /// A type system that widens <c>SUM</c> over an exact integer to <c>BIGINT</c>. Calcite's default
    /// <c>deriveSumType</c> answers the argument type, so <c>SUM</c> of an <c>INTEGER</c> column is an
    /// <c>INTEGER</c>; this is the shape of type system that changes which Java class a row carries.
    /// </summary>
    public class WideSumTypeSystem : RelDataTypeSystemImpl
    {

        public override RelDataType deriveSumType(RelDataTypeFactory typeFactory, RelDataType argumentType)
        {
            return typeFactory.createTypeWithNullability(
                typeFactory.createSqlType(SqlTypeName.BIGINT), argumentType.isNullable());
        }

    }

    /// <summary>
    /// A type system handed out by a public static field named <c>INSTANCE</c>, which the plugin machinery
    /// reads in preference to the default constructor when no member is named.
    /// </summary>
    public class InstanceTypeSystem : RelDataTypeSystemImpl
    {

        /// <summary>
        /// The instance.
        /// </summary>
        public static readonly InstanceTypeSystem INSTANCE = new InstanceTypeSystem();

        public override int getMaxPrecision(SqlTypeName typeName)
        {
            return typeName == SqlTypeName.DECIMAL ? 22 : base.getMaxPrecision(typeName);
        }

    }

    /// <summary>
    /// A type system handed out by a public static parameterless method, which is the .NET form of the
    /// <c>#Member</c> suffix and the one upstream's field-only lookup does not reach.
    /// </summary>
    public class MethodTypeSystem : RelDataTypeSystemImpl
    {

        /// <summary>
        /// Answers the instance.
        /// </summary>
        public static MethodTypeSystem Create()
        {
            return new MethodTypeSystem();
        }

        public override int getMaxPrecision(SqlTypeName typeName)
        {
            return typeName == SqlTypeName.DECIMAL ? 23 : base.getMaxPrecision(typeName);
        }

    }

    /// <summary>
    /// A type system behind a <see cref="ThreadLocal{T}"/>, which is the CLR spelling of the holder
    /// Avatica unwraps a <c>java.lang.ThreadLocal</c> for.
    /// </summary>
    public class ThreadLocalTypeSystem : RelDataTypeSystemImpl
    {

        /// <summary>
        /// The instance for the calling thread.
        /// </summary>
        public static readonly ThreadLocal<ThreadLocalTypeSystem> Current = new(() => new ThreadLocalTypeSystem());

        public override int getMaxPrecision(SqlTypeName typeName)
        {
            return typeName == SqlTypeName.DECIMAL ? 24 : base.getMaxPrecision(typeName);
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

        /// <summary>
        /// A connection string naming a type system. The name is quoted because an assembly-qualified one
        /// carries a comma, which the connection string would otherwise read as a separator.
        /// </summary>
        static string Cs(string typeSystem)
        {
            return TestModels.InlineEmptyModelConnectionString + ";TypeSystem=\"" + typeSystem + "\"";
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

        /// <summary>
        /// A type named by itself is constructed, which is the plain case and the only one a .NET user
        /// writing their own type system needs.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_construct_a_type_named_by_itself()
        {
            using var c = new CalciteConnection(Cs(typeof(TestTypeSystem).AssemblyQualifiedName!));
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

        /// <summary>
        /// The point of the member form. Calcite ships its type systems as anonymous classes behind
        /// public static fields, so they have no type to name and a type-only syntax cannot reach any of
        /// them at all.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_reach_a_type_system_calcite_ships()
        {
            using var c = new CalciteConnection(Cs("[org.apache.calcite.sql.dialect.MysqlSqlDialect, calcite.core]::MYSQL_TYPE_SYSTEM"));
            c.Open();
            Assert.Same(org.apache.calcite.sql.dialect.MysqlSqlDialect.MYSQL_TYPE_SYSTEM, c.TypeFactory.getTypeSystem());
        }

        /// <summary>
        /// A Java <c>static final</c> is a CLR property under IKVM rather than a field, so the member
        /// lookup has to try both.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_read_a_member_that_ikvm_surfaces_as_a_property()
        {
            using var c = new CalciteConnection(Cs("[org.apache.calcite.rel.type.RelDataTypeSystem, calcite.core]::DEFAULT"));
            c.Open();
            Assert.Same(RelDataTypeSystem.DEFAULT, c.TypeFactory.getTypeSystem());
        }

        [Fact]
        public void TypeSystem_option_should_read_a_static_field_member()
        {
            using var c = new CalciteConnection(Cs("[" + typeof(TestTypeSystem).AssemblyQualifiedName + "]::Handle"));
            c.Open();
            Assert.Same(TestTypeSystem.Handle, c.TypeFactory.getTypeSystem());
        }

        [Fact]
        public void TypeSystem_option_should_read_a_static_method_member()
        {
            using var c = new CalciteConnection(Cs("[" + typeof(MethodTypeSystem).AssemblyQualifiedName + "]::Create"));
            c.Open();
            Assert.Equal(23, c.TypeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL));
        }

        /// <summary>
        /// The thread-local holder Avatica unwraps, in its CLR spelling.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_unwrap_a_thread_local_member()
        {
            using var c = new CalciteConnection(Cs("[" + typeof(ThreadLocalTypeSystem).AssemblyQualifiedName + "]::Current"));
            c.Open();
            Assert.Equal(24, c.TypeFactory.getTypeSystem().getMaxPrecision(SqlTypeName.DECIMAL));
        }

        [Fact]
        public void TypeSystem_option_should_prefer_a_static_instance_member_to_the_constructor()
        {
            using var c = new CalciteConnection(Cs(typeof(InstanceTypeSystem).AssemblyQualifiedName!));
            c.Open();
            Assert.Same(InstanceTypeSystem.INSTANCE, c.TypeFactory.getTypeSystem());
        }

        /// <summary>
        /// Calcite writes a plugin member with a <c>#</c>, and a connection string copied out of its
        /// documentation should resolve rather than read as a missing type.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_read_calcites_own_member_spelling()
        {
            using var c = new CalciteConnection(Cs("org.apache.calcite.rel.type.RelDataTypeSystem, calcite.core#DEFAULT"));
            c.Open();
            Assert.Same(RelDataTypeSystem.DEFAULT, c.TypeFactory.getTypeSystem());
        }

        /// <summary>
        /// The name is resolved by <c>Type.GetType</c>, which searches the provider assembly and the core
        /// library and nowhere else, so a name without its assembly does not resolve however well formed.
        /// The message has to say so, this being the mistake a .NET user will actually make.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_say_so_when_the_name_omits_its_assembly()
        {
            using var c = new CalciteConnection(Cs(typeof(TestTypeSystem).FullName!));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("carries its assembly", e.InnerException?.Message);
        }

        [Fact]
        public void TypeSystem_option_should_fail_clearly_when_the_named_member_is_absent()
        {
            using var c = new CalciteConnection(Cs("[" + typeof(TestTypeSystem).AssemblyQualifiedName + "]::NoSuchMember"));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("NoSuchMember", e.InnerException?.Message);
        }

        [Fact]
        public void TypeSystem_option_should_fail_clearly_when_the_bracket_is_not_closed()
        {
            using var c = new CalciteConnection(Cs("[" + typeof(TestTypeSystem).AssemblyQualifiedName + "::Handle"));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("]::", e.InnerException?.Message);
        }

        [Fact]
        public void TypeSystem_option_should_fail_clearly_when_unresolvable()
        {
            using var c = new CalciteConnection(Cs("No.Such.Type"));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("No.Such.Type", e.InnerException?.Message);
        }

        /// <summary>
        /// A type that resolves but is not a type system is refused, rather than answered as one.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_refuse_a_type_that_is_not_a_type_system()
        {
            using var c = new CalciteConnection(Cs(typeof(CalciteSessionConstructionTests).AssemblyQualifiedName!));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("RelDataTypeSystem", e.InnerException?.Message);
        }

        /// <summary>
        /// The invariant the conventions rest on: a row carries Java boxed values, and which Java class
        /// it carries is <c>JavaTypeFactoryImpl.getJavaClass</c>, which reads the <c>SqlTypeName</c> and
        /// the nullability and nothing else. A type system cannot introduce a class outside that closed
        /// set, but it decides which member of it a query derives — so this is the half of the option
        /// that reaches the runtime, and the reader has to follow it.
        /// </summary>
        [Fact]
        public void A_derived_type_should_change_the_runtime_type_the_reader_answers()
        {
            const string Sql = "SELECT SUM(x) FROM (VALUES (1), (2)) AS t(x)";

            using (var dflt = new CalciteConnection(TestModels.InlineEmptyModelConnectionString))
            {
                dflt.Open();
                using var cmd = dflt.CreateCommand();
                cmd.CommandText = Sql;
                // Calcite's default deriveSumType answers the argument type, so this is an INTEGER
                Assert.Equal(typeof(int), cmd.ExecuteScalar()!.GetType());
            }

            using (var wide = new CalciteConnection(Cs(typeof(WideSumTypeSystem).AssemblyQualifiedName!)))
            {
                wide.Open();
                using var cmd = wide.CreateCommand();
                cmd.CommandText = Sql;
                Assert.Equal(typeof(long), cmd.ExecuteScalar()!.GetType());
            }
        }

        /// <summary>
        /// And the plan runs on the widened type rather than merely reporting it: the same query under
        /// the same type system through the synchronous convention agrees, both conventions inside one
        /// session sharing the session's type factory.
        /// </summary>
        [Fact]
        public void A_derived_type_should_hold_across_both_conventions()
        {
            const string Sql = "SELECT SUM(x) FROM (VALUES (1), (2)) AS t(x)";
            var typeSystem = Cs(typeof(WideSumTypeSystem).AssemblyQualifiedName!);

            using var async = new CalciteConnection(typeSystem);
            using var sync = new CalciteConnection(typeSystem + ";Synchronous=true");
            async.Open();
            sync.Open();

            using var a = async.CreateCommand();
            using var b = sync.CreateCommand();
            a.CommandText = Sql;
            b.CommandText = Sql;

            var left = a.ExecuteScalar();
            var right = b.ExecuteScalar();
            Assert.Equal(typeof(long), left!.GetType());
            Assert.Equal(left, right);
            Assert.Equal(3L, left);
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
