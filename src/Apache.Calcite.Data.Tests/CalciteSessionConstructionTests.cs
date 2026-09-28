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
    /// A type system with a distinctive <c>DECIMAL</c> precision, for showing that the <c>TypeSystem</c>
    /// connection string option reaches the session's type factory. Public with a public parameterless
    /// constructor, which is what a type named without a member requires.
    /// </summary>
    public class TestTypeSystem : RelDataTypeSystemImpl
    {

        /// <summary>
        /// A public static field, the only member kind Avatica's <c>#MEMBER</c> form reads.
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
    /// <c>INTEGER</c>; this one changes which Java class a row carries.
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
    /// A type system handed out by a public static field named <c>INSTANCE</c>, which plugin resolution
    /// reads in preference to the parameterless constructor when no member is named.
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
    /// A type system handed out by a public static parameterless method, a member kind Avatica's
    /// field-only lookup does not reach.
    /// </summary>
    public class MethodTypeSystem : RelDataTypeSystemImpl
    {

        /// <summary>
        /// Answers the instance.
        /// </summary>
        /// <returns>A new instance, whose <c>DECIMAL</c> precision is 23.</returns>
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
    /// A type system behind a <see cref="ThreadLocal{T}"/>, the CLR counterpart of the
    /// <c>java.lang.ThreadLocal</c> Avatica unwraps.
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
    /// Covers what <c>CalciteSession</c> and <c>CalciteDataSourceRoot</c> port from
    /// <c>CalciteConnectionImpl</c>'s constructor: the <c>typeSystem</c> property, the conformance-driven
    /// ragged-union wrapper, the conformance-gated <c>DUAL</c> view, and injection of the root schema and
    /// type factory. The differential tests cannot see any of this, because every convention inside one
    /// session shares the session's type factory.
    /// </summary>
    public class CalciteSessionConstructionTests
    {

        /// <summary>
        /// Reads all rows of the first column as strings.
        /// </summary>
        /// <param name="c">An open connection.</param>
        /// <param name="sql">The query to run.</param>
        /// <returns>The first column of each row, with <see langword="null"/> for a SQL null.</returns>
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
        /// <param name="typeSystem">The <c>TypeSystem</c> option's value: a type name, optionally with a
        /// member.</param>
        /// <returns>The inline empty-model connection string with the option appended.</returns>
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
        /// An assembly-qualified type name with no member is constructed through its public constructor.
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
        /// Calcite ships its type systems as anonymous classes behind public static fields, so they have no
        /// type to name and only the member form reaches them.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_reach_a_type_system_calcite_ships()
        {
            using var c = new CalciteConnection(Cs("[org.apache.calcite.sql.dialect.MysqlSqlDialect, calcite.core]::MYSQL_TYPE_SYSTEM"));
            c.Open();
            Assert.Same(org.apache.calcite.sql.dialect.MysqlSqlDialect.MYSQL_TYPE_SYSTEM, c.TypeFactory.getTypeSystem());
        }

        /// <summary>
        /// IKVM exposes a Java <c>static final</c> field as a CLR property, so the member lookup tries
        /// properties as well as fields.
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
        /// A member holding a <see cref="ThreadLocal{T}"/> is unwrapped to the calling thread's value.
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
        /// Calcite writes a plugin member with a <c>#</c>, and that form is accepted too.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_read_calcites_own_member_spelling()
        {
            using var c = new CalciteConnection(Cs("org.apache.calcite.rel.type.RelDataTypeSystem, calcite.core#DEFAULT"));
            c.Open();
            Assert.Same(RelDataTypeSystem.DEFAULT, c.TypeFactory.getTypeSystem());
        }

        /// <summary>
        /// The name is resolved by <c>Type.GetType</c>, which searches only the provider assembly and the core
        /// library, so a name without its assembly does not resolve; the message says to add it.
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
        /// A type that resolves but is not a <c>RelDataTypeSystem</c> fails the open, naming the interface expected.
        /// </summary>
        [Fact]
        public void TypeSystem_option_should_refuse_a_type_that_is_not_a_type_system()
        {
            using var c = new CalciteConnection(Cs(typeof(CalciteSessionConstructionTests).AssemblyQualifiedName!));
            var e = Assert.Throws<CalciteException>(() => c.Open());
            Assert.Contains("RelDataTypeSystem", e.InnerException?.Message);
        }

        /// <summary>
        /// A row carries Java boxed values whose class <c>JavaTypeFactoryImpl.getJavaClass</c> chooses from
        /// the <c>SqlTypeName</c> and nullability. A type system cannot add a class to that set, but it
        /// decides which type a query derives, and so which class the reader returns.
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
        /// The synchronous and awaiting opens share the session's type factory, so both return the widened
        /// type and the same value.
        /// </summary>
        /// <returns>A task that completes when both opens have been checked.</returns>
        [Fact]
        public async System.Threading.Tasks.Task A_derived_type_should_hold_across_both_opens()
        {
            const string Sql = "SELECT SUM(x) FROM (VALUES (1), (2)) AS t(x)";
            var typeSystem = Cs(typeof(WideSumTypeSystem).AssemblyQualifiedName!);

            using var connection = new CalciteConnection(typeSystem);
            connection.Open();

            using var cmd = connection.CreateCommand();
            cmd.CommandText = Sql;

            var left = cmd.ExecuteScalar();
            var right = await cmd.ExecuteScalarAsync();
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
            var options = new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString + ";Conformance=PRAGMATIC_2003;TypeSystem=\"" + typeof(TestTypeSystem).AssemblyQualifiedName + "\"");
            var session = new CalciteSession(options, CalciteDataSourceRoot.Build(options, []), ownsRoot: true, typeFactory: typeFactory);
            Assert.Same(typeFactory, session.TypeFactory);
            Assert.Same(RelDataTypeSystem.DEFAULT, session.TypeFactory.getTypeSystem());
        }

        [Fact]
        public void Injected_root_schema_should_be_used_verbatim()
        {
            var root = CalciteSchema.createRootSchema(true);
            root.plus().add("PRE", new org.apache.calcite.schema.impl.AbstractSchema());
            var built = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(), [], rootSchema: root);
            Assert.Same(root, built.Schema);
            Assert.NotNull(built.Schema.plus().getSubSchema("PRE"));
        }

        [Fact]
        public void Model_should_apply_on_top_of_an_injected_root_schema()
        {
            var root = CalciteSchema.createRootSchema(true);
            root.plus().add("PRE", new org.apache.calcite.schema.impl.AbstractSchema());
            var built = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString), [], rootSchema: root);
            Assert.NotNull(built.Schema.plus().getSubSchema("PRE"));
            Assert.NotNull(built.Schema.plus().getSubSchema("adhoc"));
            Assert.Equal("adhoc", built.DefaultSchemaName);
        }

        [Fact]
        public void Dual_should_be_added_to_an_injected_root_schema()
        {
            // DUAL is a view macro, so it registers as a nullary function rather than a plain table
            var root = CalciteSchema.createRootSchema(true);
            var built = CalciteDataSourceRoot.Build(new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString + ";Conformance=ORACLE_12"), [], rootSchema: root);
            Assert.False(built.Schema.plus().getFunctions("DUAL").isEmpty());
        }

        /// <summary>
        /// A session disposes its root, and the schemas in it, only when it was created as the root's owner.
        /// </summary>
        [Fact]
        public void Session_should_dispose_the_root_only_where_it_owns_it()
        {
            var options = new CalciteConnectionStringBuilder(TestModels.InlineEmptyModelConnectionString);

            var shared = new DisposableSchema();
            var sharedRoot = CalciteDataSourceRoot.Build(options, [root => root.add("D", shared)]);
            new CalciteSession(options, sharedRoot, ownsRoot: false).Dispose();
            Assert.False(shared.Disposed);

            var owned = new DisposableSchema();
            var ownedRoot = CalciteDataSourceRoot.Build(options, [root => root.add("D", owned)]);
            new CalciteSession(options, ownedRoot, ownsRoot: true).Dispose();
            Assert.True(owned.Disposed);
        }

        sealed class DisposableSchema : org.apache.calcite.schema.impl.AbstractSchema, IDisposable
        {

            public bool Disposed { get; private set; }

            public void Dispose()
            {
                Disposed = true;
            }

        }

    }

}
