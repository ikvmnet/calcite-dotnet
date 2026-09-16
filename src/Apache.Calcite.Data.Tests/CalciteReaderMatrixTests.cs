using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;

using org.apache.calcite;
using org.apache.calcite.rel.type;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;

using Xunit;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Every accessor the reader has, against every Calcite type that can be a column and every one that can
    /// be an array element, recorded.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <b>The recording is the assertion.</b> What a column may be read as is decided by the mapping table,
    /// one entry per pair of types, and no single hand-written assertion can show that the table says what
    /// it should: the interesting facts are the refusals, and there are far more of those than answers. So
    /// this writes the whole grid and compares it to <c>ReaderMatrix.txt</c>, and a change to what any
    /// accessor accepts is a diff to read rather than a test that happens not to exist.
    /// </para>
    /// <para>
    /// It is worth knowing what this caught being written: <c>GetInt32</c> answering 18263 for a
    /// <c>DATE</c>, because Calcite holds one as a count of days in a <c>java.lang.Integer</c> and the
    /// accessor matched that class; the same for five other temporal types. A per-accessor test would have
    /// had to think to ask.
    /// </para>
    /// <para>
    /// A type whose statement does not run is recorded as that rather than left out, so the grid says why a
    /// type is not covered. The unsigned types have no spelling Calcite's parser accepts here and
    /// <c>GEOMETRY</c> needs the spatial functions the test model does not load.
    /// </para>
    /// </remarks>
    public class CalciteReaderMatrixTests
    {

        /// <summary>
        /// Every Calcite type that can be a column, as an expression producing one.
        /// </summary>
        static readonly (string Label, string Sql)[] Types =
        [
            ("BOOLEAN", "TRUE"),
            ("TINYINT", "CAST(1 AS TINYINT)"),
            ("SMALLINT", "CAST(1 AS SMALLINT)"),
            ("INTEGER", "1"),
            ("BIGINT", "CAST(1 AS BIGINT)"),
            ("UTINYINT", "CAST(1 AS UTINYINT)"),
            ("USMALLINT", "CAST(1 AS USMALLINT)"),
            ("UINTEGER", "CAST(1 AS UINTEGER)"),
            ("UBIGINT", "CAST(1 AS UBIGINT)"),
            ("DECIMAL", "CAST(1.5 AS DECIMAL(5,2))"),
            ("REAL", "CAST(1.5 AS REAL)"),
            ("FLOAT", "CAST(1.5 AS FLOAT)"),
            ("DOUBLE", "CAST(1.5 AS DOUBLE)"),
            ("CHAR", "CAST('a' AS CHAR(1))"),
            ("CHAR(2)", "CAST('ab' AS CHAR(2))"),
            ("VARCHAR", "'ab'"),
            ("BINARY", "CAST(x'0102' AS BINARY(2))"),
            ("VARBINARY", "x'0102'"),
            ("DATE", "DATE '2020-01-02'"),
            ("TIME", "TIME '03:04:05'"),
            ("TIMESTAMP", "TIMESTAMP '2020-01-02 03:04:05'"),
            ("TIMESTAMP_TZ", "CAST(TIMESTAMP '2020-01-02 03:04:05' AS TIMESTAMP WITH TIME ZONE)"),
            ("TIMESTAMP_LTZ", "CAST(TIMESTAMP '2020-01-02 03:04:05' AS TIMESTAMP WITH LOCAL TIME ZONE)"),
            ("TIME_LTZ", "CAST(TIME '03:04:05' AS TIME WITH LOCAL TIME ZONE)"),
            ("INTERVAL_YM", "INTERVAL '1-2' YEAR TO MONTH"),
            ("INTERVAL_DT", "INTERVAL '1 2:3:4' DAY TO SECOND"),
            ("UUID", "CAST('123e4567-e89b-12d3-a456-426614174000' AS UUID)"),
            ("GEOMETRY", "ST_GeomFromText('POINT(1 2)')"),
            ("ROW", "ROW(1, 'a')"),
            ("MAP", "MAP['a', 1]"),
            ("NULL", "NULL"),
        ];

        /// <summary>
        /// The accessors that take an ordinal and nothing else.
        /// </summary>
        static readonly (string Name, Func<CalciteDataReader, object?> Get)[] Getters =
        [
            ("IsDBNull", r => r.IsDBNull(0)),
            ("GetFieldType", r => r.GetFieldType(0)),
            ("GetDataTypeName", r => r.GetDataTypeName(0)),
            ("GetValue", r => r.GetValue(0)),
            ("GetBoolean", r => r.GetBoolean(0)),
            ("GetString", r => r.GetString(0)),
            ("GetChar", r => r.GetChar(0)),
            ("GetByte", r => r.GetByte(0)),
            ("GetSByte", r => r.GetSByte(0)),
            ("GetInt16", r => r.GetInt16(0)),
            ("GetUInt16", r => r.GetUInt16(0)),
            ("GetInt32", r => r.GetInt32(0)),
            ("GetUInt32", r => r.GetUInt32(0)),
            ("GetInt64", r => r.GetInt64(0)),
            ("GetUInt64", r => r.GetUInt64(0)),
            ("GetFloat", r => r.GetFloat(0)),
            ("GetDouble", r => r.GetDouble(0)),
            ("GetDecimal", r => r.GetDecimal(0)),
            ("GetGuid", r => r.GetGuid(0)),
            ("GetDateTime", r => r.GetDateTime(0)),
            ("GetDateTimeOffset", r => r.GetDateTimeOffset(0)),
            ("GetDateOnly", r => r.GetDateOnly(0)),
            ("GetTimeOnly", r => r.GetTimeOnly(0)),
            ("GetTimeSpan", r => r.GetTimeSpan(0)),
            ("GetBytes", r => r.GetBytes(0, 0, null, 0, 0)),
            ("GetArray", r => r.GetArray(0)),
        ];

        /// <summary>
        /// Element types named explicitly, which is the ask <c>GetArray{T}</c> and
        /// <c>GetFieldValue{T}</c> have to answer alike.
        /// </summary>
        static readonly (string Sql, Type Element)[] Named =
        [
            ("SELECT ARRAY[DATE '2020-01-02']", typeof(DateTime)),
            ("SELECT ARRAY[DATE '2020-01-02']", typeof(DateOnly)),
            ("SELECT ARRAY[TIMESTAMP '2020-01-02 03:04:05']", typeof(DateOnly)),
            ("SELECT ARRAY[TIMESTAMP '2020-01-02 03:04:05']", typeof(TimeOnly)),
            ("SELECT ARRAY[TIME '03:04:05']", typeof(TimeOnly)),
            ("SELECT ARRAY[1, 2]", typeof(int)),
            ("SELECT ARRAY[1, 2]", typeof(int?)),
            ("SELECT ARRAY[1, 2]", typeof(long)),
            ("SELECT ARRAY[1, 2]", typeof(object)),
            ("SELECT ARRAY[1, NULL]", typeof(int)),
            ("SELECT ARRAY[1, NULL]", typeof(int?)),
            ("SELECT ARRAY[1, NULL]", typeof(object)),
            ("SELECT ARRAY['ab']", typeof(string)),
            ("SELECT ARRAY[x'0102']", typeof(byte[])),
            ("SELECT ARRAY[ARRAY[1, 2]]", typeof(int[])),
            ("SELECT MULTISET[DATE '2020-01-02']", typeof(DateOnly)),
            ("SELECT \"L\" FROM \"ANYT\"", typeof(int)),
            ("SELECT \"L\" FROM \"ANYT\"", typeof(object)),
        ];

        /// <remarks>
        /// <b>The session's time zone is pinned, because two of these types read by it.</b> A
        /// <c>TIMESTAMP WITH LOCAL TIME ZONE</c> and a <c>TIME WITH LOCAL TIME ZONE</c> are stored as an
        /// instant and rendered against the session zone, so the same statement answers 03:04:05 on a
        /// machine set to UTC and 09:04:05 on one at UTC-5 — correct both times, and not something a
        /// recording can hold unless the zone is stated. Nothing else in the suite reads either type, so
        /// this was the first thing to notice that they depend on it.
        /// </remarks>
        static CalciteConnection Open()
        {
            return new CalciteDataSourceBuilder(TestModels.InlineEmptyModelConnectionString + ";TimeZone=UTC")
                .ConfigureRootSchema(root =>
                {
                    root.add("JT", new JavaArrayTable());
                    root.add("ANYT", new AnyListTable());
                })
                .Build()
                .OpenConnection();
        }

        static CalciteDataReader Row(CalciteConnection c, string sql)
        {
            var cmd = c.CreateCommand();
            cmd.CommandText = sql;
            var r = (CalciteDataReader)cmd.ExecuteReader();
            Assert.True(r.Read());
            return r;
        }

        static string Name(Type t)
        {
            if (Nullable.GetUnderlyingType(t) is { } u)
                return Name(u) + "?";
            if (t.IsArray)
                return Name(t.GetElementType()!) + "[]";

            return t.Name;
        }

        /// <summary>
        /// Writes a value so that its type is as visible as its content, both being what is being recorded.
        /// </summary>
        static string Describe(object? v)
        {
            if (v is null)
                return "null";
            if (v is DBNull)
                return "DBNull";
            if (v is Type t)
                return Name(t);
            if (v is bool b)
                return b ? "true" : "false";
            if (v is DateTime dt)
                return dt.ToString("O");
            if (v is DateTimeOffset dto)
                return dto.ToString("O");

            if (v is Array a)
            {
                var s = new StringBuilder(Name(v.GetType())).Append(" [");
                for (var i = 0; i < a.Length; i++)
                    s.Append(i > 0 ? ", " : "").Append(Describe(a.GetValue(i)));

                return s.Append(']').ToString();
            }

            // invariant, so the recording does not move with the machine running it
            return $"{Name(v.GetType())} {Convert.ToString(v, System.Globalization.CultureInfo.InvariantCulture)}";
        }

        /// <summary>
        /// Runs one accessor and records its answer, a refusal counting as an answer.
        /// </summary>
        static string Try(Func<object?> f)
        {
            try
            {
                return Describe(f());
            }
            catch (TargetInvocationException e) when (e.InnerException is { } inner)
            {
                return inner is InvalidCastException ? "refused" : $"!! {inner.GetType().Name}";
            }
            catch (InvalidCastException)
            {
                return "refused";
            }
            catch (Exception e)
            {
                return $"!! {e.GetType().Name}";
            }
        }

        /// <summary>
        /// Calls a generic accessor with a type argument decided at run time, so the grid can ask for a type
        /// the column did not name.
        /// </summary>
        static object? Generic(CalciteDataReader r, string method, Type argument)
        {
            return typeof(CalciteDataReader)
                .GetMethods()
                .Single(m => m.Name == method && m.IsGenericMethodDefinition && m.GetParameters().Length == 1)
                .MakeGenericMethod(argument)
                .Invoke(r, [0]);
        }

        /// <remarks>
        /// One reader for the whole block. An accessor converts the value the row already holds and does not
        /// advance or consume anything, so a refusal leaves the next accessor exactly as it found it — and
        /// re-executing the statement once per accessor is some two thousand extra plans across the grid.
        /// </remarks>
        static void Block(StringBuilder b, CalciteConnection c, string label, string sql)
        {
            b.AppendLine($"== {label}  {sql}");

            CalciteDataReader r;

            // a statement the parser or validator refuses has no row to read, and saying so once beats a
            // column of identical failures
            try
            {
                r = Row(c, sql);
            }
            catch (Exception e)
            {
                b.AppendLine($"   <statement>        !! {e.InnerException?.InnerException?.GetType().Name ?? e.GetType().Name}");
                b.AppendLine();
                return;
            }

            using (r)
            {
                foreach (var (name, get) in Getters)
                    b.AppendLine($"   {name,-18} {Try(() => get(r))}");

                // GetFieldValue asked for what GetFieldType named, which is the documented way to read a
                // column and so the one that must never be the wrong answer
                Type? field = null;
                try
                {
                    field = r.GetFieldType(0);
                }
                catch
                {
                    // the GetFieldType row above already records it
                }

                b.AppendLine($"   {"GetFieldValue<F>",-18} {(field is null ? "refused" : Try(() => Generic(r, "GetFieldValue", field)))}");
            }

            b.AppendLine();
        }

        static string Build()
        {
            var b = new StringBuilder();

            b.AppendLine("# Every reader accessor against every Calcite type. Recorded, not hand-written:");
            b.AppendLine("# see CalciteReaderMatrixTests. A diff here is a change to what a column may be read as.");
            b.AppendLine();

            using var c = Open();

            b.AppendLine("################ as a column");
            b.AppendLine();
            foreach (var (label, sql) in Types)
                Block(b, c, label, $"SELECT {sql}");

            b.AppendLine("################ as an array element, and with a null beside it");
            b.AppendLine();
            foreach (var (label, sql) in Types.Where(t => t.Label != "NULL"))
            {
                Block(b, c, $"{label}[]", $"SELECT ARRAY[{sql}]");
                Block(b, c, $"{label}[] +null", $"SELECT ARRAY[{sql}, NULL]");
            }

            b.AppendLine("################ collections that are not a plain array of a scalar");
            b.AppendLine();
            Block(b, c, "ARRAY ARRAY", "SELECT ARRAY[ARRAY[1, 2]]");
            Block(b, c, "MULTISET MULTISET", "SELECT MULTISET[MULTISET[1, 2]]");
            Block(b, c, "null ARRAY", "SELECT CAST(NULL AS INTEGER ARRAY)");
            Block(b, c, "ANY holding a list", "SELECT \"L\" FROM \"ANYT\"");
            Block(b, c, "JavaType(String[])", "SELECT \"S\" FROM \"JT\"");

            b.AppendLine("################ naming an element type, which GetArray<E> and GetFieldValue<E[]> must answer alike");
            b.AppendLine();
            foreach (var (sql, element) in Named)
            {
                using var r = Row(c, sql);

                var array = Try(() => Generic(r, "GetArray", element));
                var field = Try(() => Generic(r, "GetFieldValue", element.MakeArrayType()));

                b.AppendLine($"== <{Name(element)}>  {sql}");
                b.AppendLine($"   GetArray<E>        {array}");
                b.AppendLine($"   GetFieldValue<E[]> {field}");
                b.AppendLine($"   agree              {(array == field ? "yes" : "NO")}");
                b.AppendLine();
            }

            return b.ToString().ReplaceLineEndings("\n");
        }

        static string Recorded()
        {
            using var s = typeof(CalciteReaderMatrixTests).Assembly.GetManifestResourceStream("Apache.Calcite.Data.Tests.ReaderMatrix.txt")
                ?? throw new InvalidOperationException("ReaderMatrix.txt is not embedded.");

            using var r = new System.IO.StreamReader(s);
            return r.ReadToEnd().ReplaceLineEndings("\n");
        }

        [Fact]
        public void The_reader_should_answer_as_recorded()
        {
            var actual = Build();
            var recorded = Recorded();

            if (actual == recorded)
                return;

            // the whole grid in a failure message is unreadable, so name the lines that moved and write the
            // new grid out for the diff that settles whether the change was wanted
            var a = actual.Split('\n');
            var e = recorded.Split('\n');
            var changes = new List<string>();

            var section = "";
            for (var i = 0; i < Math.Max(a.Length, e.Length) && changes.Count < 40; i++)
            {
                var left = i < e.Length ? e[i] : "<end>";
                var right = i < a.Length ? a[i] : "<end>";

                if (i < a.Length && a[i].StartsWith("== "))
                    section = a[i];

                if (left != right)
                    changes.Add($"{section}\n    recorded: {left}\n    actual:   {right}");
            }

            var path = System.IO.Path.Combine(System.IO.Path.GetTempPath(), "ReaderMatrix.actual.txt");
            System.IO.File.WriteAllText(path, actual);

            Assert.Fail($"The reader answers differently from ReaderMatrix.txt. Written to {path}.\n\n{string.Join("\n\n", changes)}");
        }

        /// <summary>
        /// An <c>ANY</c> column holding a list, so the untyped path has a collection to read.
        /// </summary>
        sealed class AnyListTable : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
                new RelDataTypeFactory.Builder(typeFactory)
                    .add("L", org.apache.calcite.sql.type.SqlTypeName.ANY)
                    .build();

            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                var list = new java.util.ArrayList();
                list.add(java.lang.Integer.valueOf(1));
                list.add(java.lang.Integer.valueOf(2));

                return org.apache.calcite.linq4j.Linq4j.singletonEnumerable(new object[] { list });
            }

        }

        /// <summary>
        /// A column typed by naming a Java class that happens to be an array, which Calcite calls
        /// <c>OTHER</c> and holds as a real .NET <c>string[]</c> rather than a <c>java.util.List</c>.
        /// </summary>
        sealed class JavaArrayTable : AbstractTable, ScannableTable
        {

            public override RelDataType getRowType(RelDataTypeFactory typeFactory) =>
                new RelDataTypeFactory.Builder(typeFactory)
                    .add("S", ((org.apache.calcite.adapter.java.JavaTypeFactory)typeFactory).createJavaType((java.lang.Class)typeof(string[])))
                    .build();

            public org.apache.calcite.linq4j.Enumerable scan(DataContext root)
            {
                return org.apache.calcite.linq4j.Linq4j.singletonEnumerable(new object[] { new[] { "a", "b" } });
            }

        }

    }

}
