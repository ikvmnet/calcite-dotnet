using System;
using System.Data.Common;
using System.IO;

using com.google.common.collect;

using IKVM.Jdbc.Data;

using java.awt.print;
using java.lang;
using java.sql;
using java.util;

using javax.xml.bind;
using javax.xml.bind.annotation;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.csv;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.schema;
using org.apache.calcite.schema.impl;
using org.apache.calcite.tools;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    [TestClass]
    public class AdoSchemaTests
    {

        static AdoSchemaTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            DbProviderFactories.RegisterFactory("Microsoft.Data.Sqlite", SqliteFactory.Instance);
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(org.apache.calcite.jdbc.CalciteJdbc41Factory).Assembly);
            java.lang.Class.forName("org.apache.calcite.jdbc.CalciteJdbc41Factory");
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(CsvSchemaFactory).Assembly);
            java.lang.Class.forName("org.apache.calcite.adapter.csv.CsvSchemaFactory");
        }

        /// <summary>
        /// Configures the SQLlite database.
        /// </summary>
        void SetupSqlite()
        {
            // clean existing database
            if (File.Exists("test.db"))
                File.Delete("test.db");

            // create and open new database
            using var cnn = new SqliteConnection("Data Source=./test.db");
            cnn.Open();

            // create table
            using (var cmd = new SqliteCommand("CREATE TABLE IF NOT EXISTS table1 (Id INTEGER PRIMARY KEY, Text NVARCHAR(1024) NULL)", cnn))
                cmd.ExecuteNonQuery();

            // person 2
            for (int i = 0; i < 256; i++)
            {
                using var cmd = new SqliteCommand("INSERT INTO table1 (Text) VALUES ($Name)", cnn);
                cmd.Parameters.AddWithValue("$Name", $"Person {i}");
                cmd.ExecuteNonQuery();
            }
        }

        /// <summary>
        /// Creates a schema reading from the ADO Sqlite connection.
        /// </summary>
        /// <param name="rootSchema"></param>
        void CreateSqliteSchema(SchemaPlus rootSchema)
        {
            SetupSqlite();

            var properties = new HashMap();
            properties.put("adoProviderName", "Microsoft.Data.Sqlite");
            properties.put("adoConnectionString", "Data Source=./test.db");
            properties.put("adoDatabaseMetadata", "Apache.Calcite.Adapter.Ado.Metadata.SqliteDatabaseMetadata");
            properties.put("adoSchema", "");
            var s = AdoSchemaFactory.Instance.create(rootSchema, "testdb", properties);
            rootSchema.add("testdb", s);
        }

        /// <summary>
        /// Creates a schema reading from the CSV files.
        /// </summary>
        /// <param name="rootSchema"></param>
        void CreateCsvSchema(SchemaPlus rootSchema)
        {
            var properties = new HashMap();
            properties.put("directory", System.IO.Path.Combine(System.IO.Path.GetDirectoryName(typeof(AdoSchemaTests).Assembly.Location), "csv"));
            properties.put("flavor", "translatable");
            var s = CsvSchemaFactory.INSTANCE.create(rootSchema, "csv", properties);
            rootSchema.add("csv", s);
        }

        /// <summary>
        /// Creates a new adhoc schema containing virtual tables.
        /// </summary>
        /// <param name="rootSchema"></param>
        void CreateAdhocSchema(SchemaPlus rootSchema)
        {
            var people = """
                SELECT      T."Id"                  AS "Id",
                            T."Text" || '_'         AS "Name",
                            P."Age"                 AS "Age"
                FROM        "testdb"."table1"       T
                INNER JOIN  "csv"."people"          P
                    ON      P."Id" = T."Id"
                """;

            var s = new AbstractSchema();
            var p = rootSchema.add("adhoc", s);
            p.add("people", ViewTable.viewMacro(p, people, ImmutableList.of("adhoc"), ImmutableList.of("adhoc", "people"), null));
        }

        /// <summary>
        /// Complex hand built example for testing.
        /// </summary>
        [TestMethod]
        public void ComplexSample()
        {
            JAXBContext context = JAXBContext.newInstance(typeof(Book));
            var book = (Book)context.createUnmarshaller().unmarshal(new java.io.FileReader("./book.xml"));











            var properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            var jdbcConnection = DriverManager.getConnection("jdbc:calcite:", properties);
            var calciteConnection = (CalciteConnection)jdbcConnection.unwrap(typeof(CalciteConnection));
            using var connection = new JdbcConnection(jdbcConnection);

            var rootSchema = calciteConnection.getRootSchema();
            CreateSqliteSchema(rootSchema);
            CreateCsvSchema(rootSchema);
            CreateAdhocSchema(rootSchema);

            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var planner = Frameworks.getPlanner(config);

            var query = """ SELECT * FROM "adhoc"."people" P WHERE P."Id" > 10 AND P."Name" LIKE 'Per%' """;

            Console.WriteLine("SqlNode:");
            var sqlNode = planner.parse(query);
            sqlNode = planner.validate(sqlNode);
            Console.WriteLine(sqlNode.toString());
            Console.WriteLine();

            Console.WriteLine("RelRoot:");
            var relRoot = planner.rel(sqlNode);
            Console.WriteLine(relRoot.toString());
            Console.WriteLine();

            Console.WriteLine("RelNode:");
            var relNode = relRoot.project();
            Console.WriteLine(RelOptUtil.toString(relNode));
            Console.WriteLine();

            Console.WriteLine("Plan:");
            using (var statement = connection.CreateCommand())
            {
                statement.CommandText = $""" EXPLAIN PLAN FOR {query} """;
                Console.WriteLine((string?)statement.ExecuteScalar() + " ");
            }

            using (var statement = connection.CreateCommand())
            {
                statement.CommandText = query;
                using var reader = statement.ExecuteReader();

                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write(reader.GetName(i).PadLeft(20));
                    Console.Write(" ");
                }

                Console.WriteLine();

                while (reader.Read())
                {
                    for (int i = 0; i < reader.FieldCount; i++)
                    {
                        var v = reader.GetValue(i);
                        Console.Write(v.ToString().PadLeft(20));
                        Console.Write(" ");
                    }

                    Console.WriteLine();
                }
            }
        }

    }

}
