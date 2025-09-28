using System;
using System.Data.Common;
using System.IO;

using java.sql;
using java.util;

using Microsoft.Data.Sqlite;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.csv;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;
using org.apache.calcite.plan;
using org.apache.calcite.tools;

namespace Apache.Calcite.Adapter.Ado.Tests
{

    [TestClass]
    public class AdoSchemaTests
    {

        static AdoSchemaTests()
        {
            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            DbProviderFactories.RegisterFactory("Microsoft.Data.Sqlite", SqliteFactory.Instance);
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

        [TestMethod]
        public void Test1()
        {
            SetupSqlite();

            var properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            using var connection = DriverManager.getConnection("jdbc:calcite:model=test.model", properties);
            var calciteConnection = (CalciteConnection)connection.unwrap(typeof(CalciteConnection));

            var rootSchema = calciteConnection.getRootSchema();
            var config = Frameworks.newConfigBuilder().defaultSchema(rootSchema).build();
            var planner = Frameworks.getPlanner(config);

            Console.WriteLine("SqlNode:");
            var sqlNode = planner.parse("SELECT * FROM \"testdb\".\"table1\"");
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

            using var statement = calciteConnection.createStatement();
            using var resultSet = statement.executeQuery("SELECT * FROM \"testdb\".\"table1\"");
            var c = resultSet.getMetaData().getColumnCount();

            for (int i = 0; i < c; i++)
            {
                Console.Write(resultSet.getMetaData().getColumnName(i + 1).PadLeft(20));
                Console.Write(" ");
            }

            Console.WriteLine();

            while (resultSet.next())
            {
                for (int i = 0; i < c; i++)
                {
                    var v = resultSet.getString(i + 1);
                    Console.Write(v.PadLeft(20));
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }

    }

}
