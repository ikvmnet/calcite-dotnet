using System;
using System.Data.Common;

using java.sql;
using java.util;
using java.util.logging;

using Microsoft.Data.SqlClient;
using Microsoft.VisualStudio.TestTools.UnitTesting;

using org.apache.calcite.adapter.jdbc;
using org.apache.calcite.config;
using org.apache.calcite.jdbc;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    [TestClass]
    public class AdoSchemaTests
    {

        static AdoSchemaTests()
        {
            java.lang.System.setProperty("calcite.debug", "true");
            java.lang.System.setProperty("org.codehaus.janino.source_debugging.enable", "true");

            var logger = Logger.getLogger("com.microsoft.sqlserver.jdbc");
            logger.setLevel(Level.ALL);
            ConsoleHandler handler = new ConsoleHandler();
            handler.setLevel(Level.ALL);
            handler.setFormatter(new SimpleFormatter());
            logger.addHandler(handler);
            logger.severe("This is a SEVERE message.");

            ikvm.runtime.Startup.addBootClassPathAssembly(typeof(AdoSchemaFactory).Assembly);
            DbProviderFactories.RegisterFactory("Microsoft.Data.SqlClient", SqlClientFactory.Instance);
            GC.KeepAlive(typeof(com.microsoft.sqlserver.jdbc.SQLServerDriver));
            java.lang.Class.forName("com.microsoft.sqlserver.jdbc.SQLServerDriver");
            java.lang.Class.forName("org.apache.calcite.jdbc.Driver");
        }

        [TestMethod]
        public void Test1()
        {
            var properties = new Properties();
            properties.setProperty(CalciteConnectionProperty.CASE_SENSITIVE.camelName(), "false");
            using var connection = DriverManager.getConnection("jdbc:calcite:model=sample.model", properties);
            var calciteConnection = (CalciteConnection)connection.unwrap(typeof(CalciteConnection));

            using var statement = calciteConnection.createStatement();
            using var resultSet = statement.executeQuery("SELECT * FROM trailmates1.\"User\" AS u INNER JOIN trailmates1.UserProfile as up ON u.Id = up.UserId");
            var c = resultSet.getMetaData().getColumnCount();

            for (int i = 0; i < c; i++)
            {
                Console.Write(resultSet.getMetaData().getColumnName(i + 1));
                Console.Write(" ");
            }
            Console.WriteLine();

            while (resultSet.next())
            {
                for (int i = 0; i < c; i++)
                {
                    var v = resultSet.getString(i + 1);
                    System.Console.Write(v);
                    Console.Write(" ");
                }

                Console.WriteLine();
            }
        }

    }

}
