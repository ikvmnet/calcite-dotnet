using Microsoft.VisualStudio.TestTools.UnitTesting;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Holds what the suite as a whole owns.
    /// </summary>
    [TestClass]
    public static class TestAssemblyHooks
    {

        /// <summary>
        /// Drops the LocalDB database the SQL Server, ODBC and OLE DB suites share.
        /// </summary>
        /// <remarks>
        /// Here rather than in a test cleanup because it is made once for all of them: dropping it per test
        /// means creating it per test, and that is ten minutes of LocalDB.
        /// </remarks>
        [AssemblyCleanup]
        public static void Cleanup()
        {
            SqlServerFixture.DisposeShared();
        }

    }

}
