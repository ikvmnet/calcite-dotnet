using System;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Holds what the suite as a whole owns.
    /// </summary>
    /// <remarks>
    /// Registered as the assembly fixture in <c>AssemblyInfo.cs</c>, which is what gets it disposed after
    /// the last test in the assembly rather than after each one.
    /// </remarks>
    public sealed class TestAssemblyHooks : IDisposable
    {

        /// <summary>
        /// Drops the LocalDB database the SQL Server, ODBC and OLE DB suites share.
        /// </summary>
        /// <remarks>
        /// Here rather than in a test cleanup because it is made once for all of them: dropping it per test
        /// means creating it per test, and that is ten minutes of LocalDB.
        /// </remarks>
        public void Dispose()
        {
            SqlServerFixture.DisposeShared();
        }

    }

}
