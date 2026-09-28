using System;

namespace Apache.Calcite.Adapter.AdoNet.Tests
{

    /// <summary>
    /// Owns the resources shared by the whole suite.
    /// </summary>
    /// <remarks>
    /// Registered as the assembly fixture in <c>AssemblyInfo.cs</c>, so it is disposed once, after the last
    /// test in the assembly.
    /// </remarks>
    public sealed class TestAssemblyHooks : IDisposable
    {

        /// <summary>
        /// Drops the LocalDB database the SQL Server, ODBC and OLE DB suites share.
        /// </summary>
        /// <remarks>
        /// The database is created once for all of those suites, so it is dropped here rather than per test.
        /// </remarks>
        public void Dispose()
        {
            SqlServerFixture.DisposeShared();
        }

    }

}
