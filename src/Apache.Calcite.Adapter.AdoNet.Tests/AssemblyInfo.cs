using Apache.Calcite.Adapter.AdoNet.Tests;

using Xunit;
using Xunit.Sdk;
using Xunit.v3;

// tests run one at a time: IKVM state is process wide (the boot class path, the Calcite system properties a
// module initializer sets, and the class initializers that read them), and xunit otherwise runs test
// collections in parallel
[assembly: Parallelization(Mode = ParallelMode.None)]

// the shared LocalDB database, dropped after the last test rather than after each one
[assembly: AssemblyFixture(typeof(TestAssemblyHooks))]
