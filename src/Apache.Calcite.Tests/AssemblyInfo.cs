using Xunit.Sdk;
using Xunit.v3;

// runs one test at a time: IKVM state is process wide (the boot class path, the Calcite system properties a
// module initializer sets, and the class initializers that read them), and xunit otherwise runs test
// collections in parallel
[assembly: Parallelization(Mode = ParallelMode.None)]
