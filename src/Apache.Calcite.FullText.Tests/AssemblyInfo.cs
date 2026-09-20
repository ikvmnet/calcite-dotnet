using Xunit.Sdk;
using Xunit.v3;

// one test at a time. IKVM state is process wide -- the boot class path, the Calcite system properties a
// module initializer sets, and every class initializer they feed -- and xunit runs test collections against
// one another unless this says otherwise.
[assembly: Parallelization(Mode = ParallelMode.None)]
