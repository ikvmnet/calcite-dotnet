using System.Runtime.CompilerServices;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Allows a Calcite model to load classes of this assembly and of Calcite by name.
    /// </summary>
    /// <remarks>
    /// Calcite loads a class a model names (a schema factory, a function, a driver) only where the
    /// <c>calcite.model.classes.allowed</c> system property lists its package, and the default list is
    /// empty. The property is read once, when <c>CalciteSystemProperty</c> initializes, so it is set in a
    /// module initializer, before any test touches a Calcite class.
    /// </remarks>
    internal static class ModelClasses
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            // a .NET class is listed under both its CLR name and its IKVM name (cli.-prefixed), since Calcite
            // writes a class's getName() into a model it synthesizes; Calcite's own factories need listing too
            java.lang.System.setProperty("calcite.model.classes.allowed", "Apache.Calcite.Data.Tests.,cli.Apache.Calcite.Data.Tests.,org.apache.calcite.");
        }

    }

}
