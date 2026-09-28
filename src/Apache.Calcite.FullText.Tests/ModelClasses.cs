using System.Runtime.CompilerServices;

namespace Apache.Calcite.FullText.Tests
{

    /// <summary>
    /// Allows Calcite models to load classes from this package and from Calcite by name.
    /// </summary>
    /// <remarks>
    /// Calcite loads a class a model names (a schema factory, a function, a driver) only where the
    /// <c>calcite.model.classes.allowed</c> system property lists its package, and an empty list allows
    /// none. The property is read once, when <c>CalciteSystemProperty</c> initializes, so it is set in a
    /// module initializer, before any test touches a Calcite class.
    /// </remarks>
    internal static class ModelClasses
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            // a .NET class appears under its CLR name in a written model and under its cli.-prefixed IKVM
            // name where Calcite writes getName() into a model it synthesizes; Calcite's own classes need
            // listing too
            java.lang.System.setProperty(
                "calcite.model.classes.allowed",
                "Apache.Calcite.FullText.,cli.Apache.Calcite.FullText.,org.apache.calcite.");
        }

    }

}
