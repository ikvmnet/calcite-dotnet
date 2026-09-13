using System.Runtime.CompilerServices;

namespace Apache.Calcite.Data.Tests
{

    /// <summary>
    /// Lets a model name a class of this assembly.
    /// </summary>
    /// <remarks>
    /// From the snapshots of August 2026 a model may load a class by name — a schema factory, a function,
    /// a driver — only where the <c>calcite.model.classes.allowed</c> system property lists its package;
    /// the default is empty, and empty means nothing, Calcite's own factories included. The property is
    /// read once, when <c>CalciteSystemProperty</c> initialises, so it is set here, before any test can
    /// touch a Calcite class. <c>Apache.Calcite.Geography.Tests</c> has the same, for the operator table
    /// <c>SqlSpatialTypeOperatorTable</c> builds through <c>ModelHandler.addFunctions</c>.
    /// </remarks>
    internal static class ModelClasses
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            // a .NET class is named by its CLR name in a model and by its IKVM name, cli.-prefixed, where
            // Calcite writes a class's own getName() into a model it synthesises; and the factories Calcite
            // ships are not allowed by default either
            java.lang.System.setProperty("calcite.model.classes.allowed", "Apache.Calcite.Data.Tests.,cli.Apache.Calcite.Data.Tests.,org.apache.calcite.");
        }

    }

}
