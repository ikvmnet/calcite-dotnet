using System.Runtime.CompilerServices;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Lets a model name a class, which on 1.43 it may not by default.
    /// </summary>
    /// <remarks>
    /// From the snapshots of August 2026 a model may load a class by name — a schema factory, a function, a
    /// driver — only where the <c>calcite.model.classes.allowed</c> system property lists its package. The
    /// default is empty, and empty means nothing: Calcite's own classes are refused along with everyone
    /// else's. <c>SqlSpatialTypeOperatorTable</c>'s constructor registers
    /// <c>org.apache.calcite.runtime.SpatialTypeFunctions</c> through <c>ModelHandler.addFunctions</c>, so
    /// building the operator table this suite plans with throws without it.
    ///
    /// <para>The property is read once, when <c>CalciteSystemProperty</c> initialises, so it is set here,
    /// before any test can touch a Calcite class. <c>Apache.Calcite.Data.Tests</c> has the same for the same
    /// reason.</para>
    /// </remarks>
    internal static class ModelClasses
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            // a .NET class is named by its CLR name in a model and by its IKVM name, cli.-prefixed, where
            // Calcite writes a class's own getName() into a model it synthesises; and the functions Calcite
            // ships are not allowed by default either
            java.lang.System.setProperty(
                "calcite.model.classes.allowed",
                "Apache.Calcite.Geography.,cli.Apache.Calcite.Geography.,org.apache.calcite.");
        }

    }

}
