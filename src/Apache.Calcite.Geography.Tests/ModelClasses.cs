using System.Runtime.CompilerServices;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Sets the <c>calcite.model.classes.allowed</c> system property so that Calcite may load this package's
    /// classes and its own by name.
    /// </summary>
    /// <remarks>
    /// Calcite loads a class named by a model (a schema factory, a function class, a driver) only when that
    /// property lists its package, and the default allows nothing, Calcite's own classes included.
    /// <c>SqlSpatialTypeOperatorTable</c>'s constructor registers
    /// <c>org.apache.calcite.runtime.SpatialTypeFunctions</c> through <c>ModelHandler.addFunctions</c>, so
    /// the operator table these tests plan with cannot be built without it.
    ///
    /// <para><c>CalciteSystemProperty</c> reads the property once, when it initialises, so it is set in a
    /// module initializer, before any test touches a Calcite class.</para>
    /// </remarks>
    internal static class ModelClasses
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            // A .NET class appears under its CLR name in a model and under its cli.-prefixed IKVM name where
            // Calcite writes getClass().getName() into a model it synthesises; Calcite's own functions need
            // an entry too.
            java.lang.System.setProperty(
                "calcite.model.classes.allowed",
                "Apache.Calcite.Geography.,cli.Apache.Calcite.Geography.,org.apache.calcite.");
        }

    }

}
