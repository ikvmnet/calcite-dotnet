using System.Runtime.CompilerServices;

namespace Apache.Calcite.Geography.Tests
{

    /// <summary>
    /// Allows Calcite to load its own spatial functions, which the <c>spatial</c> function library needs.
    /// </summary>
    /// <remarks>
    /// Calcite's <c>SqlSpatialTypeOperatorTable</c> registers <c>SpatialTypeFunctions</c> through
    /// <c>ModelHandler.addFunctions</c>, which refuses any class the <c>calcite.model.classes.allowed</c> system
    /// property does not list, and the list is empty by default, so <c>Fun=spatial</c> fails without it. The
    /// property is read once, when <c>CalciteSystemProperty</c> initializes, so it is set in a module
    /// initializer, before any test touches a Calcite class.
    /// </remarks>
    internal static class CalciteClassAllowlist
    {

        [ModuleInitializer]
        internal static void Allow()
        {
            java.lang.System.setProperty("calcite.model.classes.allowed", "org.apache.calcite.");
        }

    }

}
