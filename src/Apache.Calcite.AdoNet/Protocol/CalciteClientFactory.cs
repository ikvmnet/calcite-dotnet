using System;

namespace Apache.Calcite.AdoNet.Protocol
{

    /// <summary>
    /// Factory responsible for constructing an <see cref="ICalciteClient"/> from a parsed connection string.
    /// </summary>
    internal static class CalciteClientFactory
    {

        /// <summary>
        /// Creates a new client for the supplied options.
        /// </summary>
        /// <param name="options"></param>
        /// <returns></returns>
        public static ICalciteClient Create(CalciteConnectionStringBuilder options)
        {
            if (options is null)
                throw new ArgumentNullException(nameof(options));

            return new Apache.Calcite.AdoNet.Engine.CalciteEngineClient(options);
        }

    }

}
