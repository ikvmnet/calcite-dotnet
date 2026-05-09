using System;
using System.Threading;
using System.Threading.Tasks;

namespace Apache.Calcite.AdoNet.Protocol
{

    /// <summary>
    /// Abstraction over the Calcite execution engine used by the ADO.NET surface.
    /// </summary>
    /// <remarks>
    /// This interface intentionally hides the concrete engine so that the ADO.NET layer is decoupled
    /// from any specific runtime (in-process, embedded, or remote). The engine implementation is
    /// expected to be a native .NET implementation of Calcite semantics, not a wrapper over JDBC.
    /// </remarks>
    internal interface ICalciteClient : IDisposable
    {

        /// <summary>
        /// Executes the supplied request and returns a result handle.
        /// </summary>
        /// <param name="request"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        Task<ICalciteResult> ExecuteAsync(CalciteExecuteRequest request, CancellationToken cancellationToken);

    }

}
