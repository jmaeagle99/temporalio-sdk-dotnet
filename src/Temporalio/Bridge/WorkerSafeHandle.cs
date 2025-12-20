using Temporalio.Bridge.Interop;

namespace Temporalio.Bridge
{
    /// <summary>
    /// Safe handle for a Temporal worker.
    /// </summary>
    internal sealed class WorkerSafeHandle :
        UnmanagedSafeHandle<TemporalCoreWorker>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="WorkerSafeHandle" /> class.
        /// </summary>
        /// <param name="worker">Worker pointer.</param>
        public unsafe WorkerSafeHandle(TemporalCoreWorker* worker)
            : base(worker)
        {
        }

        /// <summary>
        /// Free the worker.
        /// </summary>
        /// <returns>Always returns <c>true</c>.</returns>
        protected override unsafe bool ReleaseHandle()
        {
            Methods.temporal_core_worker_free((TemporalCoreWorker*)this.handle);
            return true;
        }
    }
}