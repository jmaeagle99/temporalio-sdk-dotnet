namespace Temporalio.Worker
{
    /// <summary>
    /// Interface for external storage drivers reported in worker heartbeat.
    /// </summary>
    /// <remarks>
    /// WARNING: This API is experimental and may change in the future.
    /// </remarks>
    public interface ITemporalWorkerStorageDriver
    {
        /// <summary>
        /// Gets the storage driver type name.
        /// </summary>
        string Name { get; }
    }
}
