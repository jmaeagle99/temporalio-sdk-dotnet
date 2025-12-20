using Temporalio.Bridge.Interop;

namespace Temporalio.Bridge
{
    /// <summary>
    /// Safe handle for a Temporal client.
    /// </summary>
    internal sealed class ClientSafeHandle :
        UnmanagedSafeHandle<TemporalCoreClient>
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="ClientSafeHandle" /> class.
        /// </summary>
        /// <param name="ptr">Worker pointer.</param>
        public unsafe ClientSafeHandle(TemporalCoreClient* ptr)
            : base(ptr)
        {
        }

        /// <summary>
        /// Free the client.
        /// </summary>
        /// <returns>Always returns <c>true</c>.</returns>
        protected override unsafe bool ReleaseHandle()
        {
            Methods.temporal_core_client_free((TemporalCoreClient*)this.handle);
            return true;
        }
    }
}