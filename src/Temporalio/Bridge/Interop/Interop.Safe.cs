using System;

namespace Temporalio.Bridge.Interop
{
    internal static partial class Methods
    {
        public static unsafe WorkerSafeHandle temporal_core_worker_new(Runtime runtime, ClientSafeHandle client, PinnedSafeHandle<TemporalCoreWorkerOptions> options)
        {
            var workerOrFail = Interop.Methods.temporal_core_worker_new(
                (TemporalCoreClient*)client.DangerousGetHandle(),
                (TemporalCoreWorkerOptions*)options.DangerousGetHandle());

            if (workerOrFail.fail != null)
            {
                string failStr;
                using (var byteArray = new ByteArray(runtime, workerOrFail.fail))
                {
                    failStr = byteArray.ToUTF8();
                }
                throw new InvalidOperationException(failStr);
            }
            return new(workerOrFail.worker);
        }
    }
}