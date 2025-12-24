using System;
using System.Collections.Generic;
using System.Runtime.InteropServices;
using System.Threading.Tasks;
using Google.Protobuf;
using Microsoft.Extensions.Logging;
using Temporalio.Bridge.Interop;

namespace Temporalio.Bridge
{
    /// <summary>
    /// Core-owned Temporal worker.
    /// </summary>
    internal class Worker : IDisposable
    {
        private readonly TemporalCoreWorkerCallback defaultCompletionCallack;
        private readonly TemporalCoreWorkerPollCallback pollActivityTaskCallback;
        private readonly TemporalCoreWorkerPollCallback pollNexusTaskCallback;
        private readonly TemporalCoreWorkerPollCallback pollWorkflowActivationCallback;

        /// <summary>
        /// Initializes a new instance of the <see cref="Worker"/> class.
        /// </summary>
        /// <param name="client">Client for the worker.</param>
        /// <param name="namespace_">Namespace for the worker.</param>
        /// <param name="options">Options for the worker.</param>
        /// <param name="loggerFactory">Logger factory, used instead of the one in options by
        ///   anything in the bridge that needs it, since it's guaranteed to be set.</param>
        /// <param name="clientPlugins">Client plugins to include in heartbeat.</param>
        /// <exception cref="Exception">
        /// If any of the options are invalid including improperly defined workflows/activities.
        /// </exception>
        public Worker(
            Client client,
            string namespace_,
            Temporalio.Worker.TemporalWorkerOptions options,
            ILoggerFactory loggerFactory,
            IReadOnlyCollection<Temporalio.Client.ITemporalClientPlugin>? clientPlugins = null)
        {
            Runtime = client.Runtime;
            using (var scope = new Scope())
            {
                HandleRef = SafeHandleReference<WorkerSafeHandle>.Owned(
                    Methods.temporal_core_worker_new(
                        client.Runtime,
                        client.HandleRef.Handle,
                        scope.PinnedHandle(options.ToInteropOptions(scope, namespace_, loggerFactory, clientPlugins))));
            }

            unsafe
            {
                defaultCompletionCallack = new TemporalCoreWorkerCallback(CompletionCallback);
                pollActivityTaskCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.ActivityTask.ActivityTask.Parser));
                pollNexusTaskCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.Nexus.NexusTask.Parser));
                pollWorkflowActivationCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.WorkflowActivation.WorkflowActivation.Parser));
            }
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Worker"/> class. For use when a worker
        /// pointer already exists (like for replayer).
        /// </summary>
        /// <param name="runtime">Runtime.</param>
        /// <param name="handle">Pointer.</param>
        internal unsafe Worker(Runtime runtime, WorkerSafeHandle handle)
            : this(runtime, SafeHandleReference<WorkerSafeHandle>.AddRef(handle))
        {
        }

        /// <summary>
        /// Initializes a new instance of the <see cref="Worker"/> class from another. Only for
        /// testing.
        /// </summary>
        /// <param name="other">Other worker to reference.</param>
        internal Worker(Worker other)
            : this(other.Runtime, SafeHandleReference<WorkerSafeHandle>.AddRef(other.HandleRef.Handle))
        {
        }

        private Worker(Runtime runtime, SafeHandleReference<WorkerSafeHandle> handleRef)
        {
            Runtime = runtime;
            HandleRef = handleRef;

            unsafe
            {
                defaultCompletionCallack = new TemporalCoreWorkerCallback(CompletionCallback);
                pollActivityTaskCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.ActivityTask.ActivityTask.Parser));
                pollNexusTaskCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.Nexus.NexusTask.Parser));
                pollWorkflowActivationCallback = CreatePollCompletionCallback(
                    (byteArray) => byteArray?.ToProto(Api.WorkflowActivation.WorkflowActivation.Parser));
            }
        }

        /// <summary>
        /// Gets the add ref/release handle.
        /// </summary>
        internal SafeHandleReference<WorkerSafeHandle> HandleRef { get; private init; }

        /// <summary>
        /// Gets the runtime associated with this worker.
        /// </summary>
        internal Runtime Runtime { get; private init; }

        /// <summary>
        /// Validate the worker.
        /// </summary>
        /// <returns>Validation task.</returns>
        public async Task ValidateAsync()
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_validate(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(defaultCompletionCallack));
                }
                await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Replace the client.
        /// </summary>
        /// <param name="client">New client.</param>
        public void ReplaceClient(Client client)
        {
            using (var scope = new Scope())
            {
                unsafe
                {
                    Interop.Methods.temporal_core_worker_replace_client(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.UnmanagedPointer(client.HandleRef.Handle));
                }
            }
        }

        /// <summary>
        /// Poll for the next workflow activation.
        /// </summary>
        /// <remarks>Only virtual for testing.</remarks>
        /// <returns>The activation or null if poller is shut down.</returns>
        public virtual async Task<Api.WorkflowActivation.WorkflowActivation?> PollWorkflowActivationAsync()
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<Api.WorkflowActivation.WorkflowActivation?>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_poll_workflow_activation(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(pollWorkflowActivationCallback));
                }
                return await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Poll for the next activity task.
        /// </summary>
        /// <remarks>Only virtual for testing.</remarks>
        /// <returns>The task or null if poller is shut down.</returns>
        public virtual async Task<Api.ActivityTask.ActivityTask?> PollActivityTaskAsync()
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<Api.ActivityTask.ActivityTask?>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_poll_activity_task(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(pollActivityTaskCallback));
                }
                return await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Poll for the next Nexus task.
        /// </summary>
        /// <remarks>Only virtual for testing.</remarks>
        /// <returns>The task or null if poller is shut down.</returns>
        public virtual async Task<Api.Nexus.NexusTask?> PollNexusTaskAsync()
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<Api.Nexus.NexusTask?>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_poll_nexus_task(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(pollNexusTaskCallback));
                }
                return await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Complete a workflow activation.
        /// </summary>
        /// <param name="comp">Activation completion.</param>
        /// <returns>Completion task.</returns>
        public async Task CompleteWorkflowActivationAsync(
            Api.WorkflowCompletion.WorkflowActivationCompletion comp)
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_complete_workflow_activation(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ByteArray(comp.ToByteArray()),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(defaultCompletionCallack));
                }
                await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Complete an activity task.
        /// </summary>
        /// <param name="comp">Task completion.</param>
        /// <returns>Completion task.</returns>
        public async Task CompleteActivityTaskAsync(Api.ActivityTaskCompletion comp)
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_complete_activity_task(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ByteArray(comp.ToByteArray()),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(defaultCompletionCallack));
                }
                await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Complete a Nexus task.
        /// </summary>
        /// <param name="comp">Task completion.</param>
        /// <returns>Completion task.</returns>
        public async Task CompleteNexusTaskAsync(Api.Nexus.NexusTaskCompletion comp)
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_complete_nexus_task(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ByteArray(comp.ToByteArray()),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(defaultCompletionCallack));
                }
                await completion.Task.ConfigureAwait(false);
            }
        }

        /// <summary>
        /// Record an activity heartbeat.
        /// </summary>
        /// <param name="heartbeat">Heartbeat to record.</param>
        public void RecordActivityHeartbeat(Api.ActivityHeartbeat heartbeat)
        {
            using (var scope = new Scope())
            {
                unsafe
                {
                    var fail = Interop.Methods.temporal_core_worker_record_activity_heartbeat(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ByteArray(heartbeat.ToByteArray()));
                    if (fail != null)
                    {
                        string failStr;
                        using (var byteArray = new ByteArray(Runtime, fail))
                        {
                            failStr = byteArray.ToUTF8();
                        }
                        throw new InvalidOperationException(failStr);
                    }
                }
            }
        }

        /// <summary>
        /// Request workflow eviction.
        /// </summary>
        /// <param name="runId">Run ID of the workflow to evict.</param>
        public void RequestWorkflowEviction(string runId)
        {
            using (var scope = new Scope())
            {
                unsafe
                {
                    Interop.Methods.temporal_core_worker_request_workflow_eviction(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ByteArray(runId));
                }
            }
        }

        /// <summary>
        /// Initiate shutdown for this worker.
        /// </summary>
        public void InitiateShutdown()
        {
            using (var scope = new Scope())
            {
                unsafe
                {
                    Interop.Methods.temporal_core_worker_initiate_shutdown(
                        scope.UnmanagedPointer(HandleRef.Handle));
                }
            }
        }

        /// <summary>
        /// Finalize shutdown of this worker. This should only be called after shutdown and all
        /// polling has stopped.
        /// </summary>
        /// <returns>Completion task.</returns>
        public async Task FinalizeShutdownAsync()
        {
            using (var scope = new Scope())
            {
                var completion = new TaskCompletionSource<bool>(
                    TaskCreationOptions.RunContinuationsAsynchronously);
                unsafe
                {
                    Interop.Methods.temporal_core_worker_finalize_shutdown(
                        scope.UnmanagedPointer(HandleRef.Handle),
                        scope.ManagedPointer(completion),
                        scope.FunctionPointer(defaultCompletionCallack));
                }
                await completion.Task.ConfigureAwait(false);
            }
        }

        /// <inheritdoc/>
        public void Dispose()
        {
            this.HandleRef.Dispose();
        }

        private unsafe TemporalCoreWorkerPollCallback CreatePollCompletionCallback<T>(Func<ByteArray?, T?> deserialize)
        {
            return (userData, success, fail) =>
            {
                using ByteArray failByteArray = new(Runtime, fail);
                using ByteArray successByteArray = new(Runtime, success);

                if (userData == null)
                {
                    return;
                }

                TaskCompletionSource<T?>? tcs = GCHandle.FromIntPtr((IntPtr)userData).Target as TaskCompletionSource<T?>;
                if (tcs == null)
                {
                    return;
                }

                if (fail != null)
                {
                    tcs.TrySetException(new InvalidOperationException(failByteArray.ToUTF8()));
                }
                else if (success != null)
                {
                    tcs.TrySetResult(deserialize(successByteArray));
                }
                else
                {
                    tcs.TrySetResult(deserialize(null));
                }
            };
        }

        private unsafe void CompletionCallback(void* userData, TemporalCoreByteArray* fail)
        {
            using ByteArray failByteArray = new(Runtime, fail);

            if (userData == null)
            {
                return;
            }

            TaskCompletionSource<bool>? tcs = GCHandle.FromIntPtr((IntPtr)userData).Target as TaskCompletionSource<bool>;
            if (tcs == null)
            {
                return;
            }

            if (fail != null)
            {
                tcs.TrySetException(new InvalidOperationException(failByteArray.ToUTF8()));
            }
            else
            {
                tcs.TrySetResult(true);
            }
        }
    }
}
