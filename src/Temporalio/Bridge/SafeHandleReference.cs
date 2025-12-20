using System;
using System.Runtime.InteropServices;

namespace Temporalio.Bridge
{
    /// <summary>
    /// Helper class to ensure that a <see cref="SafeHandle" /> is properly referenced and dereferenced.
    /// </summary>
    internal abstract class SafeHandleReference :
        IDisposable
    {
        private readonly SafeHandle handle;
        private readonly bool owned;

        private bool disposed;

        /// <summary>
        /// Initializes a new instance of the <see cref="SafeHandleReference" /> class.
        /// </summary>
        /// <param name="handle">The safe handle to add a reference to.</param>
        /// <param name="owned">Indicates whether the safe handle is owned by this instance.</param>
        protected SafeHandleReference(SafeHandle handle, bool owned)
        {
            this.handle = handle;
            this.owned = owned;
        }

        /// <summary>
        /// Releases the reference to the safe handle.
        /// </summary>
        public void Dispose()
        {
            if (disposed)
            {
                return;
            }
            disposed = true;

            if (owned)
            {
                handle.Dispose();
            }
            else
            {
                handle.DangerousRelease();
            }
        }

        /// <summary>
        /// Gets the safe handle.
        /// </summary>
        /// <returns>The safe handle.</returns>
        protected SafeHandle GetHandle() => handle;
    }
}