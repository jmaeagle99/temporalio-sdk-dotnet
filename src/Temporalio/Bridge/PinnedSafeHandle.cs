using System.Runtime.InteropServices;
using Microsoft.Win32.SafeHandles;

namespace Temporalio.Bridge
{
    /// <summary>
    /// Safe handle for GC pinnable object.
    /// </summary>
    internal abstract class PinnedSafeHandle :
        SafeHandleZeroOrMinusOneIsInvalid
    {
        private readonly GCHandle gcHandle;

        /// <summary>
        /// Initializes a new instance of the <see cref="PinnedSafeHandle"/> class.
        /// </summary>
        /// <param name="value">The value to pin.</param>
        public PinnedSafeHandle(object value)
            : base(true)
        {
            gcHandle = GCHandle.Alloc(value, GCHandleType.Pinned);
            SetHandle(gcHandle.AddrOfPinnedObject());
        }

        /// <summary>
        /// Frees the pinned GC handle.
        /// </summary>
        /// <returns>Always returns <c>true</c>.</returns>
        protected override unsafe bool ReleaseHandle()
        {
            gcHandle.Free();
            return true;
        }
    }
}