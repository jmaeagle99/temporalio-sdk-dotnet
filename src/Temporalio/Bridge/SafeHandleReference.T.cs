using System;
using System.Runtime.InteropServices;

namespace Temporalio.Bridge
{
    /// <summary>
    /// A reference to a safe handle that can be used to manage the lifecycle of the handle.
    /// </summary>
    /// <typeparam name="T">The type of the safe handle to reference.</typeparam>
    internal sealed class SafeHandleReference<T>
        : SafeHandleReference
        where T : SafeHandle
    {
        private SafeHandleReference(T handle, bool owned)
            : base(handle, owned)
        {
        }

        /// <summary>
        /// Gets the safe handle that this reference owns or references.
        /// </summary>
        public T Handle => (T)GetHandle();

        /// <summary>
        /// Creates a new <see cref="SafeHandleReference" /> instance that owns the safe handle.
        /// </summary>
        /// <param name="handle">The safe handle to own.</param>
        /// <returns>A new <see cref="SafeHandleReference" /> instance that owns the safe handle.</returns>
        public static SafeHandleReference<T> Owned(T handle)
        {
            return new SafeHandleReference<T>(handle, owned: true);
        }

        /// <summary>
        /// Creates a new <see cref="SafeHandleReference" /> instance that references the safe handle.
        /// </summary>
        /// <param name="handle">The safe handle to reference.</param>
        /// <returns>A new <see cref="SafeHandleReference" /> instance that references the safe handle.</returns>
        public static SafeHandleReference<T> AddRef(T handle)
        {
            bool success = false;
            handle.DangerousAddRef(ref success);
            if (!success)
            {
                // Documentation states that DangerousAddRef will throw if the handle is disposed
                // (and there should be no other condition under which refAdded would rename false),
                // but throw just in case it returns without throwing.
                throw new InvalidOperationException("Safe handle is no longer valid.");
            }
            return new SafeHandleReference<T>(handle, owned: false);
        }
    }
}