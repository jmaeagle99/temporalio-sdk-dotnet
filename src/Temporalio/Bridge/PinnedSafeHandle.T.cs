namespace Temporalio.Bridge
{
    /// <summary>
    /// A <see cref="PinnedSafeHandle"/> that can hold a value of type <typeparamref name="T"/>.
    /// </summary>
    /// <typeparam name="T">Type that will be GC pinned.</typeparam>
    internal sealed class PinnedSafeHandle<T> :
        PinnedSafeHandle
        where T : unmanaged
    {
        /// <summary>
        /// Initializes a new instance of the <see cref="PinnedSafeHandle{T}"/> class that holds the specified value.
        /// </summary>
        /// <param name="value">Value that will be GC pinned.</param>
        public PinnedSafeHandle(T value)
            : base(value)
        {
        }
    }
}