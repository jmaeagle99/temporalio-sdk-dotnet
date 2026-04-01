namespace Temporalio.Runtime
{
    /// <summary>
    /// Interface to implement for an up-down counter metric.
    /// </summary>
    /// <typeparam name="T">Type of value for the metric.</typeparam>
    public interface ICustomMetricUpDownCounter<T> : ICustomMetric<T>
        where T : struct
    {
        /// <summary>
        /// Add the given value to the up-down counter. The value may be negative.
        /// </summary>
        /// <param name="value">Value to add. Currently this will always be a <c>long</c>.</param>
        /// <param name="tags">Tags. This will be the same value/type as returned from
        /// <see cref="ICustomMetricMeter.CreateTags" />.</param>
        void Add(T value, object tags);
    }
}
