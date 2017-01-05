using System;
using KafkaClient.Common;

namespace KafkaClient
{
    public interface IRouterConfiguration
    {
        /// <summary>
        /// Retry configuration for refreshing the cache.
        /// </summary>
        IRetry RefreshRetry { get; }

        /// <summary>
        /// The minimum time to cache metadata (unless explicitly forced to refresh).
        /// </summary>
        TimeSpan CacheExpiration { get; }
    }
}