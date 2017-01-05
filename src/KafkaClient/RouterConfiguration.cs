using System;
using KafkaClient.Common;

namespace KafkaClient
{
    public class RouterConfiguration : IRouterConfiguration
    {
        public RouterConfiguration(IRetry refreshRetry = null, TimeSpan? cacheExpiration = null)
        {
            RefreshRetry = refreshRetry ?? Defaults.RefreshRetry();
            CacheExpiration = cacheExpiration ?? TimeSpan.FromMilliseconds(Defaults.CacheExpirationMilliseconds);
        }

        /// <inheritdoc />
        public IRetry RefreshRetry { get; }

        /// <inheritdoc />
        public TimeSpan CacheExpiration { get; }

        public static class Defaults
        {
            /// <summary>
            /// The default timeout for requests made to refresh the cache
            /// </summary>
            public const int RefreshTimeoutSeconds = 200;

            /// <summary>
            /// The default maximum number of attempts made when refreshing the cache
            /// </summary>
            public const int MaxRefreshAttempts = 2;

            /// <summary>
            /// The default RefreshRetry backoff delay
            /// </summary>
            public const int RefreshDelayMilliseconds = 100;

            /// <summary>
            /// The default expiration length for <see cref="RouterConfiguration.CacheExpiration"/>
            /// </summary>
            public const int CacheExpirationMilliseconds = 10;

            public static IRetry RefreshRetry(TimeSpan? timeout = null)
            {
                return new BackoffRetry(
                    timeout ?? TimeSpan.FromSeconds(RefreshTimeoutSeconds), 
                    TimeSpan.FromMilliseconds(RefreshDelayMilliseconds), 
                    MaxRefreshAttempts,
                    true);
            }
        }
    }
}