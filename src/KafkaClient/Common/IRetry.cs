using System;

namespace KafkaClient.Common
{
    public interface IRetry
    {
        /// <summary>
        /// The maximum time to wait across all attempts.
        /// </summary>
        TimeSpan? Timeout { get; }

        /// <summary>
        /// How much to delay before the next attempt.
        /// </summary>
        /// <param name="attempt">The attempt number.</param>
        /// <param name="timeTaken">How much time has already been taken.</param>
        /// <returns>null if no more attempts should be made</returns>
        TimeSpan? RetryDelay(int attempt, TimeSpan timeTaken);

        /// <summary>
        /// Whether to retry.
        /// </summary>
        /// <param name="attempt">The attempt number.</param>
        /// <param name="timeTaken">How much time has already been taken.</param>
        bool ShouldRetry(int attempt, TimeSpan timeTaken);
    }
}