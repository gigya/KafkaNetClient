namespace KafkaClient.Common
{
    public class RetryAttempt<T>
    {
        private static RetryAttempt<T> _retry;
        public static RetryAttempt<T> Retry => _retry ?? (_retry = new RetryAttempt<T>(default(T), false));

        private static RetryAttempt<T> _abort;
        public static RetryAttempt<T> Abort => _abort ?? (_abort = new RetryAttempt<T>(default(T), false, false));

        public RetryAttempt(T value, bool isSuccessful = true, bool shouldRetry = true)
        {
            Value = value;
            IsSuccessful = isSuccessful;
            ShouldRetry = shouldRetry;
        }

        public bool IsSuccessful { get; }
        public bool ShouldRetry { get; }
        public T Value { get; }
    }
}