using System;

namespace KafkaClient
{
    /// <summary>
    /// An exception cause by invalid/missing/out-of-date metadata in the local metadata cache
    /// </summary>
    public class CachedMetadataException : KafkaException
    {
        public CachedMetadataException(string message)
            : base(message)
        {
        }

        public CachedMetadataException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public string TopicName { get; set; }
        public int? Partition { get; set; }
    }
}