using System.Collections.Generic;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// DeleteTopics Response (Version: 0) => [topic_error_codes] 
    ///  topic_error_codes => topic error_code 
    ///    topic => STRING
    ///    error_code => INT16
    /// </summary>
    public class DeleteTopicsResponse : TopicsResponse
    {
        public DeleteTopicsResponse(IEnumerable<Topic> topics = null)
            : base (topics)
        {
        }
    }
}