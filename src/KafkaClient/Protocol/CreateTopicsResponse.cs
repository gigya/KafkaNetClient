using System.Collections.Generic;

namespace KafkaClient.Protocol
{
    /// <summary>
    /// CreateTopics Response => [topic_error_codes] 
    ///  topic_error_codes => topic error_code 
    ///    topic => STRING
    ///    error_code => INT16
    /// </summary>
    public class CreateTopicsResponse : TopicsResponse
    {
        public CreateTopicsResponse(IEnumerable<Topic> topics = null)
            : base (topics)
        {
        }
    }
}