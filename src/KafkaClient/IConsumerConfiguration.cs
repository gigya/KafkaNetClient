using System;
using KafkaClient.Common;

namespace KafkaClient
{
    public interface IConsumerConfiguration
    {
        /// <summary>
        /// Maximum bytes to accumulate in the response to a a FetchRequest (<see cref="KafkaClient.Protocol.FetchRequest.MaxBytes"/>). Note that this is not an absolute maximum, if the first message in the 
        /// first non-empty partition of the fetch is larger than this value, the message will still be returned to ensure that progress can be made.
        /// </summary>
        int? MaxFetchBytes { get; }

        /// <summary>
        /// The maximum bytes to include in the message set for a particular partition in a FetchRequest (<see cref="KafkaClient.Protocol.FetchRequest.Topic.MaxBytes"/>). This helps bound the size of the response.
        /// </summary>
        int? MaxPartitionFetchBytes { get; }

        /// <summary>
        /// This is the minimum number of bytes of messages that must be available to give a response.
        /// If the client sets this to 0 the server will always respond immediately, however if there is no new data since their last request they will just get back empty message sets.
        /// If this is set to 1, the server will respond as soon as at least one partition has at least 1 byte of data or the specified timeout occurs.
        /// By setting higher values in combination with the timeout the consumer can tune for throughput and trade a little additional latency for reading only large chunks of data
        /// (e.g. setting MaxWaitTime to 100 ms and setting MinBytes to 64k would allow the server to wait up to 100ms to try to accumulate 64k of data before responding).
        /// </summary>
        int? MinFetchBytes { get; }

        /// <summary>
        /// The max wait time is the maximum amount of time to block waiting if insufficient data is available at the time the request is issued in a FetchRequest (<see cref="KafkaClient.Protocol.FetchRequest.MaxWaitTime"/>).
        /// </summary>
        TimeSpan? MaxFetchServerWait { get; }

        /// <summary>
        /// Once the coordinator begins rebalancing, each member in the group has up to the session timeout in order to send a new JoinGroup request. 
        /// If they fail to do so, they will be removed from the group. In 0.10.1, a new version of the JoinGroup request was created with a separate 
        /// RebalanceTimeout field. Once a rebalance begins, each client has up to this duration to rejoin, but note that if the session timeout is 
        /// lower than the rebalance timeout, the client must still continue to send heartbeats.
        /// </summary>
        TimeSpan GroupRebalanceTimeout { get; }

        /// <summary>
        /// If the coordinator of a group does not receive at least one heartbeat (see below) before expiration of the session timeout, then the member 
        /// will be removed from the group. Prior to version 0.10.1, the session timeout was also used as the timeout to complete a needed rebalance.
        /// 
        /// The SessionTimeout field is used to indicate client liveness. It will be set to 2x the GroupHeartbeat in case there are network blips.
        /// </summary>
        TimeSpan GroupHeartbeat { get; }

        /// <summary>
        /// Used for retrying heartbeat and rebalance operations on groups.
        /// </summary>
        IRetry GroupCoordinationRetry { get; }

        /// <summary>
        /// The protocol name to use for this consumer.
        /// </summary>
        string GroupProtocol { get; }

        // fetch buffer size multiplier -- do I still need this??

    }
}