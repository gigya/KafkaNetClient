using System.Collections.Generic;
using System.Collections.Immutable;
using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public class VersionSupport : IVersionSupport
    {
        private static VersionSupport _kafka8;
        /// <summary>
        /// 0.8.2 &lt;= Version &lt; 0.9
        /// </summary>
        public static VersionSupport Kafka8 => _kafka8 ?? (_kafka8 = new VersionSupport(
            new Dictionary<ApiKeyRequestType, short> {
                {ApiKeyRequestType.Produce, 0},
                {ApiKeyRequestType.Fetch, 0},
                {ApiKeyRequestType.Offset, 0},
                {ApiKeyRequestType.Metadata, 0},
                {ApiKeyRequestType.OffsetCommit, 1},
                {ApiKeyRequestType.OffsetFetch, 1},
                {ApiKeyRequestType.GroupCoordinator, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka9;
        /// <summary>
        /// 0.9 &lt;= Version &lt; 0.10
        /// </summary>
        public static VersionSupport Kafka9 => _kafka9 ?? (_kafka9 = new VersionSupport(
            new Dictionary<ApiKeyRequestType, short> {
                {ApiKeyRequestType.Produce, 1},
                {ApiKeyRequestType.Fetch, 1},
                {ApiKeyRequestType.Offset, 0},
                {ApiKeyRequestType.Metadata, 0},
                {ApiKeyRequestType.OffsetCommit, 2},
                {ApiKeyRequestType.OffsetFetch, 1},
                {ApiKeyRequestType.GroupCoordinator, 0},
                {ApiKeyRequestType.JoinGroup, 0},
                {ApiKeyRequestType.Heartbeat, 0},
                {ApiKeyRequestType.LeaveGroup, 0},
                {ApiKeyRequestType.SyncGroup, 0},
                {ApiKeyRequestType.DescribeGroups, 0},
                {ApiKeyRequestType.ListGroups, 0},
                {ApiKeyRequestType.SaslHandshake, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka10;
        /// <summary>
        /// 0.10 &lt;= Version 
        /// </summary>
        public static VersionSupport Kafka10 => _kafka10 ?? (_kafka10 = new VersionSupport(
            new Dictionary<ApiKeyRequestType, short> {
                {ApiKeyRequestType.Produce, 2},
                {ApiKeyRequestType.Fetch, 2},
                {ApiKeyRequestType.Offset, 0},
                {ApiKeyRequestType.Metadata, 1},
                {ApiKeyRequestType.OffsetCommit, 2},
                {ApiKeyRequestType.OffsetFetch, 1},
                {ApiKeyRequestType.GroupCoordinator, 0},
                {ApiKeyRequestType.JoinGroup, 0},
                {ApiKeyRequestType.Heartbeat, 0},
                {ApiKeyRequestType.LeaveGroup, 0},
                {ApiKeyRequestType.SyncGroup, 0},
                {ApiKeyRequestType.DescribeGroups, 0},
                {ApiKeyRequestType.ListGroups, 0},
                {ApiKeyRequestType.SaslHandshake, 0},
                {ApiKeyRequestType.ApiVersions, 0}
            }.ToImmutableDictionary()));

        private static VersionSupport _kafka10_1;
        /// <summary>
        /// 0.10 &lt;= Version 
        /// </summary>
        public static VersionSupport Kafka10_1 => _kafka10_1 ?? (_kafka10_1 = new VersionSupport(
            new Dictionary<ApiKeyRequestType, short> {
                {ApiKeyRequestType.Produce, 2},
                {ApiKeyRequestType.Fetch, 3},
                {ApiKeyRequestType.Offset, 1},
                {ApiKeyRequestType.Metadata, 2},
                {ApiKeyRequestType.OffsetCommit, 2},
                {ApiKeyRequestType.OffsetFetch, 1},
                {ApiKeyRequestType.GroupCoordinator, 0},
                {ApiKeyRequestType.JoinGroup, 1},
                {ApiKeyRequestType.Heartbeat, 0},
                {ApiKeyRequestType.LeaveGroup, 0},
                {ApiKeyRequestType.SyncGroup, 0},
                {ApiKeyRequestType.DescribeGroups, 0},
                {ApiKeyRequestType.ListGroups, 0},
                {ApiKeyRequestType.SaslHandshake, 0},
                {ApiKeyRequestType.ApiVersions, 0},
                {ApiKeyRequestType.CreateTopics, 0},
                {ApiKeyRequestType.DeleteTopics, 0}
            }.ToImmutableDictionary()));


        private readonly IImmutableDictionary<ApiKeyRequestType, short> _versionSupport;

        public VersionSupport(IImmutableDictionary<ApiKeyRequestType, short> versionSupport)
        {
            _versionSupport = versionSupport;
            IsDynamic = false;
        }

        public VersionSupport(VersionSupport versionSupport, bool isDynamic)
        {
            _versionSupport = versionSupport._versionSupport;
            IsDynamic = isDynamic;
        }

        /// <inheritdoc />
        public bool IsDynamic { get; }

        public short? GetVersion(ApiKeyRequestType requestType)
        {
            short version;
            return _versionSupport.TryGetValue(requestType, out version) ? version : (short?)null;
        }
    }
}