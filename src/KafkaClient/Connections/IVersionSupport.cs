using KafkaClient.Protocol;

namespace KafkaClient.Connections
{
    public interface IVersionSupport
    {
        bool IsDynamic { get; }
        short? GetVersion(ApiKeyRequestType requestType);
    }
}