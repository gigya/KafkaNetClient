namespace KafkaClient.Connections
{
    public static class Extensions
    {
        public static IConnection Create(this KafkaOptions options, Endpoint endpoint)
        {
            return options.ConnectionFactory.Create(endpoint, options.ConnectionConfiguration, options.Log);
        }

        public static IVersionSupport MakeDynamic(this VersionSupport versionSupport)
        {
            return new VersionSupport(versionSupport, isDynamic: true);
        }
    }
}