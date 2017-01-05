namespace KafkaClient.Protocol
{
    /// <summary>
    /// Enumeration which specifies the compression type of messages
    /// </summary>
    public enum MessageCodec : byte
    {
        CodecNone = 0x00,
        CodecGzip = 0x01,
        CodecSnappy = 0x02
    }
}