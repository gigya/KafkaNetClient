using System;

namespace KafkaClient.Protocol
{
    public class CrcValidationException : KafkaException
    {
        public CrcValidationException(uint crc, uint calculatedCrc)
            : base("Calculated CRC did not match reported CRC.")
        {
            Crc = crc;
            CalculatedCrc = calculatedCrc;
        }

        public CrcValidationException(string message)
            : base(message)
        {
        }

        public CrcValidationException(string message, Exception innerException)
            : base(message, innerException)
        {
        }

        public uint Crc { get; set; }
        public uint CalculatedCrc { get; set; }
    }
}