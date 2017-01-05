using System.Linq;
using KafkaClient.Protocol;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [TestFixture]
    public class ProtocolTests
    {
        [Test]
        public void MetadataResponseShouldDecode()
        {
            var response = KafkaEncoder.Decode<MetadataResponse>(new RequestContext(1), MessageHelper.CreateMetadataResponse(1, "Test").Skip(4).ToArray());

            Assert.That(response.Topics[0].TopicName, Is.EqualTo("Test"));
        }
    }
}