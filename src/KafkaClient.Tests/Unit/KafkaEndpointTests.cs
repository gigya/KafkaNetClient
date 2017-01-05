using System;
using System.Net;
using KafkaClient.Connections;
using NUnit.Framework;

namespace KafkaClient.Tests.Unit
{
    [Category("Unit")]
    [TestFixture]
    public class KafkaEndpointTests
    {
        [Test]
        public void EnsureEndpointCanBeResulved()
        {
            var expected = IPAddress.Parse("127.0.0.1");
            var endpoint = new ConnectionFactory().Resolve(new Uri("http://localhost:8888"), TestConfig.Log);
            Assert.That(endpoint.IP.Address, Is.EqualTo(expected));
            Assert.That(endpoint.IP.Port, Is.EqualTo(8888));
        }

        [Test]
        public void EnsureTwoEndpointNotOfTheSameReferenceButSameIPAreEqual()
        {
            var endpoint1 = new ConnectionFactory().Resolve(new Uri("http://localhost:8888"), TestConfig.Log);
            var endpoint2 = new ConnectionFactory().Resolve(new Uri("http://localhost:8888"), TestConfig.Log);

            Assert.That(ReferenceEquals(endpoint1, endpoint2), Is.False, "Should not be the same reference.");
            Assert.That(endpoint1, Is.EqualTo(endpoint2));
        }

        [Test]
        public void EnsureTwoEndointWithSameIPButDifferentPortsAreNotEqual()
        {
            var endpoint1 = new ConnectionFactory().Resolve(new Uri("http://localhost:8888"), TestConfig.Log);
            var endpoint2 = new ConnectionFactory().Resolve(new Uri("http://localhost:1"), TestConfig.Log);

            Assert.That(endpoint1, Is.Not.EqualTo(endpoint2));
        }
    }
}