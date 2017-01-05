using System;
using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;
using KafkaClient.Common;
using System.Linq;
using Nito.AsyncEx;

namespace KafkaClient.Connections
{
    public class Endpoint : IEquatable<Endpoint>
    {
        public Endpoint(Uri serverUri, IPEndPoint ip)
        {
            ServerUri = serverUri;
            IP = ip;
        }

        public Uri ServerUri { get; }
        [SuppressMessage("ReSharper", "InconsistentNaming")]
        public IPEndPoint IP { get; }

        #region Equality

        public override bool Equals(object obj)
        {
            return Equals(obj as Endpoint);
        }

        public bool Equals(Endpoint other)
        {
            if (ReferenceEquals(null, other)) return false;
            if (ReferenceEquals(this, other)) return true;
            return Equals(IP, other.IP);
        }

        public override int GetHashCode()
        {
            // calculated like this to ensure ports on same address sort in descending order
            return IP?.Address.GetHashCode() + IP?.Port ?? 0;
        }

        public static bool operator ==(Endpoint left, Endpoint right)
        {
            return Equals(left, right);
        }

        public static bool operator !=(Endpoint left, Endpoint right)
        {
            return !Equals(left, right);
        }

        #endregion

        public override string ToString() => ServerUri.ToString();

        public static Endpoint Resolve(Uri serverUri, ILog log)
        {
            var ipEndpoint = new IPEndPoint(GetFirstAddress(serverUri.Host, log), serverUri.Port);
            return new Endpoint(serverUri, ipEndpoint);
        }

        private static IPAddress GetFirstAddress(string hostname, ILog log)
        {
            try {
                var addresses = AsyncContext.Run(() => Dns.GetHostAddressesAsync(hostname));
                if (addresses.Length > 0) {
                    foreach (var address in addresses) {
                        log?.Debug(() => LogEvent.Create($"Found address {address} for {hostname}"));
                    }

                    var selectedAddress = addresses.FirstOrDefault(item => item.AddressFamily == AddressFamily.InterNetwork) ?? addresses.First();
                    log?.Debug(() => LogEvent.Create($"Using address {selectedAddress} for {hostname}"));
                    return selectedAddress;
                }
            } catch (Exception ex) {
                log?.Info(() => LogEvent.Create(ex));
            }

            throw new ConnectionException($"Could not resolve the following hostname: {hostname}");
        }
    }
}