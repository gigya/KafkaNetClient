using System.Threading;
using System.Threading.Tasks;
using KafkaClient.Common;
using KafkaClient.Protocol;

namespace KafkaClient
{
    internal class RoutedGroupRequest<T> : RoutedRequest<T> 
        where T : class, IResponse
    {
        public RoutedGroupRequest(IRequest<T> request, string groupId, ILog log)
            : base(request, log)
        {
            _groupId = groupId;
        }

        private readonly string _groupId;

        protected override async Task<Broker> GetBrokerAsync(IRouter router, CancellationToken cancellationToken)
        {
            return await router.GetGroupBrokerAsync(_groupId, cancellationToken).ConfigureAwait(false);
        }
    }
}