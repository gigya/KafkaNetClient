using System.Collections.Immutable;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Assignment
{
    public interface IMembershipAssignor
    {
        string AssignmentStrategy { get; }

        Task<IImmutableDictionary<string, IMemberAssignment>> AssignMembersAsync(IRouter router, IImmutableDictionary<string, IMemberMetadata> memberMetadata, CancellationToken cancellationToken);
    }
}