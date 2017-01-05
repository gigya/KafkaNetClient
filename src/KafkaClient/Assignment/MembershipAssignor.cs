using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace KafkaClient.Assignment
{
    public abstract class MembershipAssignor<TMetadata, TAssignment> : IMembershipAssignor
        where TMetadata : IMemberMetadata
        where TAssignment : IMemberAssignment
    {
        protected MembershipAssignor(string assignmentStrategy)
        {
            AssignmentStrategy = assignmentStrategy;
        }

        public string AssignmentStrategy { get; }

        protected abstract Task<IImmutableDictionary<string, TAssignment>> AssignAsync(IRouter router, IImmutableDictionary<string, TMetadata> memberMetadata, CancellationToken cancellationToken);

        public async Task<IImmutableDictionary<string, IMemberAssignment>> AssignMembersAsync(IRouter router, IImmutableDictionary<string, IMemberMetadata> memberMetadata, CancellationToken cancellationToken)
        {
            if (memberMetadata == null) throw new ArgumentNullException(nameof(memberMetadata));

            var typedMetadata = memberMetadata.Select(pair => new KeyValuePair<string, TMetadata>(pair.Key, (TMetadata)pair.Value)).ToImmutableDictionary();
            var typedAssignment = await AssignAsync(router, typedMetadata, cancellationToken);
            return typedAssignment.Select(pair => new KeyValuePair<string, IMemberAssignment>(pair.Key, pair.Value)).ToImmutableDictionary();
        }
    }
}