using System;
using System.Collections.Generic;
using System.Collections.Immutable;
using System.Linq;

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

        protected abstract IImmutableDictionary<string, TAssignment> Assign(IImmutableDictionary<string, TMetadata> memberMetadata);

        public IImmutableDictionary<string, IMemberAssignment> AssignMembers(IImmutableDictionary<string, IMemberMetadata> memberMetadata)
        {
            if (memberMetadata == null) throw new ArgumentNullException(nameof(memberMetadata));

            var typedMetadata = memberMetadata.Select(pair => new KeyValuePair<string, TMetadata>(pair.Key, (TMetadata)pair.Value)).ToImmutableDictionary();
            var typedAssignment = Assign(typedMetadata);
            return typedAssignment.Select(pair => new KeyValuePair<string, IMemberAssignment>(pair.Key, pair.Value)).ToImmutableDictionary();
        }
    }
}