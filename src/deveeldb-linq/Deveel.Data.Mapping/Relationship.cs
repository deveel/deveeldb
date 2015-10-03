using System;

namespace Deveel.Data.Mapping {
	public sealed class Relationship {
		internal Relationship(Type sourceType, string sourceMember, Type targetType, RelationshipType relationshipType, bool isOptional) {
			SourceType = sourceType;
			SourceMember = sourceMember;
			TargetType = targetType;
			RelationshipType = relationshipType;
			IsOptional = isOptional;
		}

		public Type SourceType { get; private set; }
		
		public string SourceMember { get; private set; }

		public Type TargetType { get; private set; }

		public RelationshipType RelationshipType { get; private set; }

		public bool IsOptional { get; private set; }
	}
}
