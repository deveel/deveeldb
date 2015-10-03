using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Mapping {
	public sealed class MappingModel {
		private readonly Dictionary<Type, TypeMapping> typeMappings;
		private readonly List<Relationship> relationships; 

		internal MappingModel() {
			typeMappings = new Dictionary<Type, TypeMapping>();
			relationships = new List<Relationship>();
		}

		internal void Map(TypeMapping mapping) {
			typeMappings[mapping.Type] = mapping;
		}

		internal void AddRelationship(Relationship relationship) {
			if (relationship == null)
				throw new ArgumentNullException("relationship");

			if (!IsMapped(relationship.SourceType))
				throw new ArgumentException(String.Format("Type '{0}' source of the relationship is not mapped.", relationship.SourceType));
			if (!IsMapped(relationship.TargetType))
				throw new ArgumentException(String.Format("Type '{0}' destination of the relationship is not mapped.", relationship.TargetType));

			var typeMapping = GetMapping(relationship.SourceType);
			if (!typeMapping.IsMemberMapped(relationship.SourceMember))
				throw new ArgumentException(String.Format("Member '{0}' in type '{1}' source of the relationship is not mapped",
					relationship.SourceMember, relationship.SourceType));

			relationships.Add(relationship);
		}

		public IEnumerable<Type> Types {
			get { return typeMappings.Keys.AsEnumerable(); }
		}

		public IEnumerable<TypeMapping> TypeMappings {
			get { return typeMappings.Values.AsEnumerable(); }
		} 

		public IEnumerable<Relationship> Relationships {
			get { return relationships.AsReadOnly(); }
		}

		public bool IsMapped(Type type) {
			return typeMappings.ContainsKey(type);
		}

		public TypeMapping GetMapping(Type type) {
			if (type == null)
				throw new ArgumentNullException("type");

			TypeMapping mapping;
			if (!typeMappings.TryGetValue(type, out mapping))
				return null;

			return mapping;
		}

		public Relationship GetRelationship(Type sourceType, string sourceMember) {
			return relationships.FirstOrDefault(x => x.SourceType == sourceType &&
			                                         x.SourceMember == sourceMember);
		}
	}
}
