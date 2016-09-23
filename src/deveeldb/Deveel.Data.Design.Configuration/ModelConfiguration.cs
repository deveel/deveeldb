using System;
using System.Collections.Generic;
using System.Linq;

namespace Deveel.Data.Design.Configuration {
	public sealed class ModelConfiguration {
		private Dictionary<Type, TypeModelConfiguration> typeConfigurations;
		private Dictionary<AssociationKey, AssociationModelConfiguration> associationConfigurations;
		private IList<Type> ignoredTypes;

		internal ModelConfiguration() {
			typeConfigurations = new Dictionary<Type, TypeModelConfiguration>();
			associationConfigurations = new Dictionary<AssociationKey, AssociationModelConfiguration>();
			ignoredTypes = new List<Type>();
		}

		internal IEnumerable<Type> Types {
			get { return typeConfigurations.Keys.AsEnumerable(); }
		}

		public void IgnoreType(Type type) {
			if (!ignoredTypes.Contains(type))
				ignoredTypes.Add(type);
		}

		public TypeModelConfiguration Type(Type type) {
			if (ignoredTypes.Contains(type))
				throw new InvalidOperationException(String.Format("The type '{0}' is ignored.", type));

			TypeModelConfiguration configuration;
			if (!typeConfigurations.TryGetValue(type, out configuration)) {
				configuration = new TypeModelConfiguration(this, type);
				typeConfigurations[type] = configuration;
			}

			return configuration;
		}

		//public AssociationModelConfiguration Associate(string memberName, AssociationType type) {
		//	MemberModelConfiguration configuration;
		//	if (!members.TryGetValue(memberName, out configuration))
		//		throw new InvalidOperationException(String.Format("The member '{0}' was not defined in type '{1}'.", memberName, Type));

		//	var association = new AssociationModelConfiguration(this, memberName, type);
		//	associations[memberName] = association;

		//	return association;
		//}

		//public AssociationModelConfiguration GetAssociation(string memberName) {
		//	AssociationModelConfiguration association;
		//	if (!associations.TryGetValue(memberName, out association))
		//		return null;

		//	return association;
		//}

		internal ModelConfiguration Clone() {
			var cloned = new ModelConfiguration {
				ignoredTypes = new List<Type>(ignoredTypes),
			};

			cloned.typeConfigurations = new Dictionary<Type, TypeModelConfiguration>(typeConfigurations.ToDictionary(x => x.Key, y => y.Value.Clone(cloned)));

			foreach (var configuration in associationConfigurations) {
				var typeModel = cloned.typeConfigurations[configuration.Key.SourceType];
				cloned.associationConfigurations.Add(configuration.Key, configuration.Value.Clone(typeModel));
			}

			return cloned;
		}

		internal bool IsDependantMember(Type type, string memberName) {
			var fwdKey = new AssociationKey(type, memberName, AssociationType.Destination);
			if (associationConfigurations.ContainsKey(fwdKey))
				return true;

			foreach (var association in associationConfigurations.Values) {
				if (association.TargetType.Type == type &&
				    association.TargetMember.Member.Name == memberName)
					return true;
			}

			return false;
		}

		public AssociationModelConfiguration Associate(Type sourceType, string sourceMemberName, AssociationType associationType) {
			var key = new AssociationKey(sourceType, sourceMemberName, associationType);
			AssociationModelConfiguration configuration;

			if (!associationConfigurations.TryGetValue(key, out configuration)) {
				var typeModel = Type(sourceType);

				configuration = new AssociationModelConfiguration(typeModel, sourceMemberName, associationType);
				associationConfigurations[key] = configuration;
			}

			return configuration;
		}

		#region AssociationKey

		class AssociationKey : IEquatable<AssociationKey> {
			public AssociationKey(Type sourceType, string memberName, AssociationType type) {
				SourceType = sourceType;
				Type = type;
				MemberName = memberName;
			}

			public Type SourceType { get; private set; }

			public AssociationType Type { get; private set; }

			public string MemberName { get; private set; }

			public bool Equals(AssociationKey other) {
				return SourceType == other.SourceType &&
				       Type == other.Type &&
				       MemberName == other.MemberName;
			}
		}

		#endregion
	}
}
