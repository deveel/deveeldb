using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design.Configuration {
	public sealed class TypeModelConfiguration {
		private Dictionary<string, MemberModelConfiguration> members;
		private Dictionary<ConstraintKey, ConstraintModelConfiguration> constraints;

		internal TypeModelConfiguration(ModelConfiguration model, Type type) {
			Model = model;
			Type = type;
			members = new Dictionary<string, MemberModelConfiguration>();
			constraints = new Dictionary<ConstraintKey, ConstraintModelConfiguration>();
		}

		public ModelConfiguration Model { get; private set; }

		public string TableName { get; set; }

		public Type Type { get; private set; }

		public IEnumerable<string> MemberNames {
			get { return members.Keys; }
		}

		public void IgnoreMember(string memberName) {
			if (!members.Remove(memberName))
				throw new InvalidOperationException(String.Format("Member '{0}' was not found in type '{1}'.", memberName, Type));
		}

		public MemberModelConfiguration IncludeMember(string memberName, bool nonPublic) {
			var flags = BindingFlags.Instance | BindingFlags.Public;
			if (nonPublic)
				flags |= BindingFlags.NonPublic;

			var member = Type.GetMember(memberName, flags);
			if (member.Length == 0)
				throw new InvalidOperationException(String.Format("Member '{0}' was not found in type '{1}'.", memberName, Type));

			if (member.Length > 1)
				throw new InvalidOperationException(String.Format("Ambiguous reference for member name '{0}' in type '{1}.", memberName, Type));

			if (!(member[0] is PropertyInfo) &&
				!(member[0] is FieldInfo))
				throw new InvalidOperationException(String.Format("The member '{0}' in type '{1}' is not a property or a field.", memberName, Type));

			var memberBuildInfo = new MemberModelConfiguration(this, member[0]);
			members[memberName] = memberBuildInfo;

			return memberBuildInfo;
		}

		public MemberModelConfiguration GetMember(string member) {
			MemberModelConfiguration configuration;
			if (!members.TryGetValue(member, out configuration))
				return null;

			return configuration;
		}

		public IEnumerable<ConstraintModelConfiguration> GetConstraints() {
			return constraints.Values.AsEnumerable();
		}

		public ConstraintModelConfiguration GetConstraint(string name, ConstraintType type) {
			var key = new ConstraintKey(name, type);
			ConstraintModelConfiguration constraintConfiguration;
			if (!constraints.TryGetValue(key, out constraintConfiguration)) {
				constraintConfiguration = new ConstraintModelConfiguration(this, name, type);
				constraints[key] = constraintConfiguration;
			}

			return constraintConfiguration;
		}

		public ConstraintModelConfiguration GetConstraint(ConstraintType type) {
			return GetConstraint(null, type);
		}

		internal bool HasMember(string memberName) {
			return members.ContainsKey(memberName);
		}

		internal TypeModelConfiguration Clone(ModelConfiguration model) {
			var cloned = new TypeModelConfiguration(model, Type);
			cloned.TableName = TableName;
			cloned.members = new Dictionary<string, MemberModelConfiguration>(members.ToDictionary(x => x.Key, y => y.Value.Clone(cloned)));
			cloned.constraints = new Dictionary<ConstraintKey, ConstraintModelConfiguration>(constraints.ToDictionary(x => x.Key, y => y.Value.Clone(cloned)));
			return cloned;
		}

		#region ConstraintKey

		class ConstraintKey : IEquatable<ConstraintKey> {
			public ConstraintKey(string name, ConstraintType constraintType) {
				Name = name;
				ConstraintType = constraintType;
			}

			public string Name { get; private set; }

			public ConstraintType ConstraintType { get; private set; }

			public bool Equals(ConstraintKey other) {
				return String.Equals(Name, other.Name) &&
				       ConstraintType == other.ConstraintType;
			}
		}

		#endregion
	}
}
