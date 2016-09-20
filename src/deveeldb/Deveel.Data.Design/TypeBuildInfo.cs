using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;

using Deveel.Data.Sql.Tables;

namespace Deveel.Data.Design {
	public sealed class TypeBuildInfo {
		private Dictionary<string, TypeBuildMemberInfo> members;
		private Dictionary<string, TypeBuildAssociationInfo> associations;
		private Dictionary<ConstraintKey, TypeBuildConstraintInfo> constraints;

		internal TypeBuildInfo(Type type) {
			Type = type;
			members = new Dictionary<string, TypeBuildMemberInfo>();
			associations = new Dictionary<string, TypeBuildAssociationInfo>();
			constraints = new Dictionary<ConstraintKey, TypeBuildConstraintInfo>();
		}

		public string TableName { get; set; }

		public Type Type { get; private set; }

		public IEnumerable<TypeBuildMemberInfo> GetMembers() {
			return members.Values.AsEnumerable();
		}

		public void IgnoreMember(string memberName) {
			if (!members.Remove(memberName))
				throw new InvalidOperationException(String.Format("Member '{0}' was not found in type '{1}'.", memberName, Type));
		}

		public TypeBuildMemberInfo IncludeMember(string memberName, bool nonPublic) {
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

			var memberBuildInfo = new TypeBuildMemberInfo(this, member[0]);
			members[memberName] = memberBuildInfo;

			return memberBuildInfo;
		}

		public TypeBuildMemberInfo GetMember(string member) {
			TypeBuildMemberInfo buildInfo;
			if (!members.TryGetValue(member, out buildInfo))
				return null;

			return buildInfo;
		}

		public TypeBuildAssociationInfo Associate(string memberName, AssociationType type) {
			TypeBuildMemberInfo memberInfo;
			if (!members.TryGetValue(memberName, out memberInfo))
				throw new InvalidOperationException(String.Format("The member '{0}' was not defined in type '{1}'.", memberName, Type));

			var association = new TypeBuildAssociationInfo(this, memberName, type);
			associations[memberName] = association;

			return association;
		}

		public TypeBuildAssociationInfo GetAssociation(string memberName) {
			TypeBuildAssociationInfo association;
			if (!associations.TryGetValue(memberName, out association))
				return null;

			return association;
		}

		public IEnumerable<TypeBuildConstraintInfo> GetConstraints() {
			return constraints.Values.AsEnumerable();
		}

		public TypeBuildConstraintInfo GetConstraint(string name, ConstraintType type) {
			var key = new ConstraintKey(name, type);
			TypeBuildConstraintInfo constraintInfo;
			if (!constraints.TryGetValue(key, out constraintInfo)) {
				constraintInfo = new TypeBuildConstraintInfo(this, name, type);
				constraints[key] = constraintInfo;
			}

			return constraintInfo;
		}

		public TypeBuildConstraintInfo GetConstraint(ConstraintType type) {
			return GetConstraint(null, type);
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
