using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class AssociationConfiguration<T> : IAssociationConfiguration {
		internal AssociationConfiguration(MemberInfo member, Type associatedType, MemberInfo associatedMember) {
			Member = member;
			AssociatedType = associatedType;
			AssociatedMember = associatedMember;
		}

		internal MemberInfo Member { get; private set; }

		private Type AssociatedType { get; set; }

		private MemberInfo AssociatedMember { get; set; }

		DbAssociationModel IAssociationConfiguration.CreateModel(DbModelBuildContext context) {
			throw new NotImplementedException();
		}
	}
}
