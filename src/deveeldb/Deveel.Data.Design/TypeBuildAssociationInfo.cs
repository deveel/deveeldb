using System;

namespace Deveel.Data.Design {
	public sealed class TypeBuildAssociationInfo {
		internal TypeBuildAssociationInfo(TypeBuildInfo typeInfo, string memberName, AssociationType associationType) {
			TypeInfo = typeInfo;
			MemberName = memberName;
			AssociationType = associationType;
		}

		public TypeBuildInfo TypeInfo { get; private set; }

		public string MemberName { get; private set; }

		public TypeBuildMemberInfo MemberInfo {
			get { return TypeInfo.GetMember(MemberName); }
		}

		public AssociationType AssociationType { get; private set; }

		public AssociationCardinality Cardinality { get; set; }

		public TypeBuildMemberInfo OtherMemberInfo { get; set; }
	}
}
