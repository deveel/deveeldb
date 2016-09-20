using System;

namespace Deveel.Data.Design {
	public sealed class DbAssociationInfo {
		internal DbAssociationInfo(TypeBuildAssociationInfo associationInfo) {
			AssociationInfo = associationInfo;
		}

		private TypeBuildAssociationInfo AssociationInfo { get; set; }

		public AssociationType AssociationType {
			get { return AssociationInfo.AssociationType; }
		}

		public AssociationCardinality Cardinality {
			get { return AssociationInfo.Cardinality; }
		}

		public DbMemberInfo SourceMember {
			get { return GetSourceMember(); }
		}

		public DbMemberInfo DestinationMember {
			get { return GetDestinationMember(); }
		}

		private DbMemberInfo GetSourceMember() {
			if (AssociationType == AssociationType.Source)
				return new DbMemberInfo(AssociationInfo.MemberInfo);

			return new DbMemberInfo(AssociationInfo.OtherMemberInfo);
		}

		private DbMemberInfo GetDestinationMember() {
			if (AssociationType == AssociationType.Destination)
				return new DbMemberInfo(AssociationInfo.MemberInfo);

			return new DbMemberInfo(AssociationInfo.OtherMemberInfo);
		}
	}
}
