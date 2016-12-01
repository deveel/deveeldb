using System;

using Deveel.Data.Design.Configuration;

namespace Deveel.Data.Design {
	public sealed class DbAssociationInfo {
		internal DbAssociationInfo(AssociationModelConfiguration configuration) {
			Configuration = configuration;
		}

		private AssociationModelConfiguration Configuration { get; set; }

		public AssociationType AssociationType {
			get { return Configuration.AssociationType; }
		}

		public AssociationCardinality Cardinality {
			get { return Configuration.Cardinality; }
		}

		public DbMemberInfo SourceMember {
			get { return GetSourceMember(); }
		}

		public DbMemberInfo DestinationMember {
			get { return GetDestinationMember(); }
		}

		private DbMemberInfo GetSourceMember() {
			if (AssociationType == AssociationType.Source)
				return new DbMemberInfo(Configuration.SourceMember);

			return new DbMemberInfo(Configuration.TargetMember);
		}

		private DbMemberInfo GetDestinationMember() {
			if (AssociationType == AssociationType.Destination)
				return new DbMemberInfo(Configuration.SourceMember);

			return new DbMemberInfo(Configuration.TargetMember);
		}
	}
}
