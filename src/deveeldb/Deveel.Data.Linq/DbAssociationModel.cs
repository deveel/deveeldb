using System;
using System.Reflection;

namespace Deveel.Data.Linq {
	public sealed class DbAssociationModel {
		internal DbAssociationModel(MemberInfo member, DbColumnModel sourceColumn, DbTypeModel destType, DbColumnModel destColumn) {
			Member = member;
			SourceColumn = sourceColumn;
			DestinationType = destType;
			DestinationColumn = destColumn;
		}

		public MemberInfo Member { get; private set; }

		public DbColumnModel SourceColumn { get; private set; }

		public DbTypeModel DestinationType { get; private set; }

		public DbColumnModel DestinationColumn { get; private set; }
	}
}
