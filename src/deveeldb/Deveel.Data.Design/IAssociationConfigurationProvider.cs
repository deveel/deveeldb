using System;
using System.Reflection;

namespace Deveel.Data.Design {
	interface IAssociationConfigurationProvider {
		AssociationType AssociationType { get; }

		AssociationCardinality Cardinality { get; }

		Type SourceType { get; }

		MemberInfo SourceMember { get; }

		MemberInfo OtherMember { get; }

		MemberInfo KeyMember { get; }

		Type OtherType { get; }

		bool Cascade { get; }
	}
}
