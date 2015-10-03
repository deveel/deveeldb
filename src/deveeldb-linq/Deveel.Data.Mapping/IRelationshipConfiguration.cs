using System;

namespace Deveel.Data.Mapping {
	interface IRelationshipConfiguration {
		Type DestinationType { get; }

		string SourceMemberName { get; }
	}
}
