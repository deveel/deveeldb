using System;
using System.Collections.Generic;

namespace Deveel.Data.Mapping {
	interface ITypeMappingConfiguration {
		Type ElementType { get; }

		string TableName { get; }

		string UniqueKeyMember { get; }

		IEnumerable<KeyValuePair<string, IMemberMappingConfiguration>> Members { get; } 
	}
}
