using System;
using System.Collections.Generic;

namespace Deveel.Data.Mapping {
	interface ITypeMappingConfiguration {
		string TableName { get; }

		string UniqueKeyMember { get; }

		IEnumerable<KeyValuePair<string, IMemberMappingConfiguration>> Members { get; } 
	}
}
