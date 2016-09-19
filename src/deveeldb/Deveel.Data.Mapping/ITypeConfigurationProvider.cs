using System;
using System.Collections.Generic;

namespace Deveel.Data.Mapping {
	interface ITypeConfigurationProvider {
		Type Type { get; }

		string TableName { get; }

		IEnumerable<IMemberConfigurationProvider> MemberConfigurations { get; }
	}
}
