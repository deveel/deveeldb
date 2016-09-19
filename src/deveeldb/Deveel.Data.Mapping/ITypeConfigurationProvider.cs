using System;
using System.Collections.Generic;

namespace Deveel.Data.Mapping {
	interface ITypeConfigurationProvider<TType> : ITypeConfiguration<TType> {
		string TableName { get; }

		IEnumerable<IMemberConfigurationProvider> MemberConfigurations { get; }
	}
}
