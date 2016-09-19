using System;
using System.Collections.Generic;

using Deveel.Data.Sql.Expressions;

namespace Deveel.Data.Design {
	interface ITypeConfigurationProvider {
		Type Type { get; }

		string TableName { get; }

		SqlExpression Check { get; }

		IEnumerable<IMemberConfigurationProvider> MemberConfigurations { get; }
	}
}
