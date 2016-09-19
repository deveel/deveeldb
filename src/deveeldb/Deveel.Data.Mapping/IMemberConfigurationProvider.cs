using System;

using Deveel.Data.Sql.Expressions;
using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	interface IMemberConfigurationProvider : IMemberConfiguration {
		string ColumnName { get; }

		SqlType ColumnType { get; }

		bool IsNotNull { get; }

		bool IsUnique { get; }

		bool IsPrimaryKey { get; }

		SqlExpression DefaultExpression { get; }

		bool IsIgnored { get; }
	}
}
