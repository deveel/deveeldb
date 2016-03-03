using System;
using System.Collections.Generic;
using System.Reflection;

using Deveel.Data.Sql.Types;

namespace Deveel.Data.Mapping {
	interface IMemberMappingConfiguration {
		MemberInfo Member { get; }

		string ColumnName { get; }

		SqlTypeCode? ColumnType { get; }

		int? Size { get; }

		int? Precision { get; }

		ColumnConstraints ColumnConstraints { get; }
	}
}
