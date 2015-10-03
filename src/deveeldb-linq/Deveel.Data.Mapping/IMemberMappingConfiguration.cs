using System;
using System.Collections.Generic;

using Deveel.Data.Types;

namespace Deveel.Data.Mapping {
	interface IMemberMappingConfiguration {
		string ColumnName { get; }

		SqlTypeCode? ColumnType { get; }

		int? Size { get; }

		int? Precision { get; }

		ColumnConstraints ColumnConstraints { get; }
	}
}
